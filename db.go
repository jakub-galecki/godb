package godb

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jakub-galecki/godb/compaction"
	"github.com/jakub-galecki/godb/sst"

	"github.com/jakub-galecki/godb/common"
	"github.com/jakub-galecki/godb/internal/cache"
	"github.com/jakub-galecki/godb/memtable"
	"github.com/jakub-galecki/godb/wal"
)

type StorageEngine interface {
	Delete(key []byte) error
	Set(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	GetSize() int

	// add iterator
}

type db struct {
	*dbEnv

	id         string
	mem        *memtable.MemTable   // mutable
	sink       []*memtable.MemTable // immutable
	l0         *sst.Level
	levels     []*sst.Level
	wl         *wal.Manager
	wlw        wal.Writer
	blockCache cache.Cacher[[]byte]
	mutex      sync.Mutex
	opts       dbOpts
	manifest   *Manifest
	cleaner    *cleaner
	compaction *compaction.LeveledCompaction
	compacting atomic.Bool
}

func Open(name string, opts ...DbOpt) (*db, error) {
	dbOpts := defaultOpts(name, opts)
	if err := dbOpts.validate(); err != nil {
		return nil, err
	}
	d := db{
		id:         string(sha256.New().Sum([]byte(name))),
		sink:       make([]*memtable.MemTable, 0),
		blockCache: cache.New(cache.WithVerbose[[]byte](true)),
		opts:       dbOpts,
		cleaner:    newClener(),
	}
	switch _, err := os.Stat(dbOpts.path); {
	case err == nil:
		// todo: after recover - wal lsn resets
		err = d.recover()
		if err != nil {
			panic(err)
		}
	case errors.Is(err, os.ErrNotExist):
		err = d.new()
		if err != nil {
			panic(err)
		}
	}
	d.compaction = compaction.NewLeveledCompaction(compaction.DefaultOptions)
	go d.drainSink()
	return &d, nil
}

func (l *db) recover() (err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	start := time.Now()
	// todo: include scenario where we recover but all memtables were flushed
	m, err := readManifest(l.opts.path)
	if err != nil {
		return err
	}
	if m.Id != l.id {
		return errors.New("id hash did not match")
	}
	// // if DEBUG = true
	if true {
		b, err := json.Marshal(m)
		if err != nil {
			l.opts.logger.Error().Err(err).Msg("marshaling Manifest for log")
		}
		l.opts.logger.Info().RawJSON("Manifest", b).Msg("recovered Manifest")
	}

	l.manifest = m
	l.dbEnv = envFromManifest(m)
	l.wl, err = wal.Init(wal.DefaultOpts.WithDir(path.Join(l.opts.path, common.WAL)))
	if err != nil {
		return err
	}
	logFiles, err := l.getLogFiles()
	if err != nil {
		return nil
	}
	toDel := make([]string, 0, len(logFiles))

	toDel, logsToRecover := l.getLogsToRecover(logFiles, toDel)
	toDel = append(toDel, l.getDeadFiles()...)
	l.cleaner.removeSync(toDel)
	err = l.recoverWal(logsToRecover)
	if err != nil {
		return err
	}
	err = l.loadLevels()
	if err != nil {
		return err
	}
	l.opts.logger.Event("recover", start)
	return nil
}

func (l *db) getLogFiles() ([]wal.WalLogNum, error) {
	wals, err := common.ListDir(path.Join(l.opts.path, common.WAL), func(f string) (wal.WalLogNum, bool) {
		logSeqIndex := strings.IndexByte(f, '.')
		if logSeqIndex < 0 {
			return 0, false
		}
		return wal.WalLogNumFromString(f[:logSeqIndex])
	})
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(wals, func(a, b wal.WalLogNum) int { return cmp.Compare(a, b) })
	return wals, nil
}

func (l *db) getLogsToRecover(logFiles []wal.WalLogNum, toDel []string) ([]string, []wal.WalLogNum) {
	i := 0
	for j := 0; j < len(logFiles); j++ {
		if uint64(logFiles[j]) < l.manifest.LastFlushedFileNumber {
			toDel = append(toDel, path.Join(l.opts.path, common.WAL, logFiles[j].FileName()))
			i++
		}
	}
	return toDel, logFiles[i:]
}

func (l *db) recoverWal(wals []wal.WalLogNum) (err error) {
	start := time.Now()
	getMem := func(id wal.WalLogNum) (*memtable.MemTable, error) {
		f, err := os.Open(l.getLogPath(id.FileName()))
		defer func() error { return f.Close() }()
		if err != nil {
			return nil, err
		}
		it, err := wal.NewIterator(f)
		if err != nil {
			return nil, err
		}

		mem := memtable.New(uint64(id))
		err = wal.Iter(it, func(raw []byte) error {
			b := DecodeBatch(raw)
			defer b.release()
			return applyToMemtable(mem, b)
		})
		if err != nil {
			return nil, err
		}
		return mem, nil
	}
	if len(wals) == 0 {
		fnum := l.getNextFileNum()
		l.wlw, err = l.wl.NewWAL(wal.WalLogNum(fnum))
		if err != nil {
			return err
		}
		l.mem = memtable.New(fnum)
		return nil
	}
	if len(wals) == 1 {
		l.mem, err = getMem(wals[0])
		if err != nil {
			return err
		}
		l.wlw, err = l.wl.OpenWAL(wals[0])
		if err != nil {
			return err
		}
		return nil
	}
	for i := 0; i <= len(wals)-2; i++ {
		mem, err := getMem(wals[i])
		if err != nil {
			return err
		}
		l.sink = append(l.sink, mem)
	}

	mem, err := getMem(wals[len(wals)-1])
	if err != nil {
		return err
	}
	l.wlw, err = l.wl.OpenWAL(wals[len(wals)-1])
	if err != nil {
		return err
	}
	l.mem = mem
	l.opts.logger.Event("recoverWal", start)
	return nil
}

func (l *db) loadLevels() (err error) {
	start := time.Now()
	// load l0 levels
	if l.manifest == nil {
		return errors.New("Manifest not loaded")
	}
	if l.l0 == nil {
		l.l0 = sst.NewLevel(0, l.getSstPath(), &l.mutex, l.blockCache, l.opts.logger)
	}
	err = l.l0.LoadTables(l.manifest.L0)
	if err != nil {
		return err
	}

	// load the rest of levels
	l.levels = make([]*sst.Level, l.manifest.MaxLevels)
	for i, ssts := range l.manifest.Levels {
		if l.levels[i] == nil {
			l.levels[i] = sst.NewLevel(i+1, l.getSstPath(), &l.mutex, l.blockCache, l.opts.logger)
		}
		err = l.levels[i].LoadTables(ssts)
		if err != nil {
			return err
		}
	}

	l.opts.logger.Event("loadLevels", start)
	return nil
}

func (l *db) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.applyEnv(l); err != nil {
		return err
	}
	return nil
}

func (l *db) new() (err error) {
	if err := common.EnsureDir(l.opts.path); err != nil {
		return err
	}
	// todo: take levels from opts
	l.manifest, err = newManifest(l.id, l.opts.path, l.opts.table, sst.BLOCK_SIZE, 4)
	if err != nil {
		return err
	}
	l.dbEnv = envFromManifest(l.manifest)
	l.wl, err = wal.Init(wal.DefaultOpts.WithDir(path.Join(l.opts.path, common.WAL)))
	if err != nil {
		return err
	}
	fnum := l.getNextFileNum()
	l.wlw, err = l.wl.NewWAL(wal.WalLogNum(fnum))
	if err != nil {
		return err
	}
	sstPath := path.Join(l.opts.path, common.SST_DIR)
	if err := common.EnsureDir(sstPath); err != nil {
		return err
	}
	l.mem = memtable.New(fnum)
	if err := l.applyEnv(l); err != nil {
		return err
	}
	// for now use global cache, maybe change so l0 has its own block cache
	l.l0 = sst.NewLevel(0, sstPath, &l.mutex, l.blockCache, l.opts.logger)
	l.levels = make([]*sst.Level, l.manifest.MaxLevels)
	for i := 0; i < l.manifest.MaxLevels; i++ {
		l.levels[i] = sst.NewLevel(i+1, l.getSstPath(), &l.mutex, l.blockCache, l.opts.logger)
	}
	return nil
}

func (l *db) Iterator() common.InnerStorageIterator {
	return l.mem.Iterator()
}

// func (l *db) backgroundCleaner() {
// 	for file := range l.delChan {
// 		os.Remove()
// 	}
// }

func (l *db) getNextSeqNum() uint64 {
	res := l.manifest.SeqNum
	l.manifest.SeqNum++
	return res
}

func (l *db) getSstPath() string {
	return path.Join(l.opts.path, common.SST_DIR)
}

func (l *db) getLogPath(fileName string) string {
	return path.Join(l.opts.path, common.WAL, fileName)
}

func (l *db) getDeadFiles() []string {
	applied := func() []string {
		files := make([]string, 0)
		for _, f := range l.manifest.L0 {
			files = append(files, f)
		}
		for _, lvl := range l.manifest.Levels {
			for _, f := range lvl {
				files = append(files, f)
			}
		}
		return files
	}()
	toDel, err := common.ListDir(path.Join(l.opts.path, common.SST_DIR), func(f string) (string, bool) {
		index := strings.IndexByte(f, '.')
		if index < 0 || slices.Contains(applied, f[:index]) {
			return "", false
		}
		return path.Join(l.opts.path, common.SST_DIR, f), true
	})
	if err != nil {
		l.opts.logger.Error().Err(err).Msg("error listing sstables")
		return nil
	}
	return toDel
}

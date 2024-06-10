package main

import (
	"cmp"
	"crypto/sha256"
	"errors"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"godb/log"
	"godb/sst"

	"godb/common"
	"godb/internal/cache"
	"godb/internal/skiplist"
	"godb/memtable"
	"godb/wal"
)

type StorageEngine interface {
	Delete(key []byte) error
	Set(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	GetSize() int

	// add iterator
}

type db struct {
	id         string
	mem        *memtable.MemTable   // mutable
	sink       []*memtable.MemTable // immutable
	flushChan  chan *memtable.MemTable
	l0         *level
	levels     []*level
	wl         *wal.Manager
	wlw        wal.Writer
	blockCache *cache.Cache[[]byte]
	mutex      sync.Mutex
	opts       dbOpts
	manifest   *Manifest
	delChan    chan string
	logger     *log.Logger
}

type dbOpts struct {
	table string
	path  string
	// enableWal bool
}

type Opts struct {
	DbPath string
}

type DbOpt func(*dbOpts)

func WithDbPath(path string) DbOpt {
	return func(o *dbOpts) {
		o.path = path
	}
}

func Open(table string, opts ...DbOpt) *db {
	dbOpts := dbOpts{
		table: table,
		path:  "/tmp/",
	}

	for _, ofn := range opts {
		ofn(&dbOpts)
	}

	dbOpts.path = path.Join(dbOpts.path, table)

	// if err := common.EnsureDir(dbOpts.path)

	d := db{
		id:         string(sha256.New().Sum([]byte(table))),
		sink:       make([]*memtable.MemTable, 0),
		blockCache: cache.New(cache.WithVerbose[[]byte](true)),
		opts:       dbOpts,
		logger:     log.NewLogger("godb"),
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

	go d.drainSink()

	return &d
}

func (l *db) recover() (err error) {
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
	// if true {
	// 	b, err := json.Marshal(m)
	// 	if err != nil {
	// 		// trace.Error().Err(err).Msg("marshaling Manifest for log")
	// 	}
	// 	// trace.Info().RawJSON("Manifest", b).Msg("recovered Manifest")
	// }

	l.manifest = m
	l.wl, err = wal.Init(wal.DefaultOpts.WithDir(path.Join(l.opts.path, common.WAL)))
	if err != nil {
		return err
	}
	walss, err := common.ListDir(path.Join(l.opts.path, common.WAL), func(f string) (wal.WalLogNum, bool) {
		logSeqIndex := strings.IndexByte(f, '.')
		if logSeqIndex < 0 {
			return 0, false
		}
		return wal.WalLogNumFromString(f[:logSeqIndex])
	})
	if err != nil {
		return err
	}
	slices.SortStableFunc(walss, func(a, b wal.WalLogNum) int { return cmp.Compare(a, b) })
	var toDel []wal.WalLogNum
	i := 0
	for j := 0; j < len(walss); j++ {
		if uint64(walss[j]) <= l.manifest.LastFlushedSeqNum {
			toDel = append(toDel, walss[j])
			i++
		}
	}
	err = l.recoverWal(walss[i:])
	if err != nil {
		return err
	}
	err = l.loadLevels()
	if err != nil {
		return err
	}
	l.logger.Event("recover", start)
	return nil
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
		err = wal.Iter(it, func(wr *wal.WalIteratorResult) error {
			switch wr.Op {
			case SET:
				mem.Set(wr.Key, wr.Value)
			case DELETE:
				mem.Delete(wr.Key)
			default:
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return mem, nil
	}
	if len(wals) == 0 {
		seqNum := l.getNextSeqNum()
		l.wlw, err = l.wl.NewWAL(wal.WalLogNum(seqNum))
		if err != nil {
			return err
		}
		l.mem = memtable.New(seqNum)
		return nil
	}
	if len(wals) == 1 {
		l.mem, err = getMem(wals[0])
		if err != nil {
			return err
		}
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
	l.logger.Event("recoverWal", start)
	return nil
}

func (l *db) loadLevels() (err error) {
	start := time.Now()
	// load l0 levels
	if l.manifest == nil {
		return errors.New("Manifest not loaded")
	}
	if l.l0 == nil {
		l.l0 = newLevel(0, l.getSstPath(), l.blockCache)
	}
	err = l.l0.loadSSTs(l.manifest.L0)
	if err != nil {
		return err
	}

	// load the rest of levels
	if l.manifest.LevelCount > 1 {
		l.levels = make([]*level, l.manifest.LevelCount-1) // -1 because L0 is stored in separated field
		for i, ssts := range l.manifest.Levels {
			if l.levels[i] == nil {
				l.levels[i] = newLevel(i, l.getSstPath(), l.blockCache)
			}
			err = l.levels[i].loadSSTs(ssts)
			if err != nil {
				return err
			}
		}
	}
	l.logger.Event("loadLevels", start)
	return nil
}

func (l *db) Close() error {
	err := l.manifest.fsync()
	if err != nil {
		return err
	}
	return nil
}

func (l *db) new() (err error) {
	if err := common.EnsureDir(l.opts.path); err != nil {
		return err
	}
	l.manifest, err = newManifest(l.id, l.opts.path, l.opts.table, sst.BLOCK_SIZE, 7)
	if err != nil {
		return err
	}
	l.wl, err = wal.Init(wal.DefaultOpts.WithDir(path.Join(l.opts.path, common.WAL)))
	if err != nil {
		return err
	}
	l.wlw, err = l.wl.NewWAL(wal.WalLogNum(l.manifest.SeqNum))
	if err != nil {
		return err
	}
	sstPath := path.Join(l.opts.path, common.SST_DIR)
	if err := common.EnsureDir(sstPath); err != nil {
		return err
	}
	l.mem = memtable.New(l.getNextSeqNum())
	err = l.manifest.fsync()
	if err != nil {
		return err
	}
	// for now use global cache, maybe change so l0 has its own block cache
	l.l0 = newLevel(0, sstPath, l.blockCache)
	l.levels = make([]*level, 0)
	return nil
}

func (l *db) GetSize() int {
	return l.mem.GetSize()
}

func (l *db) Iterator() *skiplist.Iterator {
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

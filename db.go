package main

import (
	"crypto/sha256"
	"errors"
	"godb/sst"
	"os"
	"path"
	"sync"

	"godb/common"
	"godb/internal/cache"
	"godb/internal/skiplist"
	"godb/log"
	"godb/memtable"
	"godb/wal"
)

var (
	trace = log.NewLogger("db")
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
	mem        *memtable.MemTable // mutable
	sink       []*flushable       // immutable
	flushChan  chan *memtable.MemTable
	l0         *level
	levels     []*level
	wl         *wal.Wal
	blockCache *cache.Cache[[]byte]
	mutex      sync.Mutex
	opts       dbOpts
	manifest   *manifest
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
		mem:        memtable.New(),
		sink:       make([]*flushable, 0),
		blockCache: cache.New[[]byte](cache.WithVerbose[[]byte](true)),
		opts:       dbOpts,
	}

	switch _, err := os.Stat(dbOpts.path); {
	case err == nil:
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
	m, err := readManifest(l.opts.path)
	if err != nil {
		return err
	}
	if m.Id != l.id {
		return errors.New("id hash did not match")
	}
	l.manifest = m
	err = l.loadLevels()
	if err != nil {
		return err
	}

	return nil
}

func (l *db) recoverWal() (err error) {
	return nil
}

func (l *db) loadLevels() (err error) {
	// load l0 levels
	if l.manifest == nil {
		return errors.New("manifest not loaded")
	}
	if l.l0 == nil {
		l.l0 = newLevel(0, l.opts.path, l.blockCache)
	}
	err = l.l0.loadSSTs(l.manifest.L0)
	if err != nil {
		return err
	}

	// load the rest of levels
	l.levels = make([]*level, l.manifest.NLevels-1) // -1 because L0 is stored in separated field
	for lvl, ssts := range l.manifest.Levels {
		if l.levels[lvl] == nil {
			l.levels[lvl] = newLevel(lvl, l.opts.path, l.blockCache)
		}
		err = l.levels[lvl].loadSSTs(ssts)
		if err != nil {
			return err
		}
	}
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
	wl, err := wal.NewWal(wal.GetDefaultOpts(l.opts.path, l.mem.GetId().String()))
	if err != nil {
		panic(err)
	}

	l.wl = wl

	sstPath := path.Join(l.opts.path, common.SST_DIR)
	if err := common.EnsureDir(sstPath); err != nil {
		return err
	}
	l.manifest, err = newManifest(l.id, l.opts.path, l.opts.table, sst.BLOCK_SIZE, 7)
	if err != nil {
		return err
	}
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

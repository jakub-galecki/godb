package main

import (
	"crypto/sha256"
	"errors"
	"os"
	"path"
	"sync"

	"godb/common"
	"godb/internal/cache"
	"godb/internal/skiplist"
	"godb/level"
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
	id string

	mem  *memtable.MemTable   // mutable
	sink []*memtable.MemTable // immutable

	flushChan chan *memtable.MemTable

	l0 level.Level
	// l0Flushed sync.WaitGroup
	levels []level.Level

	// todo: manifest
	wl         *wal.Wal
	blockCache *cache.Cache[[]byte]

	mutex sync.Mutex

	opts dbOpts
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

	wl, err := wal.NewWal(wal.GetDefaultOpts(dbOpts.path))
	if err != nil {
		panic(err)
	}

	d := db{
		id:         string(sha256.New().Sum([]byte(table))),
		mem:        memtable.New(),
		sink:       make([]*memtable.MemTable, 0),
		wl:         wl,
		blockCache: cache.New[[]byte](cache.WithVerbose[[]byte](true)),
		opts:       dbOpts,
	}

	switch _, err := os.Stat(dbOpts.path); {
	case err == nil:
		err = d.tryRecover()
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

func (l *db) tryRecover() error {
	return nil
}

func (l *db) new() error {
	if err := common.EnsureDir(l.opts.path); err != nil {
		return err
	}

	sstPath := path.Join(l.opts.path, common.SST_DIR)
	if err := common.EnsureDir(sstPath); err != nil {
		return err
	}

	manifest, err := common.CreateFile(path.Join(l.opts.path, common.MANIFEST))
	if err != nil {
		return err
	}

	l.levels = make([]level.Level, 0)

	return nil
}

func (l *db) GetSize() int {
	return l.mem.GetSize()
}

func (l *db) Iterator() *skiplist.Iterator {
	return l.mem.Iterator()
}

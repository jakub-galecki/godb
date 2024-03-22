package main

import (
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"go.uber.org/zap"

	"godb/common"
	"godb/internal/cache"
	"godb/level"
	"godb/log"
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
	logger *zap.SugaredLogger
	table  string
	path   string

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
}

func NewStorageEngine(path, table string) StorageEngine {
	// core := rbt.NewRedBlackTree()
	var (
		err error

		cache   = cache.New[[]byte](10 * time.Second)
		storage = db{
			mem:        memtable.NewStorageCore(),
			logger:     log.InitLogger(),
			table:      table,
			blockCache: cache,
			l0:         level.NewLevel(0, path, table, cache),
			flushChan:  make(chan *memtable.MemTable),
		}
	)

	common.EnsureDir(path)
	storage.wl, err = wal.NewWal(nil)
	if err != nil {
		panic(err)
	}

	go storage.drainSink()

	return &storage
}

func (l *db) GetSize() int {
	return l.mem.GetSize()
}

func (l *db) Iterator() rbt.Iterator {
	return l.mem.Iterator()
}

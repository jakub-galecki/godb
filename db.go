package main

import (
	"time"

	"github.com/allegro/bigcache"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"go.uber.org/zap"

	"godb/level"
	"godb/log"
	"godb/memtable"
	"godb/wal"
)

type StorageEngine interface {
	Delete(key []byte)
	Set(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	GetSize() int

	// add iterator
}

type db struct {
	logger *zap.SugaredLogger
	table  string

	mem  *memtable.MemTable   // mutable
	sink []*memtable.MemTable // immutable

	l0     level.Level
	levels []level.Level

	// todo: manifest
	wl         wal.Wal
	blockCache *bigcache.BigCache
}

func NewStorageEngine(table string) StorageEngine {
	// core := rbt.NewRedBlackTree()
	config := bigcache.Config{
		Shards:             1024,
		LifeWindow:         10 * time.Minute,
		CleanWindow:        5 * time.Minute,
		MaxEntriesInWindow: 1000 * 10 * 60,
		MaxEntrySize:       500,
		Verbose:            true,
		HardMaxCacheSize:   4096,
	}
	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		panic(err)
	}
	storage := db{
		mem:        memtable.NewStorageCore(),
		logger:     log.InitLogger(),
		table:      table,
		blockCache: cache,
	}
	return &storage
}

func (l *db) GetSize() int {
	return l.mem.GetSize()
}

func (l *db) Iterator() rbt.Iterator {
	return l.mem.Iterator()
}

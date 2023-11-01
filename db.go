package main

import (
	"go.uber.org/zap"

	"godb/common"
	"godb/internal/rbt"
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

	mem  memtable.MemTable   // mutable
	sink []memtable.MemTable // immutable

	l0 level.Level

	levels []level.Level

	// operations regarding bloom filter
	// operations regarding sprase index
	// operations regarding sst

	// todo: manifest []string
	wl wal.Wal
}

func NewStorageEngine(table string) StorageEngine {
	core := rbt.NewRedBlackTree()
	storage := db{
		mem:    memtable.NewStorageCore(core),
		logger: log.InitLogger(),
		table:  table,
	}
	return &storage
}

func (l *db) GetSize() int {
	return l.mem.GetSize()
}

func (l *db) Iterator() common.Iterator {
	return l.mem.Iterator()
}

package lsmt

import (
	"godb/common"
	"godb/log"
	"godb/memtable"

	"go.uber.org/zap"
)

type StorageEngine interface {
	memtable.MemTable
	// operations regarding bloom filter
	// operations regarding sprase index
	// operations regarding sst
}

type lsmt struct {
	mem    memtable.MemTable
	logger *zap.SugaredLogger
	table  string
	// log wal.Wal
}

func NewStorageEngine(table string) StorageEngine {
	storage := lsmt{
		mem:    memtable.NewStorageCore(),
		logger: log.InitLogger(),
		table:  table,
	}
	return &storage
}

func (l *lsmt) Set(key, value []byte) []byte {
	l.logger.Debugf("Setting Key [%s] to value [%s]", key, value)
	return l.mem.Set(key, value)
}

func (l *lsmt) Get(key []byte) ([]byte, bool) {
	value, found := l.mem.Get(key)
	if found {
		return value, found
	}

	return nil, false
}

func (l *lsmt) Delete(key []byte) []byte {
	l.logger.Debugf("Deleting Key [%s]", key)
	return l.mem.Delete(key)
}

func (l *lsmt) GetSize() int {
	return l.mem.GetSize()
}

func (l *lsmt) Iterator() common.Iterator {
	return l.mem.Iterator()
}

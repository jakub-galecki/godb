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
	logger *zap.Logger
	table  string
	lsn    uint64
	// log wal.Wal
}

func NewStorageEngine() StorageEngine {
	var storage lsmt
	storage.mem = memtable.NewStorageCore()
	storage.logger = log.InitLogger()
	return &storage
}

func (l *lsmt) Set(key, value []byte) []byte {
	l.logger.Sugar().Debugf("Setting Key [%s] to value [%s]", key, value)
	return l.mem.Set(key, value)
}

func (l *lsmt) Get(key []byte) ([]byte, bool) {
	l.logger.Sugar().Debugf("Getting Key [%s]", key)
	return l.mem.Get(key)
}

func (l *lsmt) Delete(key []byte) []byte {
	l.logger.Sugar().Debugf("Deleting Key [%s]", key)
	return l.mem.Delete(key)
}

func (l *lsmt) GetSize() int {
	return l.mem.GetSize()
}

func (l *lsmt) Iterator() common.Iterator {
	return l.mem.Iterator()
}

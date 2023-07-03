package lsmt

import (
	"godb/common"
	"godb/level"
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
	logger *zap.SugaredLogger
	table  string

	mem  memtable.MemTable   // mutable
	sink []memtable.MemTable // immutable

	levels []level.Level
	// todo: manifest []string
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

func (l *lsmt) GetSize() int {
	return l.mem.GetSize()
}

func (l *lsmt) Iterator() common.Iterator {
	return l.mem.Iterator()
}

func (l *lsmt) exceededSize() bool {
	return l.mem.GetSize() == common.MAX_MEMTABLE_THRESHOLD
}

func (l *lsmt) moveToSink() {
	l.sink = append(l.sink, l.mem)
	l.mem = memtable.NewStorageCore()
}

func (l *lsmt) drainSink() {
	for _, mem := range l.sink {
		l.flushMemTable(mem)
	}
}

func (l *lsmt) flushMemTable(mem memtable.MemTable) error {
	if len(l.levels) == 0 {
		// init level
		newLevel := level.NewLevel(0, l.table)
		l.levels = append(l.levels, newLevel)
	}

	l0 := l.levels[0]
	if err := l0.AddMemtable(mem); err != nil {
		return err
	}
	return nil
}

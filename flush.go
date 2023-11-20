package main

import (
	"godb/common"
	"godb/level"
	"godb/memtable"
)

func (l *db) exceededSize() bool {
	return l.mem.GetSize() == common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() {
	l.mutex.Lock()
	l.sink = append(l.sink, l.mem)
	// core := rbt.NewRedBlackTree()
	l.mem = memtable.NewStorageCore()
	l.mutex.Unlock()
}

// todo: make asynchronous
func (l *db) drainSink() {
	var mem *memtable.MemTable

	l.mutex.Lock()
	if len(l.sink) > 0 {
		mem = l.sink[0]
	}
	l.mutex.Unlock()

	if mem != nil {
		l.l0Flushed.Add(1)
		if err := l.flushMemTable(mem); err != nil {
			l.logger.Error(err)
		}
		l.l0Flushed.Done()
	}
}

func (l *db) flushMemTable(mem *memtable.MemTable) error {
	if len(l.levels) == 0 {
		// init level
		newLevel := level.NewLevel(0, l.table, l.blockCache)
		l.levels = append(l.levels, newLevel)
	}

	if err := l.l0.AddMemtable(mem); err != nil {
		return err
	}
	return nil
}

func (l *db) maybeFlush(force bool) {
	if l.exceededSize() || force {
		l.moveToSink()
	}
}

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
	l.sink = append(l.sink, l.mem)
	// core := rbt.NewRedBlackTree()
	l.mem = memtable.NewStorageCore()
}

func (l *db) drainSink() {
	for _, mem := range l.sink {
		if err := l.flushMemTable(mem); err != nil {
			l.logger.Error(err)
		}
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
	l.maybeDrain(force)
}

func (l *db) maybeDrain(force bool) {
	if len(l.sink) == common.MAX_SINK_SIZE || force {
		l.drainSink()
	}
}

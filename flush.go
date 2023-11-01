package main

import (
	"godb/common"
	"godb/internal/rbt"
	"godb/level"
	"godb/memtable"
)

func (l *db) exceededSize() bool {
	return l.mem.GetSize() == common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() {
	l.sink = append(l.sink, l.mem)
	core := rbt.NewRedBlackTree()
	l.mem = memtable.NewStorageCore(core)
}

func (l *db) drainSink() {
	for _, mem := range l.sink {
		if err := l.flushMemTable(mem); err != nil {
			l.logger.Error(err)
		}
	}
}

func (l *db) flushMemTable(mem memtable.MemTable) error {
	if len(l.levels) == 0 {
		// init level
		newLevel := level.NewLevel(0, l.table)
		l.levels = append(l.levels, newLevel)
	}

	if err := l.l0.AddMemtable(mem); err != nil {
		return err
	}
	return nil
}

func (l *db) maybeFlush() {
	if l.exceededSize() {
		l.moveToSink()
	}
	l.maybeDrain()
}

func (l *db) maybeDrain() {
	if len(l.sink) == common.MAX_SINK_SIZE {
		l.drainSink()
	}
}

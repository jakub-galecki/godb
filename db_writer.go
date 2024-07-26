package main

import (
	"fmt"
	"time"

	"godb/common"
	"godb/memtable"
)

func (l *db) Set(key, value []byte) error {
	batch := newBatch().Set(key, value)
	return l.applyBatch(batch)
}

func (l *db) Delete(key []byte) error {
	batch := newBatch().Delete(key)
	return l.applyBatch(batch)
}

func (l *db) applyBatch(b *Batch) error {
	defer b.release()
	// start := time.Now()

	if b.committed.Load() {
		return fmt.Errorf("batch already commited")
	}

	if err := l.applyToWal(b); err != nil {
		return err
	}

	if err := applyToMemtable(l.mem, b); err != nil {
		return err
	}
	// log.Event("applyBatch", start)
	l.maybeFlush(b.forceFlush)
	return nil
}

func applyToMemtable(mem *memtable.MemTable, batch *Batch) error {
	for _, a := range batch.actions {
		switch a.kind {
		case common.SET:
			mem.Set(a.key, a.value)
		case common.DELETE:
			mem.Delete(a.key)
		default:
			panic("unhandled default case")
		}
	}
	batch.committed.Store(true)
	return nil
}

func (l *db) applyToWal(b *Batch) error {
	// todo: log entire batch
	start := time.Now()
	for _, a := range b.actions {
		err := l.wlw.Write(a.byte())
		if err != nil {
			return err
		}
	}
	l.logger.Event("applyToWal", start)
	return nil
}

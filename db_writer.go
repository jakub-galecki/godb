package main

import (
	"fmt"
	"time"

	"godb/log"
	"godb/memtable"
)

func (l *db) Set(key, value []byte) error {
	batch := newBatch().Set(key, value)
	return l.applyBatch(l.logger, batch)
}

func (l *db) Delete(key []byte) error {
	batch := newBatch().Delete(key)
	return l.applyBatch(l.logger, batch)
}

func (l *db) applyBatch(log *log.Logger, b *Batch) error {
	defer b.release()
	start := time.Now()

	if b.committed.Load() {
		return fmt.Errorf("batch already commited")
	}

	if err := l.applyToWal(b); err != nil {
		return err
	}

	if err := applyToMemtable(l.mem, b); err != nil {
		return err
	}
	log.Event("applyBatch", start)
	l.maybeFlush(b.forceFlush)
	return nil
}

func applyToMemtable(mem *memtable.MemTable, batch *Batch) error {
	for _, a := range batch.actions {
		switch a.kind {
		case SET:
			mem.Set(a.key, a.value)
		case DELETE:
			mem.Delete(a.key)
		default:
			panic("unhandled default case")
		}
	}
	batch.committed.Store(true)
	return nil
}

func (l *db) applyToWal(b *Batch) error {
	for _, a := range b.actions {
		err := l.wlw.Write(a.byte())
		if err != nil {
			return err
		}
	}
	return nil
}

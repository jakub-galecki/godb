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
	it := batch.Iter()
	for {
		op, key, val := it.Next()
		if op == 0 && key == nil && val == nil {
			// iteratior exhausted
			break
		}
		switch op {
		case common.SET:
			mem.Set(key, val)
		case common.DELETE:
			mem.Delete(key)
		default:
			panic("unknown db operation")
		}
	}
	batch.committed.Store(true)
	return nil
}

func (l *db) applyToWal(b *Batch) error {
	// todo: log entire batch
	start := time.Now()
	l.wlw.Write(b.encode())
	l.logger.Event("applyToWal", start)
	return nil
}

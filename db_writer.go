package main

import (
	"fmt"

	"godb/memtable"
)

func (l *db) Set(key, value []byte) error {
	batch := newBatch().Set(key, value)
	return l.applyBatch(batch)
}

func (l *db) applyBatch(b *Batch) error {
	if b.committed.Load() {
		return fmt.Errorf("batch already commited")
	}

	if err := l.writeToWal(b); err != nil {
		return err
	}

	if err := applyToMemtable(l.mem, b); err != nil {
		return err
	}

	l.maybeFlush(b.forceFlush)
	return nil
}

func (l *db) Delete(key []byte) {
	l.logger.Debugf("Deleting Key [%s]", key)
	l.mem.Delete(key)
	l.maybeFlush(false)
}

func applyToMemtable(mem *memtable.MemTable, batch *Batch) error {
	for _, a := range batch.actions {
		switch a.kind {
		case "SET":
			mem.Set(a.key, a.value)
		case "DEL":
			mem.Delete(a.key)
		}
	}
	batch.committed.Store(true)
	return nil
}

func (l *db) writeToWal(b *Batch) error {
	for _, a := range b.actions {
		b.wg.Add(1)
		l.wl.Write(a.repr(), b.wg)
		b.wg.Wait()
	}
	return nil
}

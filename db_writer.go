package main

import (
	"fmt"
)

func (l *db) Set(key, value []byte) error {
	l.logger.Debugf("Setting Key [%s] to value [%s]", key, value)
	batch := newBatch().Set(key, value)
	return l.applyBatch(batch)
}

func (l *db) applyBatch(b *batch) error {
	if b.committed.Load() {
		return fmt.Errorf("batch already commited")
	}

	// write to wal
	for _, a := range b.actions {
		if err := a.applyToMemtable(l.mem); err != nil {
			return err
		}
	}
	b.committed.Store(true)
	l.maybeFlush()
	return nil
}

func (l *db) Delete(key []byte) {
	l.logger.Debugf("Deleting Key [%s]", key)
	l.mem.Delete(key)

	if l.exceededSize() {
		l.moveToSink()
	}
}

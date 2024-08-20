package godb

import (
	"fmt"
	"time"

	"github.com/jakub-galecki/godb/common"
	"github.com/jakub-galecki/godb/memtable"
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
	// start := time.Now()
	defer b.release()
	if b.committed.Load() {
		return fmt.Errorf("batch already commited")
	}
	b.seqNum = l.getSeqNum(b.Size())
	err := func() error {
		l.mutex.Lock()
		defer l.mutex.Unlock()
		if err := l.applyToWal(b); err != nil {
			return err
		}
		if err := applyToMemtable(l.mem, b); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		return err
	}

	// log.Event("applyBatch", start)
	l.maybeFlush(b.forceFlush)
	return nil
}

func applyToMemtable(mem *memtable.MemTable, batch *Batch) error {
	it := batch.Iter()
	for {
		op, seq, key, val := it.Next()
		if op == 0 && key == nil && val == nil {
			// batch iterator exhausted
			break
		}
		switch op {
		case common.SET:
			_ = mem.Set(common.NewInternalKey(key, seq, common.SET), val)
		case common.DELETE:
			_ = mem.Delete(common.NewInternalKey(key, seq, common.DELETE))
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
	l.opts.logger.Event("applyToWal", start)
	return nil
}

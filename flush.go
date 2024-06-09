package main

import (
	"godb/common"
	"godb/memtable"
	"godb/wal"
	"time"
)

func (l *db) exceededSize() bool {
	// trace.Debug().Int("memtable_size", l.mem.GetSize())
	return l.mem.GetSize() > common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() error {
	l.mutex.Lock()
	l.sink = append(l.sink, l.mem)
	seq := l.getNextSeqNum()
	l.mem = memtable.New(seq)
	if err := l.rotateWal(seq); err != nil {
		return err
	}
	l.mutex.Unlock()
	return nil
}

func (l *db) drainSink() {
	for {
		var m *memtable.MemTable

		// todo: create atomic sink size ??
		// flush all memtables from the sink at once ??
		// remember to remove them only after they are flushed so that data
		// can be accepted
		if len(l.sink) > 0 {
			l.mutex.Lock()
			m = l.sink[0]
			l.mutex.Unlock()
		}

		if m != nil {
			// trace.Debug().Msg("got memtable to flush")

			if err := l.flush(m); err != nil {
				// trace.Error().Err(err).Msg("error while flushin memtable")
			}

			l.mutex.Lock()
			l.sink = l.sink[1:]
			l.mutex.Unlock()
		}
	}
}

func (l *db) flush(fl *memtable.MemTable) error {
	start := time.Now()
	l.mutex.Lock()
	defer l.mutex.Unlock()
	newSst, err := l.l0.AddMemtable(l, fl)
	if err != nil {
		return err
	}
	l.manifest.addSst(l.l0.id, newSst.GetId())
	if l.manifest.LastFlushedSeqNum > fl.GetLogSeqNum() {
		// weirdo
	}
	l.manifest.LastFlushedSeqNum = fl.GetLogSeqNum()
	// maybe delete older files
	if err := l.manifest.fsync(); err != nil {
		return err
	}
	l.logger.Event("flush", start)
	return nil
}

func (l *db) rotateWal(seqNum uint64) (err error) {
	l.wlw, err = l.wl.NewWAL(wal.WalLogNum(seqNum))
	if err != nil {
		return err
	}
	return nil
}

func (l *db) maybeFlush(force bool) {
	// trace.Debug().Int("maybe_flush", l.mem.GetSize())
	if l.exceededSize() || force {
		// trace.Debug().Int("size", l.mem.GetSize()).Msg("memtable size exceeded")
		err := l.moveToSink()
		if err != nil {
			// trace.Error().Err(err).Msg("error while moving to sink")
		}
	}
}

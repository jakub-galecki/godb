package main

import (
	"godb/common"
	"godb/memtable"
	"godb/wal"
)

// todo: make pool
type flushable struct {
	m *memtable.MemTable
	w *wal.Wal
}

func (l *db) exceededSize() bool {
	trace.Debug().Int("memtable_size", l.mem.GetSize())
	size := l.mem.GetSize()
	return size == common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() error {
	l.mutex.Lock()
	l.sink = append(l.sink, &flushable{
		m: l.mem,
		w: l.wl,
	})
	l.mem = memtable.New()
	if err := l.rotateWal(); err != nil {
		return err
	}
	l.mutex.Unlock()
	return nil
}

func (l *db) drainSink() {
	for {
		var f *flushable

		// todo: create atomic sink size ??
		// flush all memtables from the sink at once ??
		// remember to remove them only after they are flushed so that data
		// can be accepted

		l.mutex.Lock()
		if len(l.sink) > 0 {
			f = l.sink[0]
		}
		l.mutex.Unlock()

		if f != nil {
			trace.Debug().Msg("got memtable to flush")

			if err := l.flushMemTable(f); err != nil {
				trace.Error().Err(err).Msg("error while flushin memtable")
			}

			f.m = nil
			f.w = nil
			f = nil

			l.mutex.Lock()
			l.sink = l.sink[1:]
			l.mutex.Unlock()
		}
	}
}

func (l *db) flushMemTable(fl *flushable) error {
	if err := l.l0.AddMemtable(l, fl.m); err != nil {
		return err
	}

	err := fl.w.Delete()
	if err != nil {
		return err
	}

	if err := l.manifest.fsync(); err != nil {
		return err
	}

	return nil
}

func (l *db) rotateWal() (err error) {
	l.wl, err = wal.NewWal(wal.GetDefaultOpts(l.opts.path, l.mem.GetId().String()))
	if err != nil {
		return err
	}
	return nil
}

func (l *db) maybeFlush(force bool) {
	trace.Debug().Int("maybe_flush", l.mem.GetSize())
	if l.exceededSize() || force {
		trace.Debug().Int("size", l.mem.GetSize()).Msg("memtable size exceeded")
		l.moveToSink()
	}
}

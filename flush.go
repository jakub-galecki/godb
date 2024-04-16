package main

import (
	"godb/common"
	"godb/level"
	"godb/memtable"
)

func (l *db) exceededSize() bool {
	trace.Debug().Int("memtable_size", l.mem.GetSize())
	size := l.mem.GetSize()
	return size == common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() {
	l.mutex.Lock()
	l.sink = append(l.sink, l.mem)
	l.mem = memtable.New()
	l.mutex.Unlock()
}

func (l *db) drainSink() {
	for {
		var mem *memtable.MemTable

		// todo: create atomic sink size ??
		// flush all memtables from the sink at once ??
		// remember to remove them only after they are flushed so that data
		// can be accepted

		l.mutex.Lock()
		if len(l.sink) > 0 {
			mem = l.sink[0]
		}
		l.mutex.Unlock()

		if mem != nil {
			trace.Debug().Msg("got memtable to flush")

			if err := l.flushMemTable(mem); err != nil {
				trace.Error().Err(err).Msg("error while flushin memtable")
			}

			mem = nil

			l.mutex.Lock()
			l.sink = l.sink[1:]
			l.mutex.Unlock()
		}
	}
}

func (l *db) flushMemTable(mem *memtable.MemTable) error {
	if len(l.levels) == 0 {
		newLevel := level.NewLevel(0, l.opts.path, l.opts.table, l.blockCache)
		l.levels = append(l.levels, newLevel)
	}

	if err := l.l0.AddMemtable(mem); err != nil {
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

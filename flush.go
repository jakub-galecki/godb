package main

import (
	"godb/common"
	"godb/memtable"
	"godb/wal"
	"log"
	"time"
)

func (l *db) exceededSize() bool {
	l.opts.logger.Debug().Uint64("memtable_size", l.mem.GetSize())
	return l.mem.GetSize() > common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() error {
	l.mutex.Lock()
	l.sink = append(l.sink, l.mem)
	fnum := l.getNextFileNum()
	l.mem = memtable.New(fnum)
	if err := l.rotateWal(fnum); err != nil {
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
			l.opts.logger.Debug().Msg("got memtable to flush")

			if err := l.flush(m); err != nil {
				panic(err)
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
	if l.manifest.LastFlushedFileNumber > fl.GetFileNum() {
		log.Fatalf("last flushed seq num higher than memtable log seq num")
	}
	l.manifest.LastFlushedFileNumber = fl.GetFileNum()
	// maybe delete older files
	if err := l.manifest.fsync(); err != nil {
		return err
	}

	l.opts.logger.Event("flush", start)
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
	l.opts.logger.Debug().Uint64("maybe_flush", l.mem.GetSize())
	if l.exceededSize() || force {
		l.opts.logger.Debug().Uint64("size", l.mem.GetSize()).Msg("memtable size exceeded")
		err := l.moveToSink()
		if err != nil {
			l.opts.logger.Error().Err(err).Msg("error while moving to sink")
		}
	}
}

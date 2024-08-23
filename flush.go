package godb

import (
	"time"

	"github.com/jakub-galecki/godb/compaction"
	"github.com/jakub-galecki/godb/wal"

	"github.com/jakub-galecki/godb/common"
	"github.com/jakub-galecki/godb/memtable"
)

func (l *db) exceededSize() bool {
	l.opts.logger.Debug().Uint64("memtable_size", l.mem.GetSize())
	return l.mem.GetSize() > common.MAX_MEMTABLE_THRESHOLD
}

func (l *db) moveToSink() (err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sink = append(l.sink, l.mem)
	fnum := l.getNextFileNum()
	l.mem = memtable.New(fnum)
	l.wlw, err = l.wl.NewWAL(wal.WalLogNum(fnum))
	if err != nil {
		return err
	}
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
	newSST, err := l.l0.AddMemtable(fl)
	if err != nil {
		return err
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.appendL0Sst(newSST.GetId())
	l.setLastFlushedSeqNum(fl.GetFileNum())
	l.applyEnv(l)
	l.dbEnv.refresh(l.manifest)
	l.opts.logger.Event("flush", start)
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
	// read dbenv with lock
	cr := l.getCompactionReq()
	if cr, err := l.compaction.MaybeTriggerCompaction(cr); cr != nil && err == nil {
		go l.runCompaction(cr)
	}
}

func (l *db) runCompaction(req *compaction.CompactionReq) {

}

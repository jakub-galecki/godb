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

func (l *db) drainSink(exitCh chan struct{}) {
	for {
		select {
		case <-exitCh:
			return
		default:
			var m *memtable.MemTable

			l.mutex.Lock()
			if len(l.sink) > 0 {
				m = l.sink[0]
			}
			l.mutex.Unlock()

			if m != nil && !l.compacting.Load() {
				l.opts.logger.Debug().Msg("got memtable to flush")
				l.compacting.Store(true)
				l.compactWg.Add(1)
				if err := l.flush(m); err != nil {
					panic(err)
				}
				l.mutex.Lock()
				l.sink = l.sink[1:]
				l.mutex.Unlock()
				l.compacting.Store(false)
				l.compactWg.Done()
			}
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
	l.append(0, newSST)
	l.setLastFlushedSeqNum(fl.GetFileNum())
	if err := l.applyEnv(l); err != nil {
		return err
	}
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
	if !l.compacting.Load() {
		cr := l.getCompactionReq()
		cr, err := l.compaction.MaybeTriggerCompaction(cr)
		if cr != nil && err == nil {
			l.compacting.Store(true)
			l.compactWg.Add(1)
			go l.runCompaction(cr)
		}
	}
}

// runCompaction acquires l.mutex and runs compaction process. Mutex is dropped for IO operations.
func (l *db) runCompaction(req *compaction.CompactionReq) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	start := time.Now()
	l.opts.logger.Debug().
		RawJSON("compaction_request", req.Json()).
		Msg("running compaction process")
	res, err := l.compact(req)
	if err != nil {
		l.opts.logger.Error().Err(err).Msg("running compaction failed")
		return
	}
	l.applyCompaction(res)
	l.opts.logger.Debug().
		RawJSON("compaction_response", res.Json()).
		Msg("compaction complete")
	l.opts.logger.Event("runCompaction", start)
	l.compacting.Store(false)
	l.compactWg.Done()
}

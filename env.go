package godb

import (
	"sync/atomic"
)

type dbEnv struct {
	seqNum             atomic.Uint64
	lastFlushedFileNum atomic.Uint64
	l0                 []string
	levels             [][]string
}

func envFromManifest(m *Manifest) *dbEnv {
	env := &dbEnv{}
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	env.l0 = make([]string, 0)
	return env
}

func (env *dbEnv) refresh(m *Manifest) {
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	env.l0 = make([]string, 0)
}

func (env *dbEnv) getSeqNum(count int) uint64 {
	seq := env.seqNum.Add(uint64(count)) - uint64(count) + 1
	return seq
}

func (env *dbEnv) setLastFlushedSeqNum(fnum uint64) {
	if env.lastFlushedFileNum.Load() > fnum {
		panic("new last flushed file number is smaller")
	}
	env.lastFlushedFileNum.Store(fnum)
}

func (env *dbEnv) appendL0Sst(sstId string) {
	env.l0 = append(env.l0, sstId)
}

// requires to hold db lock
func (env *dbEnv) applyEnv(db *db) error {
	db.manifest.SeqNum = env.seqNum.Load()
	db.manifest.LastFlushedFileNumber = env.lastFlushedFileNum.Load()
	for _, sstId := range env.l0 {
		db.manifest.addSst(db.l0.GetId(), sstId)
	}
	return db.manifest.fsync()
}

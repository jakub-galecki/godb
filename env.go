package main

import "sync/atomic"

type dbEnv struct {
	seqNum             atomic.Uint64
	lastFlushedFileNum atomic.Uint64
}

func envFromManifest(m *Manifest) *dbEnv {
	env := &dbEnv{}
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	return env
}

func (env *dbEnv) getSeqNum(count int) uint64 {
	seq := env.seqNum.Add(uint64(count)) - uint64(count)
	return seq
}

func (env *dbEnv) setLastFlushedSeqNum(fnum uint64) {
	if env.lastFlushedFileNum.Load() > fnum {
		panic("new last flushed file number is smaller")
	}
	env.lastFlushedFileNum.Store(fnum)
}

// required to hold db lock
func (env *dbEnv) applyEnv(db *db) error {
	db.manifest.SeqNum = env.seqNum.Load()
	db.manifest.LastFlushedFileNumber = env.lastFlushedFileNum.Load()
	return db.manifest.fsync()
}

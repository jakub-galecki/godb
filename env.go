package main

import "sync/atomic"

type dbEnv struct {
	seqNum            atomic.Uint64
	lastFlushedSeqNum atomic.Uint64
}

func envFromManifest(m *Manifest) *dbEnv {
    env := &dbEnv{}
    env.seqNum.Store(m.SeqNum)
    env.lastFlushedSeqNum.Store(m.LastFlushedFileNumber)
    return env
}

package main

import "sync/atomic"

type dbEnv struct {
	seqNum            atomic.Uint64
	lastFlushedSeqNum uint64
	fileNum           uint64
}

func envFromManifest(m *Manifest) {

}

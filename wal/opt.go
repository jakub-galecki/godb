package wal

import (
	"time"
)

type Opts struct {
	Dir           string
	SyncInterval  time.Duration
	Encoder       func([]byte) []byte
	Sync          bool
	MinFlushedSeq uint64
	// TimeFormat   string
	// LOGER
	// todo: create Segment
}

var DefaultOpts = &Opts{
	SyncInterval: 50 * time.Millisecond,
	Encoder:      func(b []byte) []byte { return b },
	Sync:         true,
}

func (o *Opts) WithDir(dirName string) *Opts {
	o.Dir = dirName
	return o
}

func (o *Opts) WithMinFlushedSeq(seq uint64) *Opts {
	o.MinFlushedSeq = seq
	return o
}

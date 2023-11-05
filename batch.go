package main

import (
	"sync"
	"sync/atomic"
)

var batchPool = sync.Pool{New: func() interface{} { return new(Batch) }}

type Batch struct {
	actions actions

	committed atomic.Bool

	forceFlush bool
}

func newBatch(acs ...*action) *Batch {
	b := batchPool.Get().(*Batch)
	b.actions = acs
	return b
}

func (b *Batch) release() {
	batchPool.Put(b)
}

func (b *Batch) Set(key, value []byte) *Batch {
	newAction := newAction(key, value, "SET")
	b.actions = append(b.actions, &newAction)
	return b
}

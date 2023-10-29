package main

import (
	"sync"
	"sync/atomic"
)

var batchPool = sync.Pool{New: func() interface{} { return new(batch) }}

type batch struct {
	actions actions

	committed atomic.Bool
}

func newBatch(acs ...*action) *batch {
	b := batchPool.Get().(*batch)
	b.actions = acs
	return b
}

func (b *batch) release() {
	batchPool.Put(b)
}

func (b *batch) Set(key, value []byte) *batch {
	newAction := newAction(key, value, "SET")
	b.actions = append(b.actions, &newAction)
	return b
}

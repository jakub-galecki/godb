package sst

import (
	"godb/common"
)

// L0 iterator is different from other iterators in the sense that
// keys from underlying iterators can be overlapping, so we must always pick the newest one

type OverlappingIter struct {
	inner []common.Iterator
}

//func NewOverlappingIter(iters ...common.Iterator) *OverlappingIter {}

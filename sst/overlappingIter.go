package sst

import (
	"godb/common"
)

type HeapIter []common.Iterator

func (h HeapIter) Len() int           { return len(h) }
func (h HeapIter) Less(i, j int) bool { return h[i].Key().Compare(h[j].Key()) < 0 }
func (h HeapIter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *HeapIter) Push(x any) {
	it := x.(common.Iterator)
	*h = append(*h, it)
}
func (h *HeapIter) Pop() any {
	ol := *h
	n := len(*h)
	item := ol[n-1]
	ol[n-1] = nil
	*h = ol[0 : n-1]
	return item
}

// Overlappinig iterator is different from other iterators in the sense that
// keys from underlying iterators can be overlapping, so we must always pick the newest one

type OverlappingIter struct {
	inner []common.Iterator
	heap  HeapIter
}

func NewOverlappingIter(iters ...common.Iterator) *OverlappingIter {
	for _, it := range iters {

	}
}

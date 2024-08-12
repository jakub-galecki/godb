package compaction

import (
	"container/heap"
	"errors"
	"godb/common"
)

var (
	ErrEmptyIters = errors.New("provided empty slice of iterators")
)

type ikey = *common.InternalKey

// min heap to maintaint sst property, keys can be overlapping
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

type MergeIter struct {
	initIters []common.Iterator
	heap      HeapIter
}

// NewMergeIter creates a new MergeIter from given iterators. It assumes that iterators are already pointed to the first
// element.
func NewMergeIter(iters ...common.Iterator) (*MergeIter, error) {
	mi := &MergeIter{}
	// assume that all iters are valid
	mi.heap = make(HeapIter, len(iters))
	mi.initIters = make([]common.Iterator, len(iters))
	if len(iters) == 0 {
		return nil, ErrEmptyIters
	}

	for _, it := range iters {
		if !it.Valid() {
			continue
		}
		mi.initIters = append(mi.initIters, it)
		mi.heap = append(mi.heap, it)
	}
	if len(mi.heap) == 0 {
		return nil, ErrEmptyIters
	}
	heap.Init(&mi.heap)
	return mi, nil
}

func (mi *MergeIter) Next() (ikey, []byte, error) {
	cur := mi.heap[0]
	// check that other iters dont have the same key
	for i, it := range mi.heap {
		if cur.Key().Equal(it.Key()) {
			_, _, err := it.Next()
			if err != nil {
				heap.Remove(&mi.heap, i)
			}
		}
		if !it.Valid() {
			heap.Remove(&mi.heap, i)
		}
	}
	_, _, err := cur.Next()
	if err != nil {
		heap.Remove(&mi.heap, 0)
	}
	heap.Fix(&mi.heap, 0)
	for !mi.heap[0].Valid() {
		heap.Remove(&mi.heap, 0)
	}
	cur = mi.heap[0]
	return cur.Key(), cur.Value(), nil
}

func (mi *MergeIter) Key() ikey {
	if mi.heap[0] == nil {
		return nil
	}
	return mi.heap[0].Key()
}

func (mi *MergeIter) Value() []byte {
	if mi.heap[0] == nil {
		return nil
	}
	return mi.heap[0].Value()
}

func (mi *MergeIter) Valid() bool {
	if mi.heap[0] == nil {
		return false
	}
	return len(mi.heap[0].Key().UserKey) > 0
}

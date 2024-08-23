package compaction

import (
	"container/heap"
	"errors"

	"github.com/jakub-galecki/godb/common"
)

var (
	ErrEmptyIters = errors.New("provided empty slice of iterators")
)

type ikey = *common.InternalKey

// HeapIter 0 min heap to maintain sst property, keys can be overlapping
type HeapIter []common.Iterator

func (h HeapIter) Len() int { return len(h) }
func (h HeapIter) Less(i, j int) bool {
	return h[i].Key().Compare(h[j].Key()) < 0
}
func (h HeapIter) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
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

// MergeIter has keys from underlying iterators that can be overlapping, so we must always pick the smallest one
type MergeIter struct {
	initIters []common.Iterator
	heap      HeapIter
}

// NewMergeIter creates a new MergeIter from given iterators.
func NewMergeIter(iters ...common.Iterator) (*MergeIter, error) {
	mi := &MergeIter{}
	// assume that all iters are valid
	mi.heap = make(HeapIter, 0, len(iters))
	mi.initIters = make([]common.Iterator, 0, len(iters))
	if len(iters) == 0 {
		return nil, ErrEmptyIters
	}
	for _, it := range iters {
		if !it.Valid() {
			_, _, err := it.SeekToFirst()
			if err != nil {
				continue
			}
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
	_, _, err := mi.heap[0].Next()
	if err != nil {
		heap.Remove(&mi.heap, 0)
	}
	if len(mi.heap) == 0 {
		return nil, nil, common.ErrIteratorExhausted
	}
	heap.Fix(&mi.heap, 0)
	mi.maybeMoveIters()
	return mi.heap[0].Key(), mi.heap[0].Value(), nil
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

func (mi *MergeIter) SeekToFirst() (*common.InternalKey, []byte, error) {
	if len(mi.heap) == 0 || mi.heap[0] == nil || !mi.heap[0].Valid() {
		return nil, nil, common.ErrIteratorExhausted
	}
	mi.maybeMoveIters()
	return mi.heap[0].Key(), mi.heap[0].Value(), nil
}

func (mi *MergeIter) maybeMoveIters() {
	if len(mi.heap) == 0 {
		return
	}
	cur := mi.heap[0]
	// check that other iters dont have the same key
	for i, it := range mi.heap[1:] {
		if cur.Key().SoftEqual(it.Key()) {
			_, _, err := it.Next()
			if err != nil {
				heap.Remove(&mi.heap, i)
			}
			heap.Fix(&mi.heap, i)
		}
	}
}

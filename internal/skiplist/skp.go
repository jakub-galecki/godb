package skiplist

/*
Implementation based on:
	Maurice Herlihy and Nir Shavit. 2012. The Art of Multiprocessor Programming,
	Revised Reprint (1st. ed.). Morgan Kaufmann Publishers Inc., San Francisco, CA, USA.
*/

import (
	"bytes"
	"godb/common"
)

const (
	maxLevel = 16
)

type iKey = common.InternalKey

type SkipList struct {
	maxLevel  int
	height    int
	head      *node
	totalSize uint64
}

func NewSkipList() *SkipList {
	skp := &SkipList{maxLevel: maxLevel, height: 1}
	skp.head = newSentinelNode(maxLevel + 1)
	return skp
}

func (skp *SkipList) find(key *iKey, preds []*node) *node {
	var (
		next,
		prev *node
	)

	prev = skp.head
	for i := skp.height - 1; i >= 0; i-- {
		for next = prev.forwards[i]; next != nil; next = prev.forwards[i] {
			if key.Compare(next.key) <= 0 {
				break
			}
			prev = next
		}
		preds[i] = prev
	}
	// soft comparison of only userKeys to ensure that when we search for SearchInternalKey whose
	// sequenceNumber is max we find it
	if next != nil && bytes.Equal(key.UserKey, next.key.UserKey) {
		return next
	}
	return nil
}

func (skp *SkipList) Get(key []byte) ([]byte, bool) {
	var (
		skey  = common.SearchInternalKey(key)
		preds = make([]*node, maxLevel)
	)
	res := skp.find(skey, preds)
	if res != nil {
		return res.value, true
	}
	return nil, false
}

func (skp *SkipList) Set(key *iKey, value []byte) error {
	var (
		preds = make([]*node, maxLevel)
	)
	res := skp.find(key, preds)
	if res != nil && res.key.Equal(key) {
		return common.ErrKeyAlreadyExists
	}
	dstLvl := randomLevel()
	n := newNode(key, value, dstLvl)
	for i := 0; i < dstLvl; i++ {
		prev := preds[i]
		if prev == nil {
			prev = skp.head
		}
		n.forwards[i] = prev.forwards[i]
		prev.forwards[i] = n
	}
	if dstLvl >= skp.height {
		skp.height = dstLvl
	}
	skp.totalSize += uint64(key.GetSize() + len(value))
	return nil
}

func (skp *SkipList) GetSize() uint64 {
	return skp.totalSize
}

type iterator struct {
	cur *node
}

func (skp *SkipList) NewIter() common.InnerStorageIterator {
	return &iterator{skp.head}
}

func (it *iterator) Valid() bool {
	return it.cur.forwards[0] != nil
}

func (it *iterator) Next() (*iKey, []byte) {
	it.cur = it.cur.forwards[0]
	if it.cur == nil {
		return nil, nil
	}
	return it.cur.key, it.cur.value
}

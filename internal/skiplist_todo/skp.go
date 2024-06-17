package skiplisttodo 

import (
	"bytes"
	"math/rand"
)

type SkipList struct {
	head     *node
	maxLevel int
	size     int
	bytes    int
}

type node struct {
	key   []byte
	value []byte
	next  []*node
}

func newNode(key, value []byte, lvl int) *node {
	n := &node{
		key:   key,
		value: value,
		next:  make([]*node, lvl),
	}
	return n
}

func New(maxLvl int) *SkipList {
	skl := &SkipList{
		maxLevel: maxLvl,
	}
	skl.head = &node{
		next: make([]*node, maxLvl),
	}
	return skl
}

func (skl *SkipList) Reset() {}

func (skl *SkipList) Get(key []byte) ([]byte, bool) {
	var (
		head = skl.head
	)

	for i := skl.maxLevel - 1; i >= 0; i-- {
		current := head.next[i]
		for ; current != nil; current = current.next[i] {
			cmp := bytes.Compare(current.key, key)
			if cmp == 0 {
				return current.value, true
			} else if cmp > 0 {
				break
			}
			head = current
		}
	}
	return nil, false
}

func (skl *SkipList) Set(key, value []byte) {
	prevNodes := skl.getPreviousNodes(key)

	if prevNodes[0].next[0] != nil && bytes.Equal(prevNodes[0].next[0].key, key) {
		prevNodes[0].next[0].value = value
		return
	}

	lvl := randomLevel(skl.maxLevel)
	n := newNode(key, value, lvl)

	for i := range n.next {
		n.next[i] = prevNodes[i].next[i]
		prevNodes[i].next[i] = n
	}

	skl.size++
	skl.bytes += len(key) + len(value)
}

func (skl *SkipList) GetSize() int {
	return skl.bytes
}

func (skl *SkipList) getPreviousNodes(key []byte) []*node {
	previousNodes := make([]*node, skl.maxLevel)

	head := skl.head
	for i := skl.maxLevel - 1; i >= 0; i-- {
		for current := head.next[i]; current != nil; current = current.next[i] {
			if cmp := bytes.Compare(current.key, key); cmp >= 0 {
				break
			}
			head = current
		}
		previousNodes[i] = head
	}

	return previousNodes
}

func randomLevel(maxLevel int) int {
	lvl := 1

	for lvl < maxLevel && rand.Intn(4) == 0 {
		lvl++
	}

	return lvl
}

func (skl *SkipList) NewIterator() *Iterator {
	return &Iterator{
		cursor: skl.head,
	}
}

type Iterator struct {
	cursor *node
}

func (it *Iterator) Next() bool {
	if len(it.cursor.next) == 0 || it.cursor.next[0] == nil {
		return false
	}

	it.cursor = it.cursor.next[0]
	return true
}

func (it *Iterator) Key() []byte {
	return it.cursor.key
}

func (it *Iterator) Value() []byte {
	return it.cursor.value
}

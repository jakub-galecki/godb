/*
Implementation based on:
Maurice Herlihy and Nir Shavit. 2012. The Art of Multiprocessor Programming,
Revised Reprint (1st. ed.). Morgan Kaufmann Publishers Inc., San Francisco, CA, USA.
*/
package skiplistlockfree

import (
	"math/rand"
	"sync/atomic"
)

type markableRef[T any] struct {
	ref    *T
	marked bool
}

func newMarkableRef[T any](ref *T, marked bool) *markableRef[T] {
	return &markableRef[T]{ref, marked}
}

type atomicMarkableRef[T any] struct {
	ref atomic.Pointer[markableRef[T]]
}

func newAtomicMarkableRef[T any](ref *T, marked bool) *atomicMarkableRef[T] {
	ptr := atomic.Pointer[markableRef[T]]{}
	ptr.Store(newMarkableRef(ref, marked))
	return &atomicMarkableRef[T]{ref: ptr}
}

func (a *atomicMarkableRef[T]) getRef() *T {
	return a.ref.Load().ref
}

func (a *atomicMarkableRef[T]) getMark() bool {
	return a.ref.Load().marked
}

func (a *atomicMarkableRef[T]) set(ref *T, marked bool) {
	cur := a.ref.Load()
	if cur.ref != ref || cur.marked != marked {
		a.ref.Store(newMarkableRef(ref, marked))
	}
}

func (a *atomicMarkableRef[T]) compAndSet(expectedRef, newRef *T, expectedMark, newMark bool) bool {
	cur := a.ref.Load()
	if expectedRef != cur.ref && expectedMark != cur.marked {
		return false
	}
	if newRef == cur.ref && newMark == cur.marked {
		return true
	}
	return a.ref.CompareAndSwap(cur, newMarkableRef(newRef, newMark))
}

type node[T comparable] struct {
	key,
	value T
	forwards []*atomicMarkableRef[node[T]]
}

func newNode[T comparable](key, value T, level int) *node[T] {
	res := &node[T]{
		key:      key,
		value:    value,
		forwards: make([]*atomicMarkableRef[node[T]], level+1),
	}
	for i := range res.forwards {
		res.forwards[i] = newAtomicMarkableRef(&node[T]{}, false)
	}
	return res
}

func newSentinelNode[T comparable](level int) *node[T] {
	res := &node[T]{
		forwards: make([]*atomicMarkableRef[node[T]], level),
	}
	for i := range res.forwards {
		res.forwards[i] = newAtomicMarkableRef(&node[T]{}, false)
	}
	return res
}

type SkipList[T comparable] struct {
	maxLevel int
	head     *node[T]
	tail     *node[T]
}

func NewSkipList[T comparable](maxLvl int) *SkipList[T] {
	skp := &SkipList[T]{maxLevel: maxLvl}
	skp.head = newSentinelNode[T](maxLvl)
	skp.tail = newSentinelNode[T](maxLvl)
	for i := range skp.head.forwards {
		skp.head.forwards[i].set(skp.tail, false)
	}
	return skp
}

func (skp *SkipList[T]) find(key T) {

}

func randomLevel(maxLevel int) int {
	lvl := 1
	for lvl < maxLevel && rand.Intn(4) == 0 {
		lvl++
	}
	return lvl
}

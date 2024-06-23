package skiplist

import "sync/atomic"

type markableRef[T any] struct {
	ref    *T
	marked bool
}

func newMarkableRef[T any](ref *T, marked bool) *markableRef[T] {
	return &markableRef[T]{ref, marked}
}

type atomicMarkableRef[T any] struct {
	ref *atomic.Pointer[markableRef[T]]
}

func newAtomicMarkableRef[T any](ref *T, marked bool) *atomicMarkableRef[T] {
	ptr := &atomic.Pointer[markableRef[T]]{}
	ptr.Store(newMarkableRef(ref, marked))
	return &atomicMarkableRef[T]{ref: ptr}
}

func (a *atomicMarkableRef[T]) getRef() *T {
	return a.ref.Load().ref
}

func (a *atomicMarkableRef[T]) getMark() bool {
	return a.ref.Load().marked
}

func (a *atomicMarkableRef[T]) get(mark *bool) *T {
	cur := a.ref.Load()
	*mark = cur.marked
	return cur.ref
}

func (a *atomicMarkableRef[T]) set(ref *T, marked bool) {
	cur := a.ref.Load()
	if cur.ref != ref || cur.marked != marked {
		a.ref.Store(newMarkableRef(ref, marked))
	}
}

func (a *atomicMarkableRef[T]) setMark(marked bool) {
	cur := a.ref.Load()
	if cur.marked != marked {
		a.ref.Store(newMarkableRef(cur.ref, marked))
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

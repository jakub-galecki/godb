package memtable

import (
	"github.com/stretchr/testify/assert"
	"godb/common"
	"testing"
)

func Test_Basic(t *testing.T) {
	mem := New(0)
	var seq uint64 = 0
	makeSetInKey := func(k []byte) *common.InternalKey {
		ik := common.NewInternalKey(k, seq, common.SET)
		seq++
		return ik
	}
	makeDelInKey := func(k []byte) *common.InternalKey {
		ik := common.NewInternalKey(k, seq, common.DELETE)
		seq++
		return ik
	}
	assertGet := func(key, val []byte) {
		got, found := mem.Get(key)
		assert.True(t, found)
		assert.Equal(t, val, got)
	}
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo")), []byte("bar")))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo1")), []byte("bar1")))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo2")), []byte("bar2")))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo3")), []byte("bar3")))
	assertGet([]byte("foo"), []byte("bar"))
	assertGet([]byte("foo1"), []byte("bar1"))
	assertGet([]byte("foo2"), []byte("bar2"))
	assertGet([]byte("foo3"), []byte("bar3"))
	assert.Equal(t, uint64(62), mem.GetSize())
	// sequenceNumber "overriding" previous values
	assert.NoError(t, mem.Delete(makeDelInKey([]byte("foo"))))
	assertGet([]byte("foo"), common.TOMBSTONE)
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo")), []byte("newValue")))
	assertGet([]byte("foo"), []byte("newValue"))

	assert.Equal(t, uint64(0), mem.GetFileNum())
}

func Test_Iterator(t *testing.T) {
	mem := New(0)
	var seq uint64 = 0
	makeSetInKey := func(k []byte) *common.InternalKey {
		ik := common.NewInternalKey(k, seq, common.SET)
		seq++
		return ik
	}
	makeDelInKey := func(k []byte) *common.InternalKey {
		ik := common.NewInternalKey(k, seq, common.DELETE)
		seq++
		return ik
	}
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo")), []byte("bar")))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo1")), []byte("bar1")))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo2")), []byte("bar2")))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo3")), []byte("bar3")))
	assert.NoError(t, mem.Delete(makeDelInKey([]byte("foo"))))
	assert.NoError(t, mem.Set(makeSetInKey([]byte("foo")), []byte("newValue")))

	it := mem.Iterator()
	k, v := it.Next()
	assert.Equal(t, k, common.NewInternalKey([]byte("foo"), 5, common.SET))
	assert.Equal(t, v, []byte("newValue"))

	k, v = it.Next()
	assert.Equal(t, k, common.NewInternalKey([]byte("foo"), 4, common.DELETE))
	assert.Equal(t, v, common.TOMBSTONE)

	k, v = it.Next()
	assert.Equal(t, k, common.NewInternalKey([]byte("foo"), 0, common.SET))
	assert.Equal(t, v, []byte("bar"))

	k, v = it.Next()
	assert.Equal(t, k, common.NewInternalKey([]byte("foo1"), 1, common.SET))
	assert.Equal(t, v, []byte("bar1"))

	k, v = it.Next()
	assert.Equal(t, k, common.NewInternalKey([]byte("foo2"), 2, common.SET))
	assert.Equal(t, v, []byte("bar2"))

	k, v = it.Next()
	assert.Equal(t, k, common.NewInternalKey([]byte("foo3"), 3, common.SET))
	assert.Equal(t, v, []byte("bar3"))
}

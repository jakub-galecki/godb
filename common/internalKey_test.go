package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SerializeDeserilze(t *testing.T) {
	key := NewInternalKey([]byte("testKey"), 10000, SET)
	serialized := key.Serialize()
	deser := DeserializeKey(serialized)
	assert.Equal(t, uint64(10000), deser.SeqNum())
	assert.Equal(t, SET, deser.Kind())
	require.Equal(t, key, deser)

	key = NewInternalKey([]byte("testKey"), 1001111, DELETE)
	serialized = key.Serialize()
	deser = DeserializeKey(serialized)
	assert.Equal(t, uint64(1001111), deser.SeqNum())
	assert.Equal(t, DELETE, deser.Kind())
	require.Equal(t, key, deser)
}

func TestStrictCompare(t *testing.T) {
	key1 := NewInternalKey([]byte("a"), 1000, SET)
	key2 := NewInternalKey([]byte("b"), 200, SET)

	// a < b
	require.Equal(t, -1, key1.Compare(key2))
	// b > a
	require.Equal(t, 1, key2.Compare(key1))
	require.False(t, key1.Equal(key2))

	// a == a
	key2 = NewInternalKey([]byte("a"), 1000, SET)
	require.Equal(t, 0, key1.Compare(key2))
	require.True(t, key1.Equal(key2))

	// same keys, different sequenceNumbers
	key2 = NewInternalKey([]byte("a"), 1001, SET)

	// userKeys are equal, higher sequence number is "smaller"
	// so in this case {"a", 1000} > {"a", 1001}
	require.Equal(t, 1, key1.Compare(key2))
	require.Equal(t, -1, key2.Compare(key1))
}

func Test_SoftCompare(t *testing.T) {
	// compare only user keys
	key1 := NewInternalKey([]byte("a"), 1000, SET)
	key2 := NewInternalKey([]byte("b"), 200, SET)

	// a < b
	require.Equal(t, -1, key1.SoftCompare(key2))
	// b > a
	require.Equal(t, 1, key2.SoftCompare(key1))
	require.False(t, key1.SoftEqual(key2))

	// a == a
	key2 = NewInternalKey([]byte("a"), 1001, SET)
	require.Equal(t, 0, key1.SoftCompare(key2))
	require.True(t, key1.SoftEqual(key2))

	// same keys, different sequenceNumbers
	key2 = NewInternalKey([]byte("a"), 1001, SET)
	require.Equal(t, 0, key1.SoftCompare(key2))
}

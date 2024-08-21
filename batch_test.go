package godb

import (
	"fmt"
	"testing"

	"github.com/jakub-galecki/godb/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewBatch(t *testing.T) {
	b := NewBatch()
	assert.NotNil(t, b)
	b.Set([]byte("test0"), []byte("testVal"))
	b.Set([]byte("test1"), []byte("testVal"))
	b.Set([]byte("test2"), []byte("testVal"))
	b.Set([]byte("test3"), []byte("testVal"))
}

func Test_BatchIter(t *testing.T) {
	b := NewBatch()
	b.seqNum = 0
	assert.NotNil(t, b)
	b.Set([]byte("test0"), []byte("testVal"))
	b.Set([]byte("test1"), []byte("testVal"))
	b.Set([]byte("test2"), []byte("testVal"))
	b.Set([]byte("test3"), []byte("testVal"))
	b.Delete([]byte("test4"))

	it := b.Iter()
	assert.NotNil(t, it)
	i := 0
	for {
		op, seq, key, val := it.Next()
		if op == 0 && key == nil && val == nil {
			require.Equal(t, i, 5)
			return
		}
		assert.Equal(t, uint64(i), seq)
		if i == 4 {
			require.Equal(t, op, common.DELETE)
			require.Equal(t, key, []byte(fmt.Sprintf("test%d", i)))
			require.Nil(t, val)
		} else {
			require.Equal(t, op, common.SET)
			require.Equal(t, key, []byte(fmt.Sprintf("test%d", i)))
			require.Equal(t, val, []byte("testVal"))
		}
		i++
	}
}

func Test_Encode(t *testing.T) {
	b := NewBatch()
	assert.NotNil(t, b)
	b.Set([]byte("test0"), []byte("testVal"))
	b.Set([]byte("test1"), []byte("testVal"))
	b.Set([]byte("test2"), []byte("testVal"))
	b.Set([]byte("test3"), []byte("testVal"))

	encoded := b.encode()
	assert.Equal(t, encoded, []byte("\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05test0\atestVal\x00\x05test1\atestVal\x00\x05test2\atestVal\x00\x05test3\atestVal"))
}

func Test_Decode(t *testing.T) {
	b := NewBatch()
	assert.NotNil(t, b)
	b.seqNum = 100
	b.Set([]byte("test0"), []byte("testVal"))
	b.Set([]byte("test1"), []byte("testVal"))
	b.Set([]byte("test2"), []byte("testVal"))
	b.Set([]byte("test3"), []byte("testVal"))
	b.Delete([]byte("test4"))

	encoded := b.encode()

	newB := NewBatch()
	newB.decode(encoded)

	assert.Equal(t, uint32(5), newB.size)

	assert.Equal(t, uint64(100), b.seqNum)
	assert.Equal(t, uint64(100), newB.seqNum)

	it := b.Iter()
	assert.NotNil(t, it)
	i := 0
	for {
		op, seq, key, val := it.Next()
		if op == 0 && key == nil && val == nil {
			require.Equal(t, i, 5)
			return
		}
		assert.Equal(t, uint64(i+100), seq)
		if i == 4 {
			require.Equal(t, op, common.DELETE)
			require.Equal(t, key, []byte(fmt.Sprintf("test%d", i)))
			require.Nil(t, val)
		} else {
			require.Equal(t, op, common.SET)
			require.Equal(t, key, []byte(fmt.Sprintf("test%d", i)))
			require.Equal(t, val, []byte("testVal"))
		}
		i++
	}
}

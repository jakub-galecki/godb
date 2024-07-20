package skiplist

import (
	"fmt"
	"godb/common"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func Test_Basic(t *testing.T) {
	skl := NewSkipList()
	for i := 0; i < 10000; i++ {
		rawk := []byte(fmt.Sprintf("foo.%d", i))
		rawv := []byte(fmt.Sprintf("bar.%d", i))
		assert.NoError(t, skl.Set(common.NewInternalKey(rawk, uint64(i), common.SET), rawv))
	}

	for i := 0; i < 10000; i++ {
		rawk := []byte(fmt.Sprintf("foo.%d", i))
		rawv := []byte(fmt.Sprintf("bar.%d", i))
		v, f := skl.Get(rawk)
		assert.True(t, f)
		assert.Equal(t, v, rawv)
	}
	assert.True(t, true)
}

func Test_TheSameKey(t *testing.T) {
	skl := NewSkipList()
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 0, common.SET), []byte("bar.1")))
	require.Error(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 0, common.SET), []byte("bar.1")))
}

func Test_VersioningKey(t *testing.T) {
	skl := NewSkipList()
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 0, common.SET), []byte("bar.1")))
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 1, common.SET), []byte("bar.2")))
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 2, common.SET), []byte("bar.3")))
	v, f := skl.Get([]byte("foo.1"))
	assert.True(t, f)
	assert.Equal(t, v, []byte("bar.3"))
}

func Test_Iterator(t *testing.T) {
	skl := NewSkipList()
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 0, common.SET), []byte("bar.1")))
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 1, common.SET), []byte("bar.2")))
	require.NoError(t, skl.Set(common.NewInternalKey([]byte("foo.1"), 2, common.SET), []byte("bar.3")))

	it := skl.NewIter()
	assert.NotNil(t, it)

	var (
		key *iKey
		val []byte
	)

	require.True(t, it.HasNext())
	key, val = it.Next()
	require.Equal(t, common.NewInternalKey([]byte("foo.1"), 2, common.SET), key)
	assert.Equal(t, []byte("bar.3"), val)

	require.True(t, it.HasNext())
	key, val = it.Next()
	require.Equal(t, common.NewInternalKey([]byte("foo.1"), 1, common.SET), key)
	assert.Equal(t, []byte("bar.2"), val)

	require.True(t, it.HasNext())
	key, val = it.Next()
	require.Equal(t, common.NewInternalKey([]byte("foo.1"), 0, common.SET), key)
	assert.Equal(t, []byte("bar.1"), val)

	require.False(t, it.HasNext())
}

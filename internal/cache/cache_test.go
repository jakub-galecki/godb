package cache

import (
	"github.com/stretchr/testify/assert"
	"godb/common"
	"strconv"
	"testing"
	"time"
)

func Test_Basic(t *testing.T) {
	c := New[string]()
	assert.NoError(t, c.Set("foo", "bar"))
	assert.True(t, c.Has("foo"))
	got, err := c.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)

	assert.NoError(t, c.Set("foo", "baz"))
	got, err = c.Get("foo")
	assert.NoError(t, err)
	assert.True(t, c.Has("foo"))
	assert.Equal(t, "baz", got)
}

func Test_Expiration(t *testing.T) {
	c := New[string]()
	assert.NoError(t, c.Set("foo", "bar"))
	assert.True(t, c.Has("foo"))
	got, err := c.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)
	time.Sleep(5 * time.Second)

	assert.False(t, c.Has("foo"))
	got, err = c.Get("foo")
	assert.Equal(t, "", got)
	assert.Error(t, common.ErrKeyNotFound, err)
}

func BenchmarkCacheSet(b *testing.B) {
	c := New[string]()
	for n := 0; n < b.N; n++ {
		_ = c.Set("foo"+strconv.Itoa(n), "bar")
	}
}

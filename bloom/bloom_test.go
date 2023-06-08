package bloom

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBloom(t *testing.T) {
	bloomFilter := NewFilter(256)
	bloomFilter.AddKey([]byte("foo"))
	bloomFilter.AddKey([]byte("bar"))
	bloomFilter.AddKey([]byte("123"))

	assert.True(t, bloomFilter.MayContain([]byte("foo")))
	assert.True(t, bloomFilter.MayContain([]byte("bar")))
	assert.True(t, bloomFilter.MayContain([]byte("123")))
	assert.False(t, bloomFilter.MayContain([]byte("fizz")))
}

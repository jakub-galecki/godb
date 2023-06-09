package bloom

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestBloomFs(t *testing.T) {
	bloomFilter := NewFilter(256)
	bloomFilter.AddKey([]byte("foo"))
	bloomFilter.AddKey([]byte("bar"))
	bloomFilter.AddKey([]byte("123"))
	f := new(bytes.Buffer)
	assert.NoError(t, bloomFilter.Write(f))

	newBloom := NewFilter(256)
	assert.NoError(t, newBloom.Read(f))
	assert.True(t, newBloom.MayContain([]byte("foo")))
	assert.True(t, newBloom.MayContain([]byte("bar")))
	assert.True(t, newBloom.MayContain([]byte("123")))
	assert.False(t, newBloom.MayContain([]byte("fizz")))
	assert.False(t, newBloom.MayContain([]byte("324")))
}

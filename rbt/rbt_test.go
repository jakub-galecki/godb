package rbt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	tree := NewRedBlackTree()
	tests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("0"), []byte("1")},
		{[]byte("1"), []byte("8")},
		{[]byte("2"), []byte("41")},
		{[]byte("foo"), []byte("bar")},
		{[]byte("fizz"), []byte("buzz")},
	}

	for _, test := range tests {
		xs := tree.Set(test.key, test.value)
		assert.Nil(t, xs)
	}

	for _, test := range tests {
		xs, ok := tree.Get(test.key)
		assert.True(t, ok)
		assert.Equal(t, xs, test.value)
	}

	assert.Equal(t, tree.GetSize(), 5)
}

func TestUpdate(t *testing.T) {
	tree := NewRedBlackTree()
	tests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("0"), []byte("1")},
		{[]byte("foo"), []byte("bar")},
		{[]byte("fizz"), []byte("buzz")},
	}

	for _, test := range tests {
		xs := tree.Set(test.key, test.value)
		assert.Nil(t, xs)
	}

	xs := tree.Set([]byte("foo"), []byte("override"))
	assert.Equal(t, xs, []byte("bar"))

	xs = tree.Set([]byte("fizz"), []byte("override"))
	assert.Equal(t, xs, []byte("buzz"))

	xs, ok := tree.Get([]byte("0"))
	assert.True(t, ok)
	assert.Equal(t, xs, []byte("1"))

	assert.Equal(t, tree.GetSize(), 3)
}

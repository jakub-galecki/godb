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
		tree.Set(test.key, test.value)
	}

	for _, test := range tests {
		xs, ok := tree.Get(test.key)
		assert.True(t, ok)
		assert.Equal(t, xs, test.value)
	}

	assert.Equal(t, tree.GetSize(), 11)
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
		tree.Set(test.key, test.value)
	}
	tree.Set([]byte("foo"), []byte("override"))

	tree.Set([]byte("fizz"), []byte("override"))

	xs, ok := tree.Get([]byte("0"))
	assert.True(t, ok)
	assert.Equal(t, xs, []byte("1"))

	assert.Equal(t, tree.GetSize(), 17)
}

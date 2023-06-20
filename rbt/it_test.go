package rbt

import (
	"bytes"
	"errors"
	"godb/common"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator(t *testing.T) {
	tree := NewRedBlackTree()
	tests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("0"), []byte("1")},
		{[]byte("1"), []byte("8")},
		{[]byte("3"), []byte("41")},
		{[]byte("4"), []byte("3121")},
		{[]byte("2"), []byte("1232131")},
	}

	for _, test := range tests {
		xs := tree.Set(test.key, test.value)
		assert.Nil(t, xs)
	}

	sort.SliceStable(tests, func(i, j int) bool {
		return bytes.Compare(tests[i].key, tests[j].key) == -1
	})

	it := tree.Iterator()
	i := 0
	for it.HasNext() {
		key, value, err := it.Next()
		if err != nil && !errors.Is(err, common.EndOfIterator) {
			t.Error(err)
		}
		assert.Equal(t, tests[i].key, key)
		assert.Equal(t, tests[i].value, value)
		i++
	}

}

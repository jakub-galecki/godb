package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCore(t *testing.T) {
	lsmt := NewStorageEngine("test")
	for i := 0; i < 200000; i++ {
		_ = lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}
	for i := 0; i < 200000; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("bar.%d", i)), val)
	}
	//lsmt.Delete([]byte("foo"))
}

package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func clearDb(name string) error {
// }

func TestCore(t *testing.T) {
	lsmt := Open("tt")
	for i := 0; i < 1000000; i++ {
		err := lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		assert.NoError(t, err)
	}
	for i := 0; i < 1000000; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("bar.%d", i)), val)
	}

	//lsmt.Delete([]byte("foo"))
}

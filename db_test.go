package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func clearDb(name string) error {
// }

func TestCore(t *testing.T) {
	lsmt := Open("tt6")
	for i := 0; i < 1000000; i++ {
		err := lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		assert.NoError(t, err)
	}
	for i := 0; i < 1; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		require.Truef(t, found, "key %s not found", fmt.Sprintf("foo.%d", i))
		require.Equal(t, []byte(fmt.Sprintf("bar.%d", i)), val)
	}
	//lsmt.Delete([]byte("foo"))
}

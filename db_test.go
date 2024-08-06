package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/stretchr/testify/require"
)

// func clearDb(name string) error {
// }

func TestCore(t *testing.T) {
	lsmt, err := Open("111333112311111112121")
	assert.NoError(t, err)
	for i := 0; i < 100000; i++ {
		err := lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		require.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		require.Truef(t, found, "key %s not found", fmt.Sprintf("foo.%d", i))
		require.Equal(t, []byte(fmt.Sprintf("bar.%d", i)), val)
	}
	//lsmt.Delete([]byte("foo"))
}

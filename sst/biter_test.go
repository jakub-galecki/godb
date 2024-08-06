package sst

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func Test_BlockIterator(t *testing.T) {
	f, err := os.Open("./testdata/example_block.bin")
	require.NoError(t, err)

	rawBlock := make([]byte, BLOCK_SIZE)
	_, err = io.ReadFull(f, rawBlock[:])
	require.NoError(t, err)

	b := &block{buf: bytes.NewBuffer(rawBlock)}
	it := NewBlockIterator(b)
	require.NotNil(t, it)

	for {
		_, err := it.Next()
		if err != nil && errors.Is(err, errNoMoreData) {
			break
		} else {
			require.NoError(t, err)
		}
		key, val := it.Key(), it.Value()
		fmt.Printf("key: %s, val: %s\n", key, val)
	}
}

package sst

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_BlockIterator(t *testing.T) {
	f, err := os.Open("./testdata/example_block.bin")
	require.NoError(t, err)

	rawBlock := make([]byte, BLOCK_SIZE)
	_, err = io.ReadFull(f, rawBlock)
	require.NoError(t, err)

	b := &block{buf: rawBlock}
	it := NewBlockIterator(b)
	require.NotNil(t, it)

	key, value, err := it.Next()
	require.NoError(t, err)
	for it.Valid() {
		require.NotNil(t, key)
		require.NotNil(t, value)
		key, value, err = it.Next()
		if errors.Is(err, errNoMoreData) {
			break
		}
	}
}

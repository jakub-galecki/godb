package sst

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	var (
		w = make([]byte, BLOCK_SIZE)
		e = newEntry([]byte("testKey"), []byte("testValue"))
	)

	_, err := encode(e, w)
	require.NoError(t, err)
	// trace.Debug().Int("n_bytes", n).Msg("written bytes")

	rawEntry := new(entry)
	_, err = decode(w, rawEntry)
	assert.NoError(t, err)

	// trace.Debug().Str("decoded_key", string(decodedEntry.key)).Str("decoded_value", string(decodedEntry.value))
	assert.Equal(t, e.key, rawEntry.key)
	assert.Equal(t, e.value, rawEntry.value)
}

func BenchmarkEncode(b *testing.B) {
	var (
		w = make([]byte, BLOCK_SIZE)

		e = newEntry([]byte("testKey"), []byte("testValue"))
	)
	for i := 0; i < b.N; i++ {
		encode(e, w)
		w = make([]byte, BLOCK_SIZE)
	}
}

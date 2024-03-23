package sst

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	var (
		w = new(bytes.Buffer)

		e = newEntry([]byte("testKey"), []byte("testValue"))
	)

	n, err := encode(e, w)
	assert.NoError(t, err)
	trace.Debug().Int("n_bytes", n).Msg("written bytes")
	decodedEntry := new(entry)
	_, err = decode(w, decodedEntry)
	assert.NoError(t, err)
	trace.Debug().Str("decoded_key", string(decodedEntry.key)).Str("decoded_value", string(decodedEntry.value))
	assert.Equal(t, e.key, decodedEntry.key)
	assert.Equal(t, e.value, decodedEntry.value)
}

func BenchmarkEncode(b *testing.B) {
	var (
		w = new(bytes.Buffer)

		e = newEntry([]byte("testKey"), []byte("testValue"))
	)
	for i := 0; i < b.N; i++ {
		encode(e, w)
		w = new(bytes.Buffer)
	}
}

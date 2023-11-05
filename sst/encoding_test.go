package sst

import (
	"bytes"
	"fmt"
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
	logger.Debugf("written bytes: [%d]", n)
	decodedEntry := new(entry)
	err = decode(w, decodedEntry)
	assert.NoError(t, err)
	fmt.Printf("decoded entry.key [%s] with value [%s]\n", decodedEntry.key, decodedEntry.value)
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

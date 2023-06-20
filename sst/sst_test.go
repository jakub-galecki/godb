package sst

import (
	"godb/memtable"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSst(t *testing.T) {
	mem := memtable.NewStorageCore()
	mem.Set([]byte("test"), []byte("1"))
	mem.Set([]byte("q"), []byte("w"))
	mem.Set([]byte("e"), []byte("r"))
	mem.Set([]byte("v"), []byte("z"))

	s, err := NewWriter(&WriterOpts{
		dirPath: "./test",
	})
	assert.NoError(t, err)
	assert.NoError(t, s.WriteMemTable(mem))

	r, err := NewReader(&ReaderOpts{
		dirPath: "./test",
	})

	assert.NoError(t, err)

	found, err := r.Get([]byte("test"))
	assert.NoError(t, err)
	assert.Equal(t, found, []byte("1"))
}

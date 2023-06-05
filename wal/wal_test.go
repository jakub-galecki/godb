package wal

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestInit(t *testing.T) {
	_, err := Init("sss.log")
	if err != nil {
		assert.NoError(t, err)
	}
	assert.FileExists(t, "sss.log")
	os.Remove("sss.log")
}

func TestAppend(t *testing.T) {
	walService, err := Init("sss.log")
	if err != nil {
		assert.NoError(t, err)
	}
	data := []byte("Hello")
	e := NewEntry(1, 0, 10001, uint32(len(data)), data)
	if err := walService.WriteEntry(e); err != nil {
		assert.NoError(t, err)
	}

	ent := new(Entry)
	nn, err := os.ReadFile("sss.log")

	if err != nil {
		assert.NoError(t, err)
	}

	if err := msgpack.Unmarshal(nn, ent); err != nil {
		assert.NoError(t, err)
	}

	if eq := cmp.Equal(*e, *ent); eq == false {
		t.Error("Entry before marshaling and unmarshaling do not match")
	}

	os.Remove("sss.log")
}

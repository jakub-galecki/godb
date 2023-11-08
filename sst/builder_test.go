package sst

import (
	"fmt"
	"testing"

	"godb/memtable"

	"github.com/stretchr/testify/assert"
)

func TestBuilder(t *testing.T) {
	storage := memtable.NewStorageCore()

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i+100)
		storage.Set([]byte(k), []byte(v))
	}

	ss, err := WriteMemTable(storage, "test")
	assert.NoError(t, err)

	logger.Debug(ss)
}

package sst

import (
	"fmt"
	"testing"

	"godb/memtable"

	"github.com/stretchr/testify/assert"
)

func TestBuilder(t *testing.T) {
	storage := memtable.New()

	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i+100)
		storage.Set([]byte(k), []byte(v))
	}

	_, err := WriteMemTable(storage, "./", "test", nil, 0, 0)
	assert.NoError(t, err)
	//logger.Debugf("%s  -> %v", ss.GetTable(), ss.GetTableMeta())

	fsst := Open("test")

	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("k%d", i)
		assert.True(t, fsst.Contains([]byte(k)))
	}

	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i+100)
		vFound, found := fsst.Get([]byte(k))
		assert.NoError(t, found)
		assert.Equal(t, []byte(v), vFound)
	}

}

package sst

import (
	"fmt"
	"os"
	"testing"

	"godb/log"
	"godb/memtable"

	"github.com/stretchr/testify/assert"
)

func TestBuilder(t *testing.T) {
	storage := memtable.New(0)
	logger := log.NewLogger("", nil)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i+100)
		storage.Set([]byte(k), []byte(v))
	}
	_, err := WriteMemTable(logger, storage, fmt.Sprintf("%s/%s", os.TempDir(), "ttt"), nil, "0.0")
	assert.NoError(t, err)
	//logger.Debugf("%s  -> %v", ss.GetTable(), ss.GetTableMeta())

	fsst, err := Open(fmt.Sprintf("%s/%s/0.0", os.TempDir(), "ttt"), "0.0", logger)
	assert.NoError(t, err)

	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		assert.True(t, fsst.Contains([]byte(k)))
	}

	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i+100)
		vFound, found := fsst.Get([]byte(k))
		assert.NoError(t, found)
		assert.Equal(t, []byte(v), vFound)
	}
}

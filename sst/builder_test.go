package sst

import (
	"fmt"
	"godb/common"
	"os"
	"testing"

	"godb/log"
	"godb/memtable"

	"github.com/stretchr/testify/assert"
)

func TestBuilder(t *testing.T) {
	storage := memtable.New(0)
	logger := log.NewLogger("", log.NilLogger)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i+100)
		assert.NoError(t, storage.Set(common.NewInternalKey([]byte(k), 0, common.SET), []byte(v)))
	}
	_, err := WriteMemTable(logger, storage, fmt.Sprintf("%s/%s", os.TempDir(), "ttt"), nil, "0.0")
	assert.NoError(t, err)

	fsst, err := Open(fmt.Sprintf("%s/%s", os.TempDir(), "ttt"), "0.0", logger)
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

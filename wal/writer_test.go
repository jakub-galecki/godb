package wal

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func cleanup(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func Test_Write(t *testing.T) {
	dir := path.Join(os.TempDir(), time.Now().Format(time.RFC3339Nano))
	def := DefaultOpts.WithDir(dir)
	mg, err := Init(def)
	require.NoError(t, err)
	wr, err := mg.NewWAL(0)
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		assert.NoError(t, wr.Write([]byte("WAL LOG ENTRY "+strconv.Itoa(i))))
	}
	assert.NoError(t, wr.Close())
	assert.FileExists(t, path.Join(dir, WalLogNum(0).FileName()))
	cleanup(dir)
}

func Test_Iterator(t *testing.T) {
	dir := path.Join(os.TempDir(), time.Now().Format(time.RFC3339Nano))
	def := DefaultOpts.WithDir(dir)
	mg, err := Init(def)
	require.NoError(t, err)
	wr, err := mg.NewWAL(0)
	require.NoError(t, err)

	for i := 0; i <= 10000; i++ {
		assert.NoError(t, wr.Write([]byte("WAL LOG ENTRY "+strconv.Itoa(i))))
	}
	assert.NoError(t, wr.Close())
	filePath := path.Join(dir, WalLogNum(0).FileName())
	assert.FileExists(t, filePath)

	f, err := os.Open(filePath)
	assert.NoError(t, err)

	it, err := NewIterator(f)
	assert.NoError(t, err)

	i := 0
	assert.NoError(t, Iter(it, func(raw []byte) error {
		assert.Equal(t, []byte("WAL LOG ENTRY "+strconv.Itoa(i)), raw)
		i++
		return nil
	}))
	assert.Equal(t, 10001, i)
	cleanup(dir)
}

func Test_WriteAfterOpen(t *testing.T) {
	dir := path.Join(os.TempDir(), time.Now().Format(time.RFC3339Nano))
	def := DefaultOpts.WithDir(dir)
	mg, err := Init(def)
	require.NoError(t, err)
	wr, err := mg.NewWAL(0)
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		assert.NoError(t, wr.Write([]byte("WAL LOG ENTRY "+strconv.Itoa(i))))
	}
	assert.NoError(t, wr.Close())
	assert.FileExists(t, path.Join(dir, WalLogNum(0).FileName()))

	wr, err = mg.OpenWAL(0)
	require.NoError(t, err)
	for i := 10000; i < 20000; i++ {
		assert.NoError(t, wr.Write([]byte("WAL LOG ENTRY "+strconv.Itoa(i))))
	}
	assert.NoError(t, wr.Close())
	filePath := path.Join(dir, WalLogNum(0).FileName())
	assert.FileExists(t, filePath)

	f, err := os.Open(filePath)
	assert.NoError(t, err)

	it, err := NewIterator(f)
	assert.NoError(t, err)

	i := 0
	assert.NoError(t, Iter(it, func(raw []byte) error {
		assert.Equal(t, []byte("WAL LOG ENTRY "+strconv.Itoa(i)), raw)
		i++
		return nil
	}))
	assert.Equal(t, 20000, i)

	cleanup(dir)
}

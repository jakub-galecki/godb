package godb

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/jakub-galecki/godb/log"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func cleanup(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func TestCore(t *testing.T) {
	dbName := time.Now().Format(time.RFC3339Nano)
	lsmt, err := Open(dbName, WithDbPath(os.TempDir()), WithLogger(log.JsonLogger))
	assert.NoError(t, err)
	for i := 0; i < 100000; i++ {
		err := lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		require.NoError(t, err)
	}
	assert.NoError(t, lsmt.Close())
	lsmt, err = Open(dbName, WithDbPath(os.TempDir()))
	assert.NoError(t, err)
	for i := 0; i < 100000; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		require.Truef(t, found, "key %s not found", fmt.Sprintf("foo.%d", i))
		require.Equal(t, []byte(fmt.Sprintf("bar.%d", i)), val)
	}
	cleanup(path.Join(os.TempDir(), dbName))
}

func TestOverwrite(t *testing.T) {
	dbName := time.Now().Format(time.RFC3339Nano)
	lsmt, err := Open(dbName, WithDbPath(os.TempDir()))
	assert.NoError(t, err)
	N := 1000
	for i := 0; i < N; i++ {
		err := lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		require.NoError(t, err)
	}
	for i := 0; i < N; i++ {
		err := lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("barr2.%d", i)))
		require.NoError(t, err)
	}
	assert.NoError(t, lsmt.Close())
	lsmt, err = Open(dbName, WithDbPath(os.TempDir()))
	assert.NoError(t, err)
	for i := 0; i < N; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		require.Truef(t, found, "key %s not found", fmt.Sprintf("foo.%d", i))
		// check that new values are returned from database
		require.Equal(t, []byte(fmt.Sprintf("barr2.%d", i)), val)
	}
	cleanup(path.Join(os.TempDir(), dbName))
}

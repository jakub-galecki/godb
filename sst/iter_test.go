package sst

import (
	"bufio"
	"fmt"
	"godb/log"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Iter(t *testing.T) {
	l := log.NewLogger("iter_test", log.NilLogger)
	sst, err := Open("./testdata/", "0", l)
	require.NoError(t, err)

	require.Equal(t, []byte("foo.0"), sst.GetMin())
	require.Equal(t, []byte("foo.9999"), sst.GetMax())

	it, err := NewSSTableIter(sst)
	require.NoError(t, err)

	expected := func() []string {
		f, err := os.Open("./testdata/keys.txt")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, f.Close())
		}()
		res := make([]string, 0)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			res = append(res, sc.Text())
		}
		require.NoError(t, sc.Err())
		return res
	}()
	// check that sst is in fact sorted
	assert.True(t, slices.IsSorted(expected))
	count := 0
	for key, value, err := it.SeekToFirst(); err == nil; key, value, err = it.Next() {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		expectedKey := fmt.Sprintf("foo.%s", expected[count])
		expectedValue := fmt.Sprintf("bar.%s", expected[count])
		require.Equal(t, []byte(expectedKey), key.UserKey)
		require.Equal(t, []byte(expectedValue), value)
		count++
	}
	assert.Equal(t, 41185, count)
}

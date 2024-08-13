package sst

import (
	"bufio"
	"errors"
	"fmt"
	"godb/common"
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
	key, value, err := it.SeekToFirst()
	require.NoError(t, err)
	for it.Valid() {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		expectedKey := fmt.Sprintf("foo.%s", expected[count])
		expectedValue := fmt.Sprintf("bar.%s", expected[count])
		require.Equal(t, []byte(expectedKey), key.UserKey)
		require.Equal(t, []byte(expectedValue), value)
		count++
		key, value, err = it.Next()
		if err != nil {
			if errors.Is(err, common.ErrIteratorExhausted) {
				break
			}
			require.NoError(t, err)
		}
	}
	assert.Equal(t, 41185, count)
}

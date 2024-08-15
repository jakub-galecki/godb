package compaction

import (
	"bufio"
	"errors"
	"fmt"
	"godb/common"
	"godb/log"
	"godb/sst"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type expectedIterResult struct {
	ukey  string
	seq   int
	op    int
	value string
}

func Test_SSTableIter(t *testing.T) {
	// iters have the same user keys but different sequence numbers and values
	// for example key foo.34620 with has value bar.34620 in 0.db and is deleted in 2.db.
	// Becuase key foo.34620 has higher sequence number in 2.db it is taken by merge iterator.
	l := log.NewLogger("iter_test", log.NilLogger)
	sst0, err := sst.Open("./testdata", "0", l)
	require.NoError(t, err)
	require.Equal(t, []byte("foo.0"), sst0.GetMin())
	require.Equal(t, []byte("foo.9999"), sst0.GetMax())

	siter0, err := sst.NewSSTableIter(sst0)
	require.NoError(t, err)

	sst1, err := sst.Open("./testdata", "1", l)
	require.NoError(t, err)
	require.Equal(t, []byte("foo.41185"), sst1.GetMin())
	require.Equal(t, []byte("foo.81514"), sst1.GetMax())
	siter1, err := sst.NewSSTableIter(sst1)
	require.NoError(t, err)

	sst2, err := sst.Open("./testdata", "2", l)
	require.NoError(t, err)
	require.Equal(t, []byte("foo.0"), sst2.GetMin())
	require.Equal(t, []byte("foo.99999"), sst2.GetMax())

	siter2, err := sst.NewSSTableIter(sst2)
	require.NoError(t, err)

	siter0.SeekToFirst()
	siter1.SeekToFirst()
	siter2.SeekToFirst()

	expectedResults := make([]expectedIterResult, 0)
	func() {
		f, err := os.Open("./testdata/expectedmergeTwoSSTKeys.txt")
		require.NoError(t, err)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			res := expectedIterResult{}
			_, err = fmt.Sscanf(line, "UserKey: %s SeqNum: %d Kind: %d Value: %s", &res.ukey, &res.seq, &res.op, &res.value)
			if !errors.Is(err, io.EOF) {
				require.NoError(t, err)
			}
			expectedResults = append(expectedResults, res)
		}
	}()

	mi, err := NewMergeIter(siter0, siter1, siter2)
	require.NoError(t, err)
	i := 0
	for key, val, err := mi.SeekToFirst(); err == nil; key, val, err = mi.Next() {
		require.Equal(t, []byte(expectedResults[i].ukey), key.UserKey)
		require.Equal(t, uint64(expectedResults[i].seq), key.SeqNum())
		require.Equal(t, uint8(expectedResults[i].op), key.Kind())
		if key.Kind() == common.DELETE {
			require.Empty(t, val)
		}
		i++
	}
}

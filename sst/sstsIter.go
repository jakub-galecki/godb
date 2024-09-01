package sst

import (
	"bytes"
	"errors"
	"sort"

	"github.com/jakub-galecki/godb/common"
)

var _ common.Iterator = (*SSTablesIter)(nil)

type SSTablesIter struct {
	cur  common.Iterator
	i    int
	init []*SST
}

// NewSSTablesIter iterates over multiple sstables without overlapping keys
func NewSSTablesIter(ssts ...*SST) (*SSTablesIter, error) {
	init := make([]*SST, 0, len(ssts))
	for _, sst := range ssts {
		init = append(init, sst)
	}
	sort.SliceStable(init, func(i, j int) bool {
		return bytes.Compare(init[i].GetMin(), init[j].GetMin()) < 0
	})

	sit := &SSTablesIter{
		cur:  nil,
		init: init,
	}
	return sit, nil
}

func (sit *SSTablesIter) setIterator() (err error) {
	if sit.i >= len(sit.init) {
		return common.ErrIteratorExhausted
	}
	sit.cur, err = NewSSTableIter(sit.init[sit.i])
	if err != nil {
		return err
	}
	sit.i++
	return nil
}

func (sit *SSTablesIter) Next() (*common.InternalKey, []byte, error) {
	k, v, err := sit.cur.Next()
	if err != nil {
		if errors.Is(err, common.ErrIteratorExhausted) {
			if err := sit.setIterator(); err != nil {
				return nil, nil, err
			}
			return sit.cur.SeekToFirst()
		}
	}
	return k, v, nil
}

func (sit *SSTablesIter) SeekToFirst() (*common.InternalKey, []byte, error) {
	if len(sit.init) == 0 {
		return nil, nil, common.ErrIteratorExhausted
	}
	sit.i = 0
	if err := sit.setIterator(); err != nil {
		return nil, nil, err
	}
	return sit.cur.SeekToFirst()
}

func (sit *SSTablesIter) Valid() bool {
	if sit == nil {
		return false
	}
	if sit.cur == nil {
		return false
	}
	return sit.cur.Valid()
}

func (sit *SSTablesIter) Key() *common.InternalKey {
	if sit.cur == nil {
		return nil
	}
	return sit.cur.Key()
}

func (sit *SSTablesIter) Value() []byte {
	if sit.cur == nil {
		return nil
	}
	return sit.cur.Value()
}

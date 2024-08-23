package sst

import (
	"errors"

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
	if len(ssts) == 0 {
		return nil, errors.New("no ssts provided")
	}
	sit := &SSTablesIter{
		cur:  nil,
		init: ssts,
	}
	return sit, nil
}

func (sit *SSTablesIter) setIterator() (err error) {
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
			sit.setIterator()
			return sit.cur.SeekToFirst()
		}
	}
	return k, v, nil
}

func (sit *SSTablesIter) SeekToFirst() (*common.InternalKey, []byte, error) {
	if len(sit.init) == 0 {
		return nil, nil, common.ErrIteratorExhausted
	}
	if err := sit.setIterator(); err != nil {
		return nil, nil, err
	}
	return sit.cur.SeekToFirst()
}

func (sit *SSTablesIter) Valid() bool {
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

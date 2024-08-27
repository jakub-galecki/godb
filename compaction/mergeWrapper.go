package compaction

import (
	"errors"

	"github.com/jakub-galecki/godb/common"
)

var _ common.Iterator = (*TwoLevelIter)(nil)

// TwoLevelIter iterates over two common.Iterator and choses
// smaller key from each iteration resulting in sorted strings table
type TwoLevelIter struct {
	current common.Iterator
	other   common.Iterator
}

func NewTwoLevelIter(i1, i2 common.Iterator) (*TwoLevelIter, error) {
	mi := &TwoLevelIter{}
	seekIfNotValid := func(i common.Iterator) error {
		if i != nil && !i.Valid() {
			_, _, err := i.SeekToFirst()
			if err != nil && !errors.Is(err, common.ErrIteratorExhausted) {
				return err
			}
		}
		return nil
	}
	if err := seekIfNotValid(i1); err != nil {
		return nil, err
	}
	if err := seekIfNotValid(i2); err != nil {
		return nil, err
	}
	mi.current, mi.other = i1, i2
	return mi, nil
}

func (mi *TwoLevelIter) setCurrent() error {
	bothValid := func() bool {
		return mi.current.Valid() && mi.other.Valid()
	}
	// todo: for now we must dump to memory all keys event,
	// when mvcc is fully implemented we will be able to discard keys whose sequenceNumber is smaller
	// then globally  visible sequence number
	if bothValid() && mi.current.Key().Equal(mi.other.Key()) {
		_, _, err := mi.other.Next()
		if err != nil {
			if errors.Is(err, common.ErrIteratorExhausted) {
				mi.swap()
				return nil
			}
			return err
		}
	}
	if !mi.other.Valid() {
		return nil
	}
	if !mi.current.Valid() {
		mi.swap()
		return nil
	}
	if mi.current.Key().Compare(mi.other.Key()) > 0 {
		mi.swap()
	}
	return nil
}

func (mi *TwoLevelIter) Valid() bool {
	if mi == nil {
		return false
	}
	return mi.current.Valid() || mi.other.Valid()
}

func (mi *TwoLevelIter) Next() (*common.InternalKey, []byte, error) {
	_, _, err := mi.current.Next()
	if err != nil {
		return nil, nil, err
	}
	err = mi.setCurrent()
	if err != nil {
		return nil, nil, err
	}
	return mi.current.Key(), mi.current.Value(), nil
}

func (mi *TwoLevelIter) SeekToFirst() (*common.InternalKey, []byte, error) {
	_, _, err := mi.current.SeekToFirst()
	if err != nil && !errors.Is(err, common.ErrIteratorExhausted) {
		return nil, nil, err
	}
	_, _, err = mi.other.SeekToFirst()
	if err != nil && !errors.Is(err, common.ErrIteratorExhausted) {
		return nil, nil, err
	}
	err = mi.setCurrent()
	if err != nil {
		return nil, nil, err
	}
	return mi.current.Key(), mi.current.Value(), nil
}

func (mi *TwoLevelIter) Key() *common.InternalKey {
	return mi.current.Key()
}

func (mi *TwoLevelIter) Value() []byte {
	return mi.current.Value()
}

func (mi *TwoLevelIter) swap() {
	mi.current, mi.other = mi.other, mi.current
}

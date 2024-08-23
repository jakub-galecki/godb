package compaction

import (
	"errors"

	"github.com/jakub-galecki/godb/common"
)

var _ common.Iterator = (*MergeWrapperIiter)(nil)

// MergeWrapperIterer iterates over two common.Iterator and choses
// smaller key from each iteration resulting in sorted table
type MergeWrapperIiter struct {
	current common.Iterator
	other   common.Iterator
}

func NewMergeWrapperIter(i1, i2 common.Iterator) (*MergeWrapperIiter, error) {
	mi := &MergeWrapperIiter{}
	seekIfNotValid := func(i common.Iterator) error {
		if !i.Valid() {
			_, _, err := i.SeekToFirst()
			if err != nil {
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

func (mi *MergeWrapperIiter) setCurrent() error {
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

func (mi *MergeWrapperIiter) Valid() bool {
	return mi.current.Valid() || mi.other.Valid()
}

func (mi *MergeWrapperIiter) Next() (*common.InternalKey, []byte, error) {
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

func (mi *MergeWrapperIiter) SeekToFirst() (*common.InternalKey, []byte, error) {
	_, _, err := mi.current.SeekToFirst()
	if err != nil {
		return nil, nil, err
	}
	_, _, err = mi.other.SeekToFirst()
	if err != nil {
		return nil, nil, err
	}
	err = mi.setCurrent()
	if err != nil {
		return nil, nil, err
	}
	return mi.current.Key(), mi.current.Value(), nil
}

func (mi *MergeWrapperIiter) Key() *common.InternalKey {
	return mi.current.Key()
}

func (mi *MergeWrapperIiter) Value() []byte {
	return mi.current.Value()
}

func (mi *MergeWrapperIiter) swap() {
	mi.current, mi.other = mi.other, mi.current
}

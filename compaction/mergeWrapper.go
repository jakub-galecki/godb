package compaction

import (
	"errors"

	"github.com/jakub-galecki/godb/common"
)

var _ common.Iterator = (*TwoLevelIter)(nil)

// TwoLevelIter iterates over two common.Iterator and choses
// smaller key from each iteration resulting in sorted strings table
type TwoLevelIter struct {
	first  common.Iterator
	second common.Iterator

	pickFirst bool
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
	mi.first, mi.second = i1, i2
	return mi, nil
}

func (mi *TwoLevelIter) pick() error {
	if !mi.firstValid() && !mi.secondValid() {
		return common.ErrIteratorExhausted
	}
	if !mi.firstValid() {
		mi.pickFirst = false
		return nil
	}
	if !mi.secondValid() {
		mi.pickFirst = true
		return nil
	}
	if mi.first.Key() == nil && mi.second.Key() == nil {
		return common.ErrIteratorExhausted
	}
	mi.pickFirst = mi.first.Key().Compare(mi.second.Key()) < 0
	return nil
}

func (mi *TwoLevelIter) firstValid() bool {
	return mi.first != nil && mi.first.Valid()
}

func (mi *TwoLevelIter) secondValid() bool {
	return mi.second != nil && mi.second.Valid()
}

func (mi *TwoLevelIter) moveSecond() error {
	if mi.firstValid() && mi.secondValid() && mi.first.Key().SoftEqual(mi.second.Key()) {
		_, _, err := mi.second.Next()
		if err != nil {
			if errors.Is(err, common.ErrIteratorExhausted) {
				mi.second = nil
				return nil
			}
			return err
		}
	}
	return nil
}

func (mi *TwoLevelIter) Valid() bool {
	if mi == nil {
		return false
	}
	return mi.firstValid() || mi.secondValid()
}

func (mi *TwoLevelIter) Next() (*common.InternalKey, []byte, error) {
	if mi.pickFirst {
		_, _, err := mi.first.Next()
		if err != nil {
			if !errors.Is(err, common.ErrIteratorExhausted) {
				return nil, nil, err
			}
			mi.first = nil
		}
	} else {
		_, _, err := mi.second.Next()
		if err != nil {
			if !errors.Is(err, common.ErrIteratorExhausted) {
				return nil, nil, err
			}
			mi.second = nil
		}
	}
	err := mi.pick()
	if err != nil {
		return nil, nil, err
	}
	err = mi.moveSecond()
	if err != nil {
		return nil, nil, err
	}
	return mi.Key(), mi.Value(), nil
}

func (mi *TwoLevelIter) SeekToFirst() (*common.InternalKey, []byte, error) {
	_, _, err := mi.first.SeekToFirst()
	if err != nil && !errors.Is(err, common.ErrIteratorExhausted) {
		return nil, nil, err
	}
	_, _, err = mi.second.SeekToFirst()
	if err != nil && !errors.Is(err, common.ErrIteratorExhausted) {
		return nil, nil, err
	}
	err = mi.pick()
	if err != nil {
		return nil, nil, err
	}
	if !mi.pickFirst {
		return mi.second.Key(), mi.second.Value(), nil
	}
	return mi.first.Key(), mi.first.Value(), nil
}

func (mi *TwoLevelIter) Key() *common.InternalKey {
	if !mi.pickFirst {
		return mi.second.Key()
	}
	return mi.first.Key()
}

func (mi *TwoLevelIter) Value() []byte {
	if !mi.pickFirst {
		return mi.second.Value()
	}
	return mi.first.Value()
}

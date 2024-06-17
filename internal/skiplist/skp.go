package skiplist

import "godb/common"

var _ common.InnerStorage = (*skipList)(nil)

type skipList struct{}

func (skp *skipList) Set(uKey common.InternalKey, v []byte) error {
	return nil
}

func (skp *skipList) Get(k []byte) ([]byte, error) {
	return nil, nil
}

func (skp *skipList) GetSize() uint64 {
	return 0
}

func (skp *skipList) Delete(k common.InternalKey) error {
	return nil
}

func (skp *skipList) NewIter() common.InnerStorageIterator {
	return nil
}

func New() *skipList {
	return nil
}

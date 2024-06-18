package skiplist

import (
	"godb/common"

	"github.com/andy-kimball/arenaskl"
)

var _ common.InnerStorage = (*skipList)(nil)

type skipList struct {
	arena *arenaskl.Arena
	inner *arenaskl.Skiplist
	it    *arenaskl.Iterator
}

func (skp *skipList) Set(uKey common.InternalKey, v []byte) error {
	return skp.it.Add(uKey.Serialize(), v, 0)
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
	return skp.it
}

func New() *skipList {
	res := &skipList{}
	res.arena = arenaskl.NewArena(common.MAX_MEMTABLE_THRESHOLD)
	res.inner = arenaskl.NewSkiplist(res.arena)
	res.it.Init(res.inner)
	return res
}

func (skp *skipList) copyIterator() common.InnerStorage {
	var it *arenaskl.Iterator
	it.Init(skp.inner)
	it.SeekToFirst()
	return nil
}

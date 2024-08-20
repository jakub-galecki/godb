package memtable

import (
	"github.com/jakub-galecki/godb/common"
	"github.com/jakub-galecki/godb/internal/skiplist"
)

//type MemTable interface {
//	common.StorageCore
//	Delete(key []byte)
//}

//var _ MemTable = (*memtable)(nil)

type MemTable struct {
	//id      uint64
	storage common.InnerStorage
	//size    int
	fileNum uint64
}

// storageCore common.StorageCore
func New(fileNum uint64) *MemTable {
	var stc MemTable
	stc.storage = skiplist.NewSkipList()
	stc.fileNum = fileNum
	return &stc
}

func (m *MemTable) Set(key *common.InternalKey, value []byte) error {
	return m.storage.Set(key, value)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	return m.storage.Get(key)
}

func (m *MemTable) GetSize() uint64 {
	return m.storage.GetSize()
}

func (m *MemTable) Delete(key *common.InternalKey) error {
	return m.storage.Set(key, common.TOMBSTONE)
}

func (m *MemTable) Iterator() common.InnerStorageIterator {
	return m.storage.NewIter()
}

func (m *MemTable) GetFileNum() uint64 {
	return m.fileNum
}

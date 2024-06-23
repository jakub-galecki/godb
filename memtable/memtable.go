package memtable

import (
	"errors"
	"godb/common"
	"godb/internal/skiplist"
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
	stc.storage = skiplist.New()
	stc.fileNum = fileNum
	return &stc
}

func (m *MemTable) Set(key, value []byte) {
	m.storage.Set(common.InternalKey{UserKey: key}, value)
}

func (m *MemTable) Get(key []byte) ([]byte, error) {
	val, found := m.storage.Get(key)
	if val != nil {
		return val, found
	}
	return nil, errors.New("not found")
}

func (m *MemTable) GetSize() int {
	// return m.storage.GetSize()
	// todo
	return 0
}

func (m *MemTable) Delete(key []byte) bool {
	return m.storage.Delete(common.InternalKey{UserKey: key})
}

func (m *MemTable) Iterator() common.InnerStorageIterator {
	return m.storage.NewIter()
}

func (m *MemTable) GetFileNum() uint64 {
	return m.fileNum
}

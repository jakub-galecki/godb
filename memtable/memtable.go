package memtable

import (
	"godb/common"
	"godb/internal/skiplist"
)

//type MemTable interface {
//	common.StorageCore
//	Delete(key []byte)
//}

//var _ MemTable = (*memtable)(nil)

type MemTable struct {
	storage *skiplist.SkipList
	size    int
}

// storageCore common.StorageCore
func NewStorageCore() *MemTable {
	var stc MemTable
	stc.size = 0
	stc.storage = skiplist.New(16)
	return &stc
}

func (m *MemTable) Set(key, value []byte) {
	m.storage.Set(key, value)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	val, found := m.storage.Get(key)
	if val != nil {
		return val, found
	}
	return nil, false
}

func (m *MemTable) GetSize() int {
	return m.storage.GetSize()
}

//func (m *MemTable) GetSizeBytes() int {
//	return m.storage.
//}

func (m *MemTable) Delete(key []byte) {
	m.storage.Set(key, common.TOMBSTONE)
}

func (m *MemTable) Iterator() *skiplist.Iterator {
	return m.storage.NewIterator()
}

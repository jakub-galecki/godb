package memtable

import (
	"bytes"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"godb/common"
)

//type MemTable interface {
//	common.StorageCore
//	Delete(key []byte)
//}

//var _ MemTable = (*memtable)(nil)

type MemTable struct {
	storage *rbt.Tree
	size    int
}

// storageCore common.StorageCore
func NewStorageCore() *MemTable {
	var stc MemTable
	stc.size = 0
	stc.storage = rbt.NewWith(func(a, b interface{}) int {
		ab := a.([]byte)
		bb := b.([]byte)
		return bytes.Compare(ab, bb)
	})
	return &stc
}

func (m *MemTable) Set(key, value []byte) {
	m.storage.Put(key, value)
}

func (m *MemTable) Put(key, value interface{}) {
	m.storage.Put(key, value)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	val, found := m.storage.Get(key)
	return val.([]byte), found
}

func (m *MemTable) GetSize() int {
	return m.storage.Size()
}

//func (m *MemTable) GetSizeBytes() int {
//	return m.storage.
//}

func (m *MemTable) Delete(key []byte) {
	m.storage.Put(key, common.TOMBSTONE)
}

func (m *MemTable) Iterator() rbt.Iterator {
	return m.storage.Iterator()
}

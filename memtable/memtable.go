package memtable

import (
	"godb/common"
	"godb/internal/rbt"
)

type MemTable interface {
	common.StorageCore
	Delete(key []byte)
}

var _ MemTable = (*memtable)(nil)

type memtable struct {
	storage common.StorageCore
	size    int
}

// storageCore common.StorageCore
func NewStorageCore() MemTable {
	var stc memtable
	stc.size = 0
	stc.storage = rbt.NewRedBlackTree()
	return &stc
}

func (m *memtable) Set(key, value []byte) {
	m.storage.Set(key, value)
}

func (m *memtable) Get(key []byte) ([]byte, bool) {
	return m.storage.Get(key)
}

func (m *memtable) GetSize() int {
	return m.size
}

func (m *memtable) GetSizeBytes() int {
	return m.storage.GetSizeBytes()
}

func (m *memtable) Delete(key []byte) {
	m.storage.Set(key, common.TOMBSTONE)
}

func (m *memtable) Iterator() common.Iterator {
	return m.storage.Iterator()
}

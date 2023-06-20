package memtable

import (
	"godb/common"
	"godb/rbt"
)

type MemTable interface {
	rbt.StorageCore
	Delete(key []byte) []byte
}

var _ MemTable = (*memtable)(nil)

type memtable struct {
	storage rbt.StorageCore
	size    int
}

func NewStorageCore() MemTable {
	var stc memtable
	stc.size = 0
	stc.storage = rbt.NewRedBlackTree()
	return &stc
}

func (m *memtable) Set(key, value []byte) []byte {
	return m.storage.Set(key, value)
}

func (m *memtable) Get(key []byte) ([]byte, bool) {
	return m.storage.Get(key)
}

func (m *memtable) GetSize() int {
	return m.size
}

func (m *memtable) Delete(key []byte) []byte {
	return m.storage.Set(key, common.TOMBSTONE)
}

func (m *memtable) Iterator() common.Iterator {
	return m.storage.Iterator()
}

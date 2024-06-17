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
	//id      uint64
	storage common.InnerStorage
	//size    int
	logSeq uint64
}

// storageCore common.StorageCore
func New(logSeq uint64) *MemTable {
	var stc MemTable
	//stc.size = 0
	stc.storage = skiplist.New(16)
	stc.logSeq = logSeq
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

func (m *MemTable) Delete(key []byte) {
	m.storage.Set(key, common.TOMBSTONE)
}

func (m *MemTable) Iterator() *skiplist.Iterator {
	return m.storage.NewIterator()
}

func (m *MemTable) GetLogSeqNum() uint64 {
	return m.logSeq
}

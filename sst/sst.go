package sst

import (
	"godb/bloom"
	"godb/memtable"
	"godb/sparse"
)

const (
	BloomFName       = "bloom.bin"
	SparseIndexFName = "sindex.bin"
	IndexFName       = "index.bin"
	DBFName          = "db.bin"
)

type entry struct {
	Key   []byte `msgpack:"k,as_array"`
	Value []byte `msgpack:"v,as_array"`
}

func newEntry(key, value []byte) *entry {
	return &entry{
		Key:   key,
		Value: value,
	}
}

type Reader interface {
	Contains([]byte) bool
	Get([]byte) ([]byte, error)
	//Close() error
}

type Writer interface {
	WriteMemTable(memtable.MemTable) error
	//Close() error
}

type SST interface {
	Reader
	Writer
}

type sst struct {
	table   string
	tableId uint

	index  sparse.Index
	bf     bloom.Filter
	blocks blocks
}

func NewSST(table string) SST {
	var (
		s   sst
		err error
	)

	s.table = table

	if err != nil {
		panic(err)
	}

	return &s
}

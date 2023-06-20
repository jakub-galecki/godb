package sst

import "godb/memtable"

const (
	BloomFName       = "bloom.bin"
	SparseIndexFName = "sindex.bin"
	IndexFName       = "index.bin"
	DBFName          = "db.bin"
)

type penc struct {
	key   []byte `msgpack:"k"`
	value []byte `msgpack:"v"`
}

type Reader interface {
	Contains([]byte) bool
	Get([]byte) ([]byte, error)
	Close() error
}

type Writer interface {
	Open() error
	Write([]byte, []byte) error
	Close() error
	WriteMemTable(memtable.MemTable) error
}

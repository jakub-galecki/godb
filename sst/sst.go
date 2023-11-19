package sst

import (
	"github.com/bits-and-blooms/bloom"

	"godb/log"
)

var (
	logger = log.InitLogger()
)

const (
	BloomFName       = "bloom.bin"
	SparseIndexFName = "sindex.bin"
	IndexFName       = "index.bin"
	DBFName          = "db.bin"
)

type Reader interface {
	Contains([]byte) bool
	Get([]byte) ([]byte, error)
	//Close() error
}

type SST interface {
	Reader

	GetTable() string
	GetTableMeta() tableMeta
}

type sst struct {
	table   string
	tableId uint

	bf  *bloom.BloomFilter
	idx *index

	meta tableMeta
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

func (s *sst) GetTableMeta() tableMeta {
	return s.meta
}

func (s *sst) GetTable() string {
	return s.table
}

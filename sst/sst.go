package sst

import (
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
}

type sst struct {
	table   string
	tableId uint

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

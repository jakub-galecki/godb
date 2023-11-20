package sst

import (
	"os"

	"github.com/allegro/bigcache"
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

//type Reader interface {
//	Contains([]byte) bool
//	Get([]byte) ([]byte, error)
//	//Close() error
//}

//type SST interface {
//	Reader
//
//	GetTable() string
//	GetTableMeta() tableMeta
//}

type SST struct {
	table   string
	tableId int

	bf   *bloom.BloomFilter
	idx  *index
	fref *os.File

	meta       tableMeta
	blockCache *bigcache.BigCache
	sstId      int
}

func NewSST(table string, idx int, cache *bigcache.BigCache) *SST {
	var (
		s   SST
		err error
	)

	s.table = table
	s.blockCache = cache
	s.sstId = idx
	if err != nil {
		panic(err)
	}

	return &s
}

func (s *SST) GetTableMeta() tableMeta {
	return s.meta
}

func (s *SST) GetTable() string {
	return s.table
}

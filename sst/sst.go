package sst

import (
	"os"

	"github.com/bits-and-blooms/bloom"

	"godb/internal/cache"
	"godb/log"
)

var (
	trace = log.NewLogger("sst")
)

const (
	BloomFName       = "bloom.bin"
	SparseIndexFName = "sindex.bin"
	IndexFName       = "index.bin"
	DBFName          = "db.bin"
)

type SST struct {
	table   string
	tableId int

	bf   *bloom.BloomFilter
	idx  *index
	fref *os.File

	meta       tableMeta
	blockCache *cache.Cache[[]byte]
	sstId      int
}

func NewSST(table string, idx int, cache *cache.Cache[[]byte]) *SST {
	var (
		s SST
	)

	s.table = table
	s.blockCache = cache
	s.sstId = idx

	return &s
}

func (s *SST) GetTableMeta() tableMeta {
	return s.meta
}

func (s *SST) GetTable() string {
	return s.table
}

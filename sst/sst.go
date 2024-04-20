package sst

import (
	"os"

	"github.com/bits-and-blooms/bloom/v3"

	"godb/internal/cache"
	"godb/log"
)

var (
	trace = log.NewLogger("sst")
)

type SST struct {
	table string

	bf   *bloom.BloomFilter
	idx  *index
	fref *os.File

	meta       tableMeta
	blockCache *cache.Cache[[]byte]
	sstId      string
}

func (s *SST) GetTableMeta() tableMeta {
	return s.meta
}

func (s *SST) GetTable() string {
	return s.table
}

func (s *SST) GetId() string {
	return s.sstId
}

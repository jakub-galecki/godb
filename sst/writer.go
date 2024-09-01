package sst

import (
	"github.com/jakub-galecki/godb/internal/cache"
	"github.com/jakub-galecki/godb/log"
	"github.com/jakub-galecki/godb/memtable"
	"time"
)

func WriteMemTable(logger *log.Logger, mem *memtable.MemTable, path string, cache cache.Cacher[[]byte], sstId string) (*SST, error) {
	it := mem.Iterator()
	start := time.Now()
	// trace.Debug().Int("MEM SIZE", mem.GetSize()).Msg("Flushing memtable to SST")

	sstBuilder := NewBuilder(logger, path, mem.GetSize(), sstId)
	for it.Valid() {
		k, v := it.Next()
		sstBuilder = sstBuilder.Add(k, v)
	}

	sst, err := sstBuilder.Finish()
	if err != nil {
		return nil, err
	}
	sst.blockCache = cache
	logger.Event("WriteMemTable", start)
	return sst, nil
}

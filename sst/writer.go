package sst

import (
	"godb/internal/cache"
	"godb/log"
	"godb/memtable"
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

	sst := sstBuilder.Finish()
	sst.blockCache = cache
	logger.Event("WriteMemTable", start)
	return sst, nil
}

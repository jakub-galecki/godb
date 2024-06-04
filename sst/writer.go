package sst

import (
	"godb/internal/cache"
	"godb/memtable"
)

func WriteMemTable(mem *memtable.MemTable, path string, cache *cache.Cache[[]byte], sstId string) (*SST, error) {
	it := mem.Iterator()

	// trace.Debug().Int("MEM SIZE", mem.GetSize()).Msg("Flushing memtable to SST")

	sstBuilder := NewBuilder(path, mem.GetSize(), sstId)
	for it.Next() {
		k, v := it.Key(), it.Value()
		sstBuilder = sstBuilder.Add(k, v)
	}

	sst := sstBuilder.Finish()
	sst.blockCache = cache
	return sst, nil
}

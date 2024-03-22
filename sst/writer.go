package sst

import (
	"godb/internal/cache"
	"godb/memtable"
)

func WriteMemTable(mem *memtable.MemTable, path, table string, cache *cache.Cache[[]byte], sstId, level int) (*SST, error) {
	it := mem.Iterator()

	//logger.Debugf("MEM SIZE %d", mem.GetSize())

	sstBuilder := NewBuilder(path, table, mem.GetSize(), level, sstId)
	for it.Next() {
		k, v := it.Key(), it.Value()
		sstBuilder = sstBuilder.Add(k.([]byte), v.([]byte))
	}

	sst := sstBuilder.Finish()
	sst.blockCache = cache
	return sst, nil
}

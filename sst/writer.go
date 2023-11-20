package sst

import (
	"github.com/allegro/bigcache"

	"godb/memtable"
)

func WriteMemTable(mem *memtable.MemTable, table string, cache *bigcache.BigCache, sstId, level int) (*SST, error) {
	it := mem.Iterator()

	//logger.Debugf("MEM SIZE %d", mem.GetSize())

	sstBuilder := NewBuilder(table, mem.GetSize(), level, sstId)
	for it.Next() {
		k, v := it.Key(), it.Value()
		sstBuilder = sstBuilder.Add(k.([]byte), v.([]byte))
	}

	sst := sstBuilder.Finish()
	sst.blockCache = cache
	return sst, nil
}

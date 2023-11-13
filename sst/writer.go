package sst

import (
	"godb/memtable"
)

func WriteMemTable(mem *memtable.MemTable, table string) (SST, error) {
	it := mem.Iterator()

	logger.Debugf("MEM SIZE %d", mem.GetSize())

	sstBuilder := NewBuilder(table, mem.GetSize())
	for it.Next() {
		k, v := it.Key(), it.Value()
		sstBuilder = sstBuilder.Add(k.([]byte), v.([]byte))
	}

	return sstBuilder.Finish(), nil
}

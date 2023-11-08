package sst

import (
	"godb/memtable"
)

func WriteMemTable(mem memtable.MemTable, table string) (SST, error) {
	it := mem.Iterator()

	sstBuilder := NewBuilder(table, mem.GetSize())
	for it.HasNext() {
		k, v, _ := it.Next()
		sstBuilder.Add(k, v)
	}

	return sstBuilder.Finish(), nil
}

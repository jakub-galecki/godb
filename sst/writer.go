package sst

import (
	"godb/memtable"
)

func WriteMemTable(mem memtable.MemTable, table string) (SST, error) {
	it := mem.Iterator()

	sstBuilder := NewBuilder(table)
	for it.HasNext() {
		k, v, _ := it.Next()
		sstBuilder.Add(k, v)
	}

	// := sstBuilder.Finish()

	return nil
}

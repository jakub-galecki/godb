package sst

import (
	"godb/memtable"
)

func (s *sst) WriteMemTable(mem memtable.MemTable) error {
	it := mem.Iterator()

	bb := NewBuilder()
	for it.HasNext() {
		k, v, _ := it.Next()
		bb.Add(k, v)
	}

	return nil
}

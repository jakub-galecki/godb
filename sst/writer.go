package sst

import (
	"godb/memtable"
)

func (s *sst) WriteMemTable(mem memtable.MemTable) error {
	// it := mem.Iterator()

	// bb := NewBuilder()
	// for it.HasNext() {
	// 	// todo: create data blocks and so on
	// 	k, v, err := it.Next()
	// 	if err != nil {
	// 		return err
	// 	}

	// }

	return nil
}

package sst

import (
	"godb/memtable"
	"testing"
)

func TestSst(t *testing.T) {
	mem := memtable.NewStorageCore()
	mem.Set([]byte("test"), []byte("1"))
	mem.Set([]byte("q"), []byte("w"))
	mem.Set([]byte("e"), []byte("r"))
	mem.Set([]byte("v"), []byte("z"))

}

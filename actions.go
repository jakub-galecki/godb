package main

import (
	"godb/memtable"
)

type actions []*action

type action struct {
	kind  string
	key   []byte
	value []byte
}

func newAction(key, value []byte, kind string) action {
	return action{
		kind:  kind,
		key:   key,
		value: value,
	}
}

func (a *action) applyToMemtable(mem memtable.MemTable) error {
	switch a.kind {
	case "SET":
		mem.Set(a.key, a.value)
	case "DEL":
		mem.Delete(a.key)
	}
	return nil
}

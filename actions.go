package main

import "fmt"

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

func (a *action) repr() []byte {
	return []byte(fmt.Sprintf("%s %s %s", a.kind, a.key, a.value))
}

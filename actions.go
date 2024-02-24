package main

import "fmt"

type actions []*action

type action struct {
	kind  int
	key   []byte
	value []byte
}

func newAction(key, value []byte, kind int) action {
	return action{
		kind:  kind,
		key:   key,
		value: value,
	}
}

func (a *action) repr() []byte {
	return []byte(fmt.Sprintf("%d %s %s", a.kind, a.key, a.value))
}

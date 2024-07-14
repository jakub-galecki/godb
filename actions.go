package main

import (
	"fmt"
	"godb/common"
)

type actions []*action

type action struct {
	kind  common.DbOp
	key   []byte
	value []byte
}

func newAction(key, value []byte, kind common.DbOp) action {
	return action{
		kind:  kind,
		key:   key,
		value: value,
	}
}

func (a *action) byte() []byte {
	// todo: optimize
	return []byte(fmt.Sprintf("%d %s %s", a.kind, a.key, a.value))
}

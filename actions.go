package main

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

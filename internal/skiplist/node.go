package skiplist

import "godb/common"

type node struct {
	key      []byte
	keyMeta  common.KeyMeta
	value    []byte
	forwards []*atomicMarkableRef[node]
	level    int
}

func newNode(key common.InternalKey, value []byte, level int) *node {
	res := &node{
		key:      key.UserKey,
		keyMeta:  key.GetMeta(),
		value:    value,
		forwards: make([]*atomicMarkableRef[node], level+1),
		level:    level,
	}
	for i := range res.forwards {
		res.forwards[i] = newAtomicMarkableRef(&node{}, false)
	}
	return res
}

func newSentinelNode(level int) *node {
	res := &node{
		forwards: make([]*atomicMarkableRef[node], level),
	}
	for i := range res.forwards {
		res.forwards[i] = newAtomicMarkableRef(&node{}, false)
	}
	return res
}

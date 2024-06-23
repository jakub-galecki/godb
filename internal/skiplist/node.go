package skiplist

type node struct {
	key      []byte
	keyMeta  uint64
	value    []byte
	forwards []*atomicMarkableRef[node]
	level    int
}

func newNode(key []byte, value []byte, level int) *node {
	res := &node{
		key:      key,
		keyMeta:  0,
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

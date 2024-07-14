package skiplist

type node struct {
	key      *iKey
	value    []byte
	forwards []*node
}

func newNode(key *iKey, value []byte, level int) *node {
	res := &node{
		key:      key,
		value:    value,
		forwards: make([]*node, level+1),
	}
	return res
}

func newSentinelNode(level int) *node {
	res := &node{
		forwards: make([]*node, level+1),
	}
	return res
}

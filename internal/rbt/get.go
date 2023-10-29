package rbt

import (
	"bytes"
)

func (t *tree) Get(key []byte) ([]byte, bool) {
	return t.internalGet(key)
}

func (t *tree) internalGet(key []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}
	for cur := t.root; cur != nil; {
		switch compared := bytes.Compare(key, cur.key); {
		case compared < 0:
			cur = cur.leftChild
		case compared > 0:
			cur = cur.rightChild
		default:
			return cur.value, true
		}
	}
	return nil, false
}

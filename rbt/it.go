package rbt

import (
	"fmt"
	"godb/common"
)

type iterator struct {
	cursor *node
}

var _ common.Iterator = (*iterator)(nil)

func (t *tree) Iterator() common.Iterator {
	cursor := t.root
	if cursor != nil && cursor.leftChild != nil {
		cursor = cursor.leftChild
	}

	return &iterator{
		cursor: cursor,
	}
}

func (it iterator) HasNext() bool {
	return it.cursor != nil
}

func (it iterator) Next() ([]byte, []byte, error) {
	if !it.HasNext() {
		return nil, nil, fmt.Errorf("out of records")
	}

	cur := it.cursor
	if cur.rightChild != nil {
		it.cursor = it.cursor.rightChild
		for it.cursor.leftChild != nil {
			it.cursor = it.cursor.leftChild
		}
		return cur.key, cur.value, nil
	}

	for {
		if it.cursor.parent == nil {
			it.cursor = nil
			return cur.key, cur.value, nil
		}
		if it.cursor.parent.leftChild == it.cursor {
			it.cursor = it.cursor.parent
			return cur.key, cur.value, nil
		}
		it.cursor = it.cursor.parent
	}
}

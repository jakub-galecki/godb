package rbt

import "bytes"

func (t *tree) Set(key, value []byte) {
	n := &node{
		key:   key,
		value: value,
		color: RED,
	}
	t.internalSet(n)
}

func (t *tree) internalSet(n *node) {
	var (
		parent *node

		cur = t.root
	)

	t.entries++

	if t.root == nil {
		n.color = BLACK
		t.root = n
		t.size = len(n.value)
		return
	}

	for cur != nil {
		parent = cur
		switch cmp := bytes.Compare(n.key, cur.key); {
		case cmp == 0:
			oldValue := cur.value
			cur.value = n.value
			t.modifySize(n.value, oldValue)
			return
		case cmp < 0:
			cur = cur.leftChild
		default:
			cur = cur.rightChild
		}
	}

	n.parent = parent
	if parent == nil {
		t.root = n
	} else if cmp := bytes.Compare(n.key, parent.key); cmp < 0 {
		parent.leftChild = n
	} else {
		parent.rightChild = n
	}

	t.fixInsert(n)
	t.modifySize(n.value, nil)
}

func (t *tree) fixInsert(n *node) {
	x := n
	for x != t.root && x.parent.color == RED {
		if x.parent == x.parent.parent.leftChild {
			y := x.parent.parent.rightChild
			if y != nil && y.color == RED {
				x.parent.color = BLACK
				y.color = BLACK
				x.parent.parent.color = RED
				x = x.parent.parent
			} else {
				if x == x.parent.rightChild {
					x = x.parent
					t.rotateLeft(x)
				}
				x.parent.color = BLACK
				x.parent.parent.color = RED
				t.rotateRight(x.parent.parent)
			}
		} else {
			y := x.parent.parent.leftChild
			if y != nil && y.color == RED {
				x.parent.color = BLACK
				y.color = BLACK
				x.parent.parent.color = RED
				x = x.parent.parent
			} else {
				if x == x.parent.leftChild {
					x = x.parent
					t.rotateRight(x)
				}
				x.parent.color = BLACK
				x.parent.parent.color = RED
				t.rotateLeft(x.parent.parent)
			}
		}
	}
	t.root.color = BLACK
}

func (t *tree) modifySize(new, old []byte) {
	t.size += len(new) - len(old)
}

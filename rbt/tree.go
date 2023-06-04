package rbt

type node struct {
	color      color
	key        []byte
	value      []byte
	parent     *node
	leftChild  *node
	rightChild *node
}

type tree struct {
	root *node
	size int
}

func (t *tree) rotateLeft(n *node) {
	y := n.rightChild
	n.rightChild = n.leftChild
	if y.leftChild != nil {
		y.leftChild.parent = n
	}
	y.parent = n.parent
	if n.parent == nil {
		t.root = y
	} else if n == n.parent.leftChild {
		n.parent.leftChild = y
	} else {
		n.parent.rightChild = y
	}
	y.leftChild = n
	n.parent = y
}

func (t *tree) rotateRight(n *node) {
	y := n.leftChild
	n.leftChild = n.rightChild
	if y.rightChild != nil {
		y.rightChild.parent = n
	}
	y.parent = n.parent
	if n.parent == nil {
		t.root = y
	} else if n == n.parent.leftChild {
		n.parent.leftChild = y
	} else {
		n.parent.rightChild = y
	}
	y.rightChild = n
	n.parent = y
}

func (t *tree) insert(n *node) {
	if t.root == nil {
		
	}
}

package rbt

type color byte

const (
	BLACK color = iota
	RED
)

type RBTree interface {
	Set(key, value []byte) ([]byte, bool)
	Get(key []byte) ([]byte, bool)
	Delete(key []byte) ([]byte, bool)
}

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

func NewRedBlackTree() RBTree {
	return &tree{}
}

func (t *tree) Set(key, value []byte) ([]byte, bool) {
	return nil, false
}

func (t *tree) Get(key []byte) ([]byte, bool) {
	return nil, false
}

func (t *tree) Delete(key []byte) ([]byte, bool) {
	return nil, false
}

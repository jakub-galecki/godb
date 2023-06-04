package rbt

// based on http://staff.ustc.edu.cn/~csli/graduate/algorithms/book6/chap14.htm

type color byte

const (
	BLACK color = iota
	RED
)

var _ RBTree = (*tree)(nil)

type RBTree interface {
	Set(key, value []byte) []byte
	Get(key []byte) ([]byte, bool)
	GetSize() int
}

func NewRedBlackTree() RBTree {
	return &tree{}
}

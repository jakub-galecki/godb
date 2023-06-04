package rbt

// based on http://staff.ustc.edu.cn/~csli/graduate/algorithms/book6/chap14.htm

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

func NewRedBlackTree() RBTree {
	return &tree{}
}

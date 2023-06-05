package rbt

// based on http://staff.ustc.edu.cn/~csli/graduate/algorithms/book6/chap14.htm

type color byte

type StorageCore interface {
	Set(key, value []byte) []byte
	Get(key []byte) ([]byte, bool)
	GetSize() int
}

const (
	BLACK color = iota // 0
	RED                // 1
)

var _ StorageCore = (*tree)(nil)

func NewRedBlackTree() StorageCore {
	return &tree{}
}

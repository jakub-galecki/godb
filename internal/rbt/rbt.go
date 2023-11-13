package rbt

// based on http://staff.ustc.edu.cn/~csli/graduate/algorithms/book6/chap14.htm

type color byte

const (
	BLACK color = iota // 0
	RED                // 1
)

//var _ common.StorageCore = (*tree)(nil)
//
//func NewRedBlackTree() common.StorageCore {
//	return &tree{}
//}

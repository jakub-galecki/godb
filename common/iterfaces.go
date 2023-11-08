package common

type IteratorCore interface {
	Iterator() Iterator
}

type Iterator interface {
	Next() ([]byte, []byte, error)
	HasNext() bool
}

type StorageCore interface {
	IteratorCore
	Set(key, value []byte)
	Get(key []byte) ([]byte, bool)
	GetSize() int
	GetSizeBytes() int
}

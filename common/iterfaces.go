package common

type InnerStorage interface {
	Set(uKey *InternalKey, value []byte) error
	Get(key []byte) ([]byte, bool)
	GetSize() uint64
	NewIter() InnerStorageIterator
}

type InnerStorageIterator interface {
	HasNext() bool
	Next() (*InternalKey, []byte)
}

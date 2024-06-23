package common

type InnerStorage interface {
	Set(uKey InternalKey, value []byte) error
	Get(key []byte) ([]byte, error)
	GetSize() uint64
	Delete(key InternalKey) bool
	NewIter() InnerStorageIterator
}

type InnerStorageIterator interface {
}

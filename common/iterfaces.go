package common

type InnerStorage interface {
	Set(uKey *InternalKey, value []byte) error
	Get(key []byte) ([]byte, bool)
	GetSize() uint64
	NewIter() InnerStorageIterator
}

type InnerStorageIterator interface {
	Valid() bool
	Next() (*InternalKey, []byte)
}

type Iterator interface {
	Next() (*InternalKey, []byte, error)
	Key() *InternalKey
	Valid() bool
	Value() []byte
	SeekToFirst() (*InternalKey, []byte, error)
}

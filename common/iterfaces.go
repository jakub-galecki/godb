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

type Iterator interface {
	Next() (*InternalKey, []byte, error)
	SeekToFirst(*InternalKey, []byte, error)
	Valid() bool
	Key() *InternalKey
	Value() []byte
}

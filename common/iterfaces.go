package common

type IteratorCore interface {
	Iterator() Iterator
}

type Iterator interface {
	Next() ([]byte, []byte, error)
	HasNext() bool
}

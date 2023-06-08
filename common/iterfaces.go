package common

type Iterator interface {
	Next() ([]byte, []byte, error)
	HasNext() bool
}

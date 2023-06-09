package sst

const (
	BloomFName = "bloom.bin"
)

type Reader interface {
	Contains([]byte) bool
	Get([]byte) ([]byte, error)
}

type Writer interface {
}

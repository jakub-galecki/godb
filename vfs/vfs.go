package vfs

import (
	"fmt"
	"os"

	"godb/log"

	"go.uber.org/zap"
)

type Reader[T any] interface {
	Read([]byte) (n int, err error)
}

type Writer[T any] interface {
	Write([]byte) (n int, err error)
	Flush() error
}

type VFS[T any] interface {
	Reader[T]
	Writer[T]

	GetFileReference() *os.File
}

type vfs[T any] struct {
	path string

	f      *os.File
	logger *zap.SugaredLogger
}

func NewVFS[T any](dir, file string, flag int, perm os.FileMode) VFS[T] {
	var (
		v   vfs[T]
		err error
	)

	v.logger = log.InitLogger()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0777) // todo
		if err != nil {
			panic(err)
		}
	}
	v.path = fmt.Sprintf("%s/%s", dir, file)
	v.f, err = os.OpenFile(v.path, flag, perm)
	if err != nil {
		v.logger.Errorf("[NewVFS] error while opening file: %v", err)
		panic(err)
	}

	return v
}

func (v vfs[T]) GetFileReference() *os.File {
	return v.f
}

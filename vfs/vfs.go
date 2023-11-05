package vfs

import (
	"godb/log"
	"os"

	"go.uber.org/zap"
)

type Reader[T any] interface {
	Read([]byte) (n int, err error)
}

type Writer[T any] interface {
	Write([]byte) (n int, err error)
}

type VFS[T any] interface {
	Reader[T]
	Writer[T]
}

type vfs[T any] struct {
	path string

	f      *os.File
	logger *zap.SugaredLogger
}

func NewVFS[T any](path string, flag int, perm os.FileMode) VFS[T] {
	var (
		v   vfs[T]
		err error
	)

	v.logger = log.InitLogger()
	v.path = path
	v.f, err = os.OpenFile(path, flag, perm)
	if err != nil {
		v.logger.Errorf("[NewVFS] error while opening file: %v", err)
		panic(err)
	}

	return v
}

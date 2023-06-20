package vfs

import (
	"godb/log"
	"os"

	"go.uber.org/zap"
)

type VFSReader[T any] interface {
	Read(*T, func([]byte) (T, error)) (n int, err error)
}

type VFSWriter[T any] interface {
	Write(T, func(T) ([]byte, error)) (n int, err error)
}

type VFS[T any] interface {
	VFSReader[T]
	VFSWriter[T]
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

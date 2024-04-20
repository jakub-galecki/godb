package vfs

import (
	"godb/log"
	"os"
	"path"
)

var (
	trace = log.NewLogger("vfs")
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

	GetFileRef() *os.File
}

type vfs[T any] struct {
	path string

	f *os.File
}

func NewVFS[T any](dir, file string, flag int, perm os.FileMode) VFS[T] {
	var (
		v   vfs[T]
		err error
	)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0777) // todo
		if err != nil {
			panic(err)
		}
	}
	v.path = path.Join(dir, file)
	v.f, err = os.OpenFile(v.path, flag, perm)
	if err != nil {
		trace.Error().Err(err).Msg("error while opening file")
		panic(err)
	}

	return v
}

func (v vfs[T]) GetFileRef() *os.File {
	return v.f
}

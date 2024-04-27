package common

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
)

func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0777); err != nil {
		if errors.Is(err, os.ErrExist) {
			finfo, err := os.Stat(path)
			if err != nil {
				return err
			}
			if !finfo.IsDir() {
				return ErrPathFile
			}
			return nil
		}
		return err
	}
	return nil
}

func CreateFile(path string) (*os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func Concat(xs ...string) string {
	var buf bytes.Buffer
	for _, x := range xs {
		buf.WriteString(x)
	}
	return buf.String()
}

func ListDir[T any](path string, mut func(string) T) ([]T, error) {
	var files []T
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, mut(path))
		}
		return nil
	})
	return files, err
}

package common

import (
	"bytes"
	"errors"
	"os"
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

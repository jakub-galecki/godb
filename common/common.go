package common

import (
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

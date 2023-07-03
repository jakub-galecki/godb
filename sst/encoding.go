package sst

import (
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

func encode(e *entry, w io.Writer) (int, error) {
	encodedData, err := msgpack.Marshal(e)
	if err != nil {
		return 0, err
	}

	n, err := w.Write(encodedData)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func decode(r io.Reader, e *entry) error {
	decoder := msgpack.NewDecoder(r)
	return decoder.Decode(e)
}

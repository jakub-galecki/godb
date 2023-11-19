package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

func encode(e *entry, w io.Writer) (int, error) {
	return w.Write(e.encode())
}

func decode(r io.Reader, e *entry) (int, error) {
	return e.decode(r)
}

type entry struct {
	key   []byte
	value []byte
}

func newEntry(key, value []byte) *entry {
	return &entry{
		key:   key,
		value: value,
	}
}

func (e *entry) encode() []byte {
	res := new(bytes.Buffer)

	keyLen := make([]byte, 8)
	binary.BigEndian.PutUint64(keyLen, uint64(len(e.key)))

	binary.Write(res, binary.BigEndian, keyLen)
	binary.Write(res, binary.BigEndian, e.key)

	valueLen := make([]byte, 8)
	binary.BigEndian.PutUint64(valueLen, uint64(len(e.value)))

	binary.Write(res, binary.BigEndian, valueLen)
	binary.Write(res, binary.BigEndian, e.value)

	return res.Bytes()
}

func (e *entry) decode(r io.Reader) (int, error) {
	if e == nil {
		return 0, fmt.Errorf("nil entry")
	}
	total := 0

	keyLenBytes := make([]byte, 8)
	n, err := r.Read(keyLenBytes)
	if err != nil {
		return 0, err
	}
	total += n

	keyLen := binary.BigEndian.Uint64(keyLenBytes)
	key := make([]byte, keyLen)
	n, err = r.Read(key)
	if err != nil {
		return 0, err
	}
	total += n

	valueLenBytes := make([]byte, 8)
	n, err = r.Read(valueLenBytes)
	if err != nil {
		return 0, err
	}
	total += n

	valueLen := binary.BigEndian.Uint64(valueLenBytes)
	value := make([]byte, valueLen)
	n, err = r.Read(value)
	if err != nil {
		return 0, err
	}
	total += n
	e.key = key
	e.value = value
	return total, nil
}

func (e *entry) getSize() int {
	return len(e.key) + len(e.value) + 16
}

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

func decode(r io.Reader, e *entry) error {
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

func (e *entry) decode(r io.Reader) error {
	if e == nil {
		return fmt.Errorf("nil entry")
	}

	keyLenBytes := make([]byte, 8)
	_, err := r.Read(keyLenBytes)
	if err != nil {
		return err
	}
	keyLen := binary.BigEndian.Uint64(keyLenBytes)
	logger.Debugf("read keyLen %d", keyLen)
	key := make([]byte, keyLen)
	_, err = r.Read(key)
	if err != nil {
		return err
	}

	valueLenBytes := make([]byte, 8)
	_, err = r.Read(valueLenBytes)
	if err != nil {
		return err
	}
	valueLen := binary.BigEndian.Uint64(valueLenBytes)
	logger.Debugf("read valueLen %d", valueLen)
	value := make([]byte, valueLen)
	_, err = r.Read(value)
	if err != nil {
		return err
	}

	e.key = key
	e.value = value
	return nil
}

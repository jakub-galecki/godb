package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type syncWriter interface {
	io.Writer
	Sync() error
}

const (
	bSize = 4096
)

var (
	errBlockFull = errors.New("no more space in block")
)

// todo: chunking

type block struct {
	buf  [bSize]byte
	off  int
	size int
	w    *bufio.Writer
}

func (b *block) write(data []byte) error {
	if b.off+len(data)+binary.MaxVarintLen64 > bSize {
		return errBlockFull
	}
	dataLen := len(data)
	buf := b.buf[b.off : b.off+dataLen+binary.MaxVarintLen64]
	written := binary.PutUvarint(buf[:], uint64(dataLen))
	copy(buf[written:], data)
	totalWritten := written + dataLen
	b.off += totalWritten
	return b.persist(buf[:totalWritten])
}

func (b *block) persist(data []byte) error {
	_, err := b.w.Write(data)
	if err != nil {
		return err
	}
	err = b.w.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (b *block) rotate() error {
	clear(b.buf[b.off:])
	err := b.persist(b.buf[b.off:])
	if err != nil {
		return err
	}
	b.off = 0
	return nil
}

type writer struct {
	o    *Opts
	file *os.File
	b    *block

	// lsn        int
}

type Writer interface {
	Write([]byte) error
	Close() error
}

func newWriter(f *os.File, o *Opts) (*writer, error) {
	w := &writer{
		file: f,
		o:    o,
		b: &block{
			w: bufio.NewWriter(f),
		},
	}
	return w, nil
}

func (w *writer) Write(data []byte) error {
	err := w.b.write(data)
	if err != nil {
		if err != errBlockFull {
			return err
		}
		if err := w.b.rotate(); err != nil {
			return err
		}
		if err := w.b.write(data); err != nil {
			// todo: before adding chunking iff we just roated block and still can't write record
			// then just panic as there is nothing we could do
			panic(err)
		}
	}
	return nil
}

func (w *writer) Close() error {
	if err := w.b.rotate(); err != nil {
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	return nil
}

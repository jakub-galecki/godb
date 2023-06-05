package wal

import (
	"bufio"
	"os"
	"sync"
)

type WalFs interface {
	Append(WalEntry) error
}

type walfs struct {
	l         sync.Mutex
	path      string
	file      *os.File
	writeChan chan Entry
}

func NewFile(path string) (WalFs, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	w := &walfs{
		path: path,
		file: f,
	}
	w.writeChan = make(chan Entry)
	// go w.writer()
	return w, nil
}

func (w *walfs) Append(e WalEntry) error {
	data := e.Serialize()
	w.l.Lock()
	defer w.l.Unlock()
	if _, err := w.file.Write(data); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.sync()
}

func (w *walfs) AppendBatch(entries []WalEntry) error {
	buf := bufio.NewWriter(w.file)
	for _, e := range entries {
		serialized := e.Serialize()
		if _, err := buf.Write(serialized); err != nil {
			return err
		}
	}

	if err := buf.Flush(); err != nil {
		return err
	}

	return w.sync()
}

func (w *walfs) sync() error {
	return w.file.Sync()
}

// func (w *walfs) writer() {
// 	for {
// 		select {
// 		case msg := <-w.writeChan:

// 			break
// 		}
// 	}
// }

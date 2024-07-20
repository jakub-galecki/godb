package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

type writer struct {
	mu sync.Mutex

	o          *Opts
	file       *os.File
	off        int64
	size       int64
	lsn        int
	buf        *bufio.Writer
	syncTicker *time.Ticker
	exitChan   chan struct{}
}

type Writer interface {
	Write([]byte) error
	Close() error
}

func newWriter(f *os.File, o *Opts) (*writer, error) {
	w := &writer{
		file:       f,
		syncTicker: time.NewTicker(o.SyncInterval),
		o:          o,
		exitChan:   make(chan struct{}),
	}

	w.buf = bufio.NewWriter(f)

	go w.runSyncWorker(w.exitChan)
	return w, nil
}

func (w *writer) internalEncode(b []byte) []byte {
	lsn := w.lsn
	data := []byte(fmt.Sprintf("%v %d|%s\n", time.Now().Unix(), lsn, b))
	// sum := md5.Sum([]byte(data))
	return data
}

func (w *writer) runSyncWorker(exitChan <-chan struct{}) {
	for {

		select {
		case <-w.syncTicker.C:
			w.mu.Lock()
			w.sync()
			w.mu.Unlock()
		case <-exitChan:
			return
		}
	}
}

// sync requiers to hold lock on mu
func (w *writer) sync() error {
	return w.buf.Flush()
	// return w.file.Sync()
}

func (w *writer) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	enc := w.internalEncode(w.o.Encoder(data))
	if _, err := w.buf.Write(enc); err != nil {
		return err
	}
	w.lsn += 1
	return nil
}

func (w *writer) Close() error {
	w.exitChan <- struct{}{}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.sync(); err != nil {
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	return nil
}

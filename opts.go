package main

import (
	"errors"
	"fmt"
	"godb/common"
	"godb/log"
	"io"
	"os"
	"path"
)

type dbOpts struct {
	table  string
	path   string
	logger *log.Logger
	// enableWal bool
}

func (o *dbOpts) validate() error {
	if o.logger == nil {
		return errors.New("logger not specified")
	}
	return nil
}

type DbOpt func(*dbOpts)

func WithDbPath(path string) DbOpt {
	return func(o *dbOpts) {
		o.path = path
	}
}

type LoggerType uint8

const (
	JsonLogger LoggerType = iota
	NilLogger
)

type discardWriterCloser struct {
	io.Writer
}

func (discardWriterCloser) Close() error {
	return nil
}

func WithLogger(l LoggerType) DbOpt {
	jsonLogger := func() *log.Logger {
		var (
			dst io.WriteCloser = os.Stdout
			err error

			logPath = os.Getenv("GODB_LOG_PATH")
			name    = "godb"
		)
		if logPath != "" {
			if err := common.EnsureDir(logPath); err != nil {
				panic(err)
			}
			dst, err = os.OpenFile(fmt.Sprintf("%s/%s.log", logPath, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
		}
		return log.NewLogger(name, dst)
	}
	return func(o *dbOpts) {
		switch l {
		case JsonLogger:
			o.logger = jsonLogger()
		case NilLogger:
			o.logger = log.NewLogger("godb", discardWriterCloser{Writer: io.Discard})
		}
	}
}

func defaultOpts(table string, opts []DbOpt) dbOpts {
	res := dbOpts{
		table: table,
		path:  "/tmp/",
	}
	for _, fn := range opts {
		fn(&res)
	}
	if res.logger == nil {
		res.logger = log.NewLogger("godb", discardWriterCloser{Writer: io.Discard})
	}
	res.path = path.Join(res.path, table)
	return res
}

package godb

import (
	"errors"
	"os"
	"path"

	"github.com/jakub-galecki/godb/log"
)

type dbOpts struct {
	table   string
	path    string
	logger  *log.Logger
	sstSize uint64
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

func WithLogger(l log.LoggerType) DbOpt {
	return func(o *dbOpts) {
		l := log.NewLogger("godb", l)
		o.logger = l
	}
}

func defaultOpts(table string, opts []DbOpt) dbOpts {
	res := dbOpts{
		table:   table,
		path:    os.TempDir(),
		sstSize: 4 * (1 << 20),
	}
	for _, fn := range opts {
		fn(&res)
	}
	if res.logger == nil {
		res.logger = log.NewLogger("godb", log.NilLogger)
	}
	res.path = path.Join(res.path, table)
	return res
}

package kv

import "godb/log"

var logger = log.InitLogger()

type IKV interface {
	Txn
}

func New() {
	logger.Debug("Initializing key-value store")
	t := Begin()
	t.Set("Foo", []byte("BarZ"))
	logger.Info(string(t.Get("Foo")))
	t.Rollback()
	t.Commit()
}

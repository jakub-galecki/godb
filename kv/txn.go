package kv

import (
	"time"

	"github.com/google/uuid"
)

type TxnOpts struct {
	TxnId string
}

type Txn interface {
	DB

	Commit()
	Rollback()
}

type transaction struct {
	db

	txnId     string
	startTime time.Time
}

func Begin(opt ...TxnOpts) Txn {
	txn := &transaction{}

	if len(opt) == 1 {
		if opt[0].TxnId != "" {
			txn.txnId = opt[0].TxnId
		} else {
			txn.txnId = getTxnId()
		}
	}

	e := newEngine()
	txn.db = newDb(e)

	return txn
}

func (t transaction) Rollback() {
	logger.Info("Rollback ")
}

func (t transaction) Commit() {
	logger.Info("Committing")
}

func getTxnId() string {
	return uuid.New().String()
}

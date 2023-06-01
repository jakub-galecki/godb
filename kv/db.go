package kv

type DB interface {
	Set(key string, value []byte)
	Get(key string) []byte
}

type db struct {
	iEngine
}

func newDb(engine iEngine) db {
	return db{engine}
}

func (d db) Set(key string, value []byte) {
	logger.Info("Set")
	d.internalSet(key, value)
}

func (d db) Get(key string) []byte {
	logger.Info("Get")
	return d.internalGet(key)
}

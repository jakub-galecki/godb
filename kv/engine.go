package kv

type iEngine interface {
	internalSet(key string, value []byte)
	internalGet(key string) []byte
}

type engine struct {
	data map[string][]byte
}

func newEngine() iEngine {
	return &engine{data: make(map[string][]byte)}
}

func (e engine) internalSet(key string, value []byte) {
	e.data[key] = value
}

func (e engine) internalGet(key string) []byte {
	return e.data[key]
}

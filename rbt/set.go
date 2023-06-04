package rbt

func (t *tree) Set(key, value []byte) ([]byte, bool) {
	return t.internalSet(key, value)
}

func (t *tree) internalSet(key, value []byte) ([]byte, bool) {

}

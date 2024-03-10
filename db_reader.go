package main

func (l *db) Get(key []byte) ([]byte, bool) {
	value, found := l.mem.Get(key)
	if found {
		return value, found
	}

	for _, mem := range l.sink {
		value, found := mem.Get(key)
		if found {
			return value, found
		}
	}

	// l.l0Flushed.Wait()
	if val, found := l.l0.Get(key); found {
		return val, found
	}

	for _, lvl := range l.levels {
		value, found := lvl.Get(key)
		if found {
			return value, found
		}
	}

	return nil, false
}

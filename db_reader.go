package main

func (l *db) Get(key []byte) ([]byte, bool) {
	l.logger.Debugf("Gettiing Key [%s]", key)
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

	for _, lvl := range l.levels {
		value, found := lvl.Get(key)
		if found {
			return value, found
		}
	}

	return nil, false
}

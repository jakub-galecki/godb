package lsmt

func (l *lsmt) Set(key, value []byte) []byte {
	l.logger.Debugf("Setting Key [%s] to value [%s]", key, value)
	oldValue := l.mem.Set(key, value)

	if l.exceededSize() {
		l.moveToSink()
	}

	return oldValue
}

func (l *lsmt) Delete(key []byte) []byte {
	l.logger.Debugf("Deleting Key [%s]", key)
	oldValue := l.mem.Delete(key)

	if l.exceededSize() {
		l.moveToSink()
	}

	return oldValue
}

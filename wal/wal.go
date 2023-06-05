package wal

type Wal interface {
	WriteEntry(WalEntry) error
}

type wal struct {
	file WalFs
}

func Init(filePath string) (*wal, error) {
	f, err := NewFile(filePath)
	if err != nil {
		return nil, err
	}
	return &wal{
		file: f,
	}, nil
}

func (w *wal) WriteEntry(e WalEntry) error {
	return w.file.Append(e)
}

package common

type InternalKey struct {
	uKey   []byte
	seqNum uint64
	kind   int
}

func (ik *InternalKey) Serialize() []byte {
	return nil
}

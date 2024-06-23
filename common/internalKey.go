package common

type KeyMeta uint64

type InternalKey struct {
	UserKey []byte
	meta    KeyMeta
}

func NewInternalKey(ukey []byte, seqNum uint64, kind uint8) *InternalKey {
	return nil
}

func (ik *InternalKey) GetMeta() KeyMeta {
	return ik.meta
}

func (ik *InternalKey) Serialize() []byte {
	return nil
}

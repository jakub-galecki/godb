package skiplist

import "godb/common"

var _ common.InnerStorage =  (*SkipList)(nil)

type SkipList struct {}

func (skp *SkipList) Set(uKey common.InternalKey ,v []byte) error {
    return nil 
}

func (skp *SkipList) Get(k []byte) ([]byte, error) {
    return nil, nil
}

func (skp *SkipList) GetSize() uint64 {
    return 0
}

func (skp *SkipList) Delete(k common.InternalKey) error {
    return nil 
}



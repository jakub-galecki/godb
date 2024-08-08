package sst

type SSTableIterator struct {
	bit      *BlockIterator
	sst      *SST
	blockIdx int
}

//
//func NewSSTableIterator(sst *SST, blockIdx int) *SSTableIterator {
//	fiirstBlock := nil
//
//	return &SSTableIterator{
//		sst:      sst,
//		blockIdx: 0,
//	}
//}

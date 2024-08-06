package sst

type SSTableIter struct {
	sst     *SST
	blkIter *BlockIterator
	index   int
}

func NewSSTableIter(sst *SST) *SSTableIter {

	return &SSTableIter{
		sst: sst,
	}
}

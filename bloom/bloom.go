package bloom

import (
	"hash"
	"hash/fnv"
)

// TODO : optimize

const (
	NHashes = 3
)

type Filter interface {
	AddKey([]byte, ...HashFunc)
	MayContain([]byte, ...HashFunc) bool
}

type bloomFilter struct {
	maxSize   uint
	size      uint
	nHashes   uint // might change later
	hashFuncs []hash.Hash64
	arr       []bool
}

type HashFunc = func(key []byte, hashFunctions []hash.Hash64) []uint

func initHashFunc(n int) (hashFuncs []hash.Hash64) {
	for i := 0; i < n; i++ {
		hashFuncs = append(hashFuncs, fnv.New64())
	}
	return
}

func NewFilter(maxSize uint) Filter {
	return &bloomFilter{
		maxSize:   maxSize,
		nHashes:   NHashes,
		size:      0,
		hashFuncs: initHashFunc(NHashes),
		arr:       make([]bool, maxSize),
	}
}

func (b *bloomFilter) defaultHashFn() func(key []byte, hashFunctions []hash.Hash64) (hashes []uint) {
	return func(key []byte, hashFunctions []hash.Hash64) (hashes []uint) {
		for _, hsf := range hashFunctions {
			if _, err := hsf.Write(key); err != nil {
				panic(err)
			}
			hashes = append(hashes, uint(hsf.Sum64()))
			hsf.Reset()
		}
		return
	}
}

func (b *bloomFilter) AddKey(key []byte, hs ...HashFunc) {
	hFn := b.defaultHashFn()
	if len(hs) == 1 {
		hFn = hs[0]
	}

	hashed := hFn(key, b.hashFuncs)

	for i := uint(0); i < b.nHashes; i++ {
		arrIdx := hashed[i] % b.maxSize
		b.arr[arrIdx] = true
	}

	b.size++
}

func (b *bloomFilter) MayContain(key []byte, hs ...HashFunc) bool {
	hFn := b.defaultHashFn()
	if len(hs) == 1 {
		hFn = hs[0]
	}

	hashed := hFn(key, b.hashFuncs)

	for i := uint(0); i < b.nHashes; i++ {
		arrIdx := hashed[i] % b.maxSize
		if !b.arr[arrIdx] {
			return false
		}
	}
	return true
}

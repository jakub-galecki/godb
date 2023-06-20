package bloom

import (
	"hash"
	"hash/fnv"
	"io"
)

// TODO : optimize

const (
	NHashes = 3
	MaxSize = 10000000
)

type Filter interface {
	AddKey([]byte, ...HashFunc)
	MayContain([]byte, ...HashFunc) bool
	Read(io.Reader) error
	Write(io.Writer) error
}

type bloomFilter struct {
	MaxSize   uint          `msgpack:"max_size"`
	Size      uint          `msgpack:"size"`
	NHashes   uint          `msgpack:"n_hashes"`
	HashFuncs []hash.Hash64 `msgpack:"-"`
	Arr       []bool        `msgpack:"arr,as_array"`
}

type HashFunc = func(key []byte, hashFunctions []hash.Hash64) []uint

func initHashFunc(n int) (hashFuncs []hash.Hash64) {
	for i := 0; i < n; i++ {
		// change to different hashing functions
		hashFuncs = append(hashFuncs, fnv.New64())
	}
	return
}

func NewFilter(maxSize uint) Filter {
	return &bloomFilter{
		MaxSize:   maxSize,
		NHashes:   NHashes,
		Size:      0,
		HashFuncs: initHashFunc(NHashes),
		Arr:       make([]bool, maxSize),
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

	hashed := hFn(key, b.HashFuncs)

	for i := uint(0); i < b.NHashes; i++ {
		arrIdx := hashed[i] % b.MaxSize
		b.Arr[arrIdx] = true
	}

	b.Size++
}

func (b *bloomFilter) MayContain(key []byte, hs ...HashFunc) bool {
	hFn := b.defaultHashFn()
	if len(hs) == 1 {
		hFn = hs[0]
	}

	hashed := hFn(key, b.HashFuncs)

	for i := uint(0); i < b.NHashes; i++ {
		arrIdx := hashed[i] % b.MaxSize
		if !b.Arr[arrIdx] {
			return false
		}
	}
	return true
}

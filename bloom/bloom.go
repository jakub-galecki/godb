package bloom

import (
	"hash"
	"hash/maphash"
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
	MaxSize   uint           `msgpack:"max_size"`
	Size      uint           `msgpack:"size"`
	NHashes   uint           `msgpack:"n_hashes"`
	HashFuncs []hash.Hash64  `msgpack:"-"`
	Seeds     []maphash.Seed `msgpack:"seeds,as_array"`
	Arr       []bool         `msgpack:"arr,as_array"`
}

type HashFunc = func(key []byte, hashFunctions []hash.Hash64) []uint

func initHashFunc(n int, seeds []maphash.Seed) (hashFuncs []hash.Hash64) {
	for i := 0; i < n; i++ {
		var hash maphash.Hash
		hash.SetSeed(seeds[i])
		hashFuncs = append(hashFuncs, &hash)
	}
	return
}

func initSeeds(n int) (seeds []maphash.Seed) {
	for i := 0; i < n; i++ {
		seeds = append(seeds, maphash.MakeSeed())
	}
	return
}

func NewFilter(maxSize uint) Filter {
	seeds := initSeeds(NHashes)
	return &bloomFilter{
		MaxSize:   maxSize,
		NHashes:   NHashes,
		Size:      0,
		Seeds:     seeds,
		HashFuncs: initHashFunc(NHashes, seeds),
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

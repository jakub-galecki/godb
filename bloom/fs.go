package bloom

import (
	"io"
	"log"

	"github.com/vmihailenco/msgpack/v5"
)

func (b *bloomFilter) Read(reader io.Reader) error {
	decoder := msgpack.NewDecoder(reader)
	if err := decoder.Decode(b); err != nil {
		return err
	}
	b.HashFuncs = initHashFunc(int(b.NHashes), b.Seeds)
	log.Printf("Creating new bloomFilter: size: %v, nHashes: %v, seeds: %v", b.Size, b.NHashes, b.Seeds)
	return nil
}

func (b *bloomFilter) Write(writer io.Writer) error {
	encoder := msgpack.NewEncoder(writer)
	encoder.UseArrayEncodedStructs(true)
	if err := encoder.Encode(b); err != nil {
		return err
	}
	return nil
}

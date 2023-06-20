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
	b.HashFuncs = initHashFunc(int(b.NHashes))
	log.Printf("Creating new bloomFilter: size: %v, nHashes: %v", b.Size, b.NHashes)
	return nil
}

func (b *bloomFilter) Write(writer io.Writer) error {
	encoder := msgpack.NewEncoder(writer)
	encoder.UseArrayEncodedStructs(true)
	log.Printf("Writing bloomFilter: size: %v, nHashes: %v", b.Size, b.NHashes)
	if err := encoder.Encode(b); err != nil {
		return err
	}
	return nil
}

package main

import (
	"fmt"
)

func main() {
	lsmt := NewStorageEngine("test")
	_ = lsmt.Set([]byte("foo"), []byte("bar"))
	val, found := lsmt.Get([]byte("foo"))
	if found {
		fmt.Printf("found value: [%s]\n", val)
	}
	lsmt.Delete([]byte("foo"))
}

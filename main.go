package main

import (
	"fmt"
)

func main() {
	lsmt := NewStorageEngine("test")
	for i := 0; i < 10000; i++ {
		_ = lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte("bar"))
	}
	for i := 0; i < 10000; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		if found {
			fmt.Printf("found value: [%s]\n", val)
		}
	}
	lsmt.Delete([]byte("foo"))
}

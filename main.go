package main

import "godb/lsmt"

func main() {
	lsmt := lsmt.NewStorageEngine("test")
	lsmt.Set([]byte("foo"), []byte("bar"))
	lsmt.Get([]byte("foo"))
	lsmt.Delete([]byte("foo"))
}

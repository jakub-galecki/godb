package main

import "fmt"

func main() {
	db := Open("test1")
	db.Set([]byte("t1"), []byte("t2"))
	v, _ := db.Get([]byte("t1"))
	fmt.Printf("%s", v)
}

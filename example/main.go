package main

import (
	"fmt"
	stdLog "log"
	"os"

	"github.com/jakub-galecki/godb"
	"github.com/jakub-galecki/godb/log"
)

func cleanup(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}
func main() {
	db, err := godb.Open("test", godb.WithDbPath("/tmp/"), godb.WithLogger(log.JsonLogger))
	if err != nil {
		stdLog.Panic(err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		if err != nil {
			stdLog.Panic(err)
		}
	}
	val, found := db.Get([]byte(fmt.Sprintf("foo.%d", 9999)))
	if !found {
		stdLog.Println("key not found")
		return
	}
	stdLog.Printf("value: %s", val)

	b := godb.NewBatch().
		Set([]byte("b_key1"), []byte("b_value1")).
		Set([]byte("b_key2"), []byte("b_value2")).
		Set([]byte("b_key3"), []byte("b_value3"))

	// batch should have seqNum equal to 0 as it is assigned by ApplyBatch function
	it := b.Iter()
	for {
		op, seq, key, val := it.Next()
		if op == 0 && key == nil && val == nil {
			// batch iterator exhausted
			break
		}
		stdLog.Printf("op: %d, seqNum: %d, key: %s, value: %s\n", op, seq, key, val)
	}
	err = db.ApplyBatch(b)
	if err != nil {
		stdLog.Panic(err)
	}
	// get value that was set in batch
	val, found = db.Get([]byte("b_key2"))
	if !found {
		stdLog.Println("batch key not found")
		return
	}
	stdLog.Printf("batch value: %s", val)

	cleanup("/tmp/test")
}

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
	cleanup("/tmp/test")
}

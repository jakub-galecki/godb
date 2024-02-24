package main

import (
	"fmt"
	"os"
	"runtime/pprof"
)

func main() {
	f, perr := os.Create("cpu.pprof")
	if perr != nil {
		panic(perr)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	lsmt := NewStorageEngine("test")
	for i := 0; i < 15000; i++ {
		_ = lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}
	for i := 0; i < 15000; i++ {
		val, found := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		if found {
			fmt.Printf("found value: [%s]\n", val)
		}
	}
	lsmt.Delete([]byte("foo"))

	memProfileFile, err := os.Create("mem.prof")
	if err != nil {
		panic(err)
	}
	defer memProfileFile.Close()

	// Write memory profile to file
	if err := pprof.WriteHeapProfile(memProfileFile); err != nil {
		panic(err)
	}
}

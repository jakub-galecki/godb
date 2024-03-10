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

	lsmt := NewStorageEngine(".", "test")
	for i := 0; i < 1000000; i++ {
		_ = lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}
	for i := 0; i < 1000000; i++ {
		_, _ = lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))

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

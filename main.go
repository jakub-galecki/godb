package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
)

func main() {
	f, perr := os.Create("cpu.pprof")
	if perr != nil {
		panic(perr)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// tf, err := os.Create("trace.out")
	// if err != nil {
	// 	panic(perr)
	// }

	// ttrace.Start(tf)

	lsmt, err := Open("tt111121")
    if err != nil {
        panic(err)
    }
	for i := 0; i < 10000; i++ {
		_ = lsmt.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}
	for i := 0; i < 10; i++ {
		valueFromDb, _ := lsmt.Get([]byte(fmt.Sprintf("foo.%d", i)))
		fmt.Printf("[%s]\n", valueFromDb)
	}
	lsmt.Delete([]byte("foo"))

	memProfileFile, err := os.Create("mem.prof")
	if err != nil {
		panic(err)
	}
	defer memProfileFile.Close()
	runtime.GC()
	// Write memory profile to file
	if err := pprof.WriteHeapProfile(memProfileFile); err != nil {
		panic(err)
	}

	// ttrace.Stop()
	// tf.Close()
}

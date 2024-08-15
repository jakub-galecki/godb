package main

import "os"

type cleaner struct {
	worker chan []string
}

func newClener() *cleaner {
	c := &cleaner{
		worker: make(chan []string),
	}
	go c.run()
	return c
}

func (c *cleaner) removeAsync(files []string) {
	c.worker <- files
}

func (c *cleaner) run() {
	for files := range c.worker {
		for _, file := range files {
			_ = os.Remove(file)
		}
	}
}

func (c *cleaner) removeSync(files []string) {
	for _, file := range files {
		_ = os.Remove(file)
	}
}

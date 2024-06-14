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

func (c *cleaner) schedule(files []string) {
	c.worker <- files
}

func (c *cleaner) run() {
	for files := range c.worker {
		for _, file := range files {
			os.Remove(file)
		}
	}
}

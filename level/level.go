package level

import "godb/sst"

type Level interface {
}

type level struct {
	id uint

	ssts []sst.SST
}

// https://www.sobyte.net/post/2022-07/go-linkname/?ref=cloudcentric.dev#2-golinkname-advanced-1-random-numbers

package skiplist

import (
	"math"
	_ "unsafe"
)

const (
	p = 0.5
)

var prob [maxLevel]uint32

func init() {
	prop := 1.0
	for i := 0; i < maxLevel; i++ {
		prob[i] = uint32(prop * float64(math.MaxUint32))
		prop *= p
	}
}

//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

func randomLevel() int {
	s := Uint32()
	lvl := 1
	for lvl < maxLevel && s <= prob[lvl] {
		lvl++
	}
	return lvl
}

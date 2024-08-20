package compaction

import "github.com/jakub-galecki/godb/sst"

type Options struct {
	LevelMultiplier      uint
	MaxBytesForLevelBase uint
	MaxLevels            uint8
	L0MaxFiles           uint
}

var DefaultOptions = &Options{
	LevelMultiplier:      10,
	MaxLevels:            4,
	MaxBytesForLevelBase: 128 * (1 << 20),
	L0MaxFiles:           4,
}

type LeveledCompaction struct {
	opt *Options
}

func NewLeveledCompaction(opt *Options) *LeveledCompaction {
	return &LeveledCompaction{
		opt: opt,
	}
}

func (l *LeveledCompaction) triggerL0Compaction() bool {
	return false
}

func (l *LeveledCompaction) compactL0(ssts []*sst.SST) bool {
	return false
}

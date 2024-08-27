package compaction

import (
	"bytes"
	"encoding/json"

	"github.com/jakub-galecki/godb/common"
	"github.com/jakub-galecki/godb/log"
	"github.com/jakub-galecki/godb/sst"
)

type Options struct {
	LevelMultiplier      int
	MaxBytesForLevelBase int
	MaxLevels            uint8
	L0MaxFiles           int
	BaseLevel            int
}

var DefaultOptions = &Options{
	LevelMultiplier:      10,
	MaxLevels:            4,
	MaxBytesForLevelBase: 128 * (1 << 20),
	L0MaxFiles:           2,
	BaseLevel:            1,
}

type CompactionReq struct {
	L0     *sst.Level
	Levels []*sst.Level
	Logger *log.Logger

	EstimatedSize  uint64 // todo
	IsL0Compaction bool
	SourceLevel    *sst.Level
	SourceTables   []*sst.SST
	TargetLevel    *sst.Level
	TargetTables   []*sst.SST
	Lower, Upper   common.Iterator

	selected *score
}

func (cr *CompactionReq) Json() []byte {
	getTableIds := func(tables []*sst.SST) []string {
		res := make([]string, 0)
		for _, table := range tables {
			res = append(res, table.GetId())
		}
		return res
	}

	if cr == nil {
		return []byte("{}")
	}
	repr := make(map[string]any)
	repr["l0_compaction"] = cr.IsL0Compaction
	repr["source_level"] = cr.SourceLevel.GetId()
	repr["source_tables"] = getTableIds(cr.SourceTables)
	repr["target_level"] = cr.TargetLevel.GetId()
	repr["target_tables"] = getTableIds(cr.TargetTables)
	raw, _ := json.Marshal(repr)
	return raw
}

type LeveledCompaction struct {
	opt              *Options
	targetLevelSizes []int64
}

func NewLeveledCompaction(opt *Options) *LeveledCompaction {
	lc := &LeveledCompaction{
		opt:              opt,
		targetLevelSizes: make([]int64, opt.MaxLevels),
	}
	lc.calculateTargetLevelSizes()
	return lc
}

func (l *LeveledCompaction) MaybeTriggerCompaction(req *CompactionReq) (*CompactionReq, error) {
	if len(req.Levels) < l.opt.BaseLevel || len(req.L0.GetTables()) == 0 {
		return nil, nil
	}
	if l.triggerL0Compaction(req) {
		return l.compactL0(req)
	}
	if l.triggerHigherLevelCompaction(req) {
		return l.compact(req)
	}
	return nil, nil
}

func (l *LeveledCompaction) triggerHigherLevelCompaction(req *CompactionReq) bool {
	scores := make(scores, l.opt.MaxLevels)
	l.calculateScores(req, scores)
	highest := scores.getHighest()
	if highest.val < 1.0 {
		return false
	}
	req.selected = highest
	return true
}

func (l *LeveledCompaction) compactL0(req *CompactionReq) (*CompactionReq, error) {
	targetLevel := req.Levels[l.opt.BaseLevel-1]
	baseLevelSst := targetLevel.GetTables()
	overlapping := l.getOverlappingTables(req.L0.GetTables(), baseLevelSst)
	level0iterators := func() []common.Iterator {
		res := make([]common.Iterator, 0, len(req.L0.GetTables()))
		for _, table := range req.L0.GetTables() {
			it, err := sst.NewSSTableIter(table)
			if err != nil {
				req.Logger.Err(err).Str("sst_id", table.GetId()).Msg("cannot create iterator from sst")
				continue
			}
			res = append(res, it)
		}
		return res
	}()
	l0MergeIter, err := NewMergeIter(level0iterators...)
	if err != nil {
		req.Logger.Error().Err(err).Msg("error while creating iterator for l0 tables")
		return nil, err
	}
	baseIter, err := sst.NewSSTablesIter(overlapping...)
	if err != nil {
		req.Logger.Error().Err(err).Msg("error while creating iterator for overlapping tables")
		return nil, err
	}
	req.Lower = l0MergeIter
	req.Upper = baseIter
	req.IsL0Compaction = true
	req.SourceLevel = req.L0
	req.SourceTables = req.L0.GetTables()
	req.TargetLevel = targetLevel
	req.TargetTables = overlapping
	return req, nil
}

func (l *LeveledCompaction) compact(req *CompactionReq) (*CompactionReq, error) {
	if req.selected == nil {
		return nil, nil
	}
	if req.selected.level == int(l.opt.MaxLevels) {
		// no more levels that could be compacted
		return nil, nil
	}
	sourceTables := func() []*sst.SST {
		return []*sst.SST{req.Levels[req.selected.level].GetOldest()}
	}()
	targetLevel := req.Levels[req.selected.level+1]
	targetLevelTables := targetLevel.GetTables()
	overlapping := l.getOverlappingTables(sourceTables, targetLevelTables)
	lowerIter, err := sst.NewSSTablesIter(sourceTables...)
	if err != nil {
		req.Logger.Error().Err(err).Msg("error while creating iterator for lower tables")
		return nil, err
	}
	upperIter, err := sst.NewSSTablesIter(overlapping...)
	if err != nil {
		req.Logger.Error().Err(err).Msg("error while creating iterator for upper tables")
		return nil, err
	}
	req.Lower = lowerIter
	req.Upper = upperIter
	req.IsL0Compaction = false
	req.TargetLevel = targetLevel
	req.TargetTables = overlapping
	req.SourceLevel = req.Levels[req.selected.level]
	req.SourceTables = sourceTables
	return req, nil
}

func (l *LeveledCompaction) triggerL0Compaction(req *CompactionReq) bool {
	return len(req.L0.GetTables()) >= l.opt.L0MaxFiles
}

func (l *LeveledCompaction) getOverlappingTables(lower, upper []*sst.SST) []*sst.SST {
	// lower and max value across all sorted string tables from lower level
	lowerMin := func() []byte {
		var min []byte
		for _, sst := range lower {
			min = common.Min(min, sst.GetMin())
		}
		return min
	}()
	lowerMax := func() []byte {
		var max []byte
		for _, sst := range lower {
			max = common.Max(max, sst.GetMax())
		}
		return max
	}()
	res := make([]*sst.SST, 0)
	for _, table := range upper {
		sstMin, sstMax := table.GetMin(), table.GetMax()
		//     lowerMin.....lowerMax
		//  lowerMiin.......lowerMax
		if bytes.Compare(sstMin, lowerMax) <= 0 &&
			bytes.Compare(sstMax, lowerMin) >= 0 {
			res = append(res, table)
		}
	}
	return res
}

func (l *LeveledCompaction) getLevelSz(req *CompactionReq) []int64 {
	sum := func(ssts []*sst.SST) int64 {
		var res int64
		for _, sst := range ssts {
			res += sst.GetFileSize()
		}
		return res
	}
	sz := make([]int64, len(req.Levels))
	for i := 0; i < len(req.Levels); i++ {
		sz[i] = sum(req.Levels[i].GetTables())
	}
	return sz
}

func (l *LeveledCompaction) calculateTargetLevelSizes() {
	baseLevelTargetSize := l.opt.MaxBytesForLevelBase
	for i := 0; i < int(l.opt.MaxLevels); i++ {
		l.targetLevelSizes[i] = int64(baseLevelTargetSize * (i + 1))
	}
}

type score struct {
	level int
	val   float64
}

func (l *LeveledCompaction) calculateScores(req *CompactionReq, scores scores) {
	levelSz := l.getLevelSz(req)
	for i := 0; i < len(levelSz); i++ {
		scores[i] = &score{
			level: i,
			val:   float64(levelSz[i]) / float64(l.targetLevelSizes[i]),
		}
	}
}

type scores []*score

func (ss scores) getHighest() *score {
	var max *score = nil
	for _, sc := range ss {
		if max == nil || max.val < sc.val {
			max = sc
		}
	}
	return max
}

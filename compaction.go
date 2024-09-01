package godb

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"

	"github.com/jakub-galecki/godb/compaction"
	"github.com/jakub-galecki/godb/sst"
)

const estimateBloomSz = 100000

type compactionRes struct {
	*compaction.CompactionReq

	outTables []*sst.SST
}

func (cr *compactionRes) Json() []byte {
	repr := make(map[string]any)
	outTables := make([]string, 0, len(cr.outTables))
	for _, table := range cr.outTables {
		outTables = append(outTables, table.GetId())
	}
	repr["out_tables"] = outTables
	res, _ := json.Marshal(repr)
	return res
}

// calling function must hold the l.mutex
func (l *db) compact(req *compaction.CompactionReq) (*compactionRes, error) {
	it, err := compaction.NewTwoLevelIter(req.Lower, req.Upper)
	if err != nil {
		return nil, err
	}
	cs := &compactionRes{
		CompactionReq: req,
		outTables:     make([]*sst.SST, 0),
	}
	sstid := l.getNextFileNum()
	l.mutex.Unlock()
	defer l.mutex.Lock()
	bd := sst.NewBuilder(req.Logger, req.TargetLevel.GetDir(),
		estimateBloomSz, strconv.FormatUint(sstid, 10)) // max sst size
	for k, v, err := it.SeekToFirst(); err == nil && k != nil; k, v, err = it.Next() {
		if bd.GetSize() >= l.opts.sstSize {
			out, err := bd.Finish()
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			if err == nil {
				cs.outTables = append(cs.outTables, out)
			}
			sstid = l.getNextFileNum()
			bd = sst.NewBuilder(req.Logger, req.TargetLevel.GetDir(), estimateBloomSz, strconv.FormatUint(sstid, 10))
		}
		bd.Add(k, v)
	}
	if bd.GetSize() > 0 {
		out, err := bd.Finish()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err == nil {
			cs.outTables = append(cs.outTables, out)
		}
	}
	return cs, nil
}

// calling function must hold the l.mutex
func (l *db) applyCompaction(res *compactionRes) {
	l.append(res.TargetLevel.GetId(), res.outTables...)
	l.remove(res.SourceLevel.GetId(), res.SourceTables...)
	l.remove(res.TargetLevel.GetId(), res.TargetTables...)
	if err := l.applyEnv(l); err != nil {
		l.opts.logger.Error().Err(err).Msg("failed to apply env after compaction")
		return
	}
	res.SourceLevel.Remove(res.SourceTables)
	res.TargetLevel.Remove(res.TargetTables)
	res.TargetLevel.Append(res.outTables)
	toDel := make([]string, 0)
	for _, table := range res.SourceTables {
		toDel = append(toDel, table.GetPath())
	}
	for _, table := range res.TargetTables {
		toDel = append(toDel, table.GetPath())
	}
	l.cleaner.removeSync(toDel)
}

func (l *db) getCompactionReq() *compaction.CompactionReq {
	return &compaction.CompactionReq{
		L0:     l.l0,
		Levels: l.levels,
		Logger: l.opts.logger,
	}
}

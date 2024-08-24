package godb

import (
	"strconv"

	"github.com/jakub-galecki/godb/compaction"
	"github.com/jakub-galecki/godb/sst"
)

type compactionRes struct {
	*compaction.CompactionReq

	outTables []*sst.SST
}

func (l *db) compact(req *compaction.CompactionReq) (*compactionRes, error) {
	it, err := compaction.NewMergeWrapperIter(req.Lower, req.Upper)
	if err != nil {
		return nil, err
	}
	sstid := l.getNextFileNum()
	// todo : check sst size
	bd := sst.NewBuilder(req.Logger, req.TargetLevel.GetDir(),
		l.opts.sstSize, strconv.FormatUint(sstid, 10)) // max sst size
	for k, v, err := it.SeekToFirst(); err == nil; k, v, err = it.Next() {
		bd.Add(k, v)
	}
	// unlock for i/o operation
	l.mutex.Unlock()
	out := bd.Finish()
	l.mutex.Lock()
	return &compactionRes{
		CompactionReq: req,
		outTables:     []*sst.SST{out},
	}, nil
}

func (l *db) applyCompaction(res *compactionRes) {
	if res.IsL0Compaction {

	}
}

// accquires db lock to read consittent state
func (l *db) getCompactionReq() *compaction.CompactionReq {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return &compaction.CompactionReq{
		L0:     l.l0,
		Levels: l.levels,
		Logger: l.opts.logger,
	}
}

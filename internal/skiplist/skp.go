package skiplist

import (
	"bytes"
	"errors"
	"godb/common"
	"math/rand"
)

var _ common.InnerStorage = (*skipList)(nil)

/*
Implementation based on:
	Maurice Herlihy and Nir Shavit. 2012. The Art of Multiprocessor Programming,
	Revised Reprint (1st. ed.). Morgan Kaufmann Publishers Inc., San Francisco, CA, USA.
*/

type skipList struct {
	maxLevel int
	head     *node
	tail     *node
}

func New(maxLvl int) *skipList {
	skp := &skipList{maxLevel: maxLvl}
	// todo:   min max value for sentinels
	skp.head = newSentinelNode(maxLvl)
	skp.tail = newSentinelNode(maxLvl)
	for i := range skp.head.forwards {
		skp.head.forwards[i].set(skp.tail, false)
	}
	return skp
}

func (skp *skipList) find(key []byte, preds, succs []*node) bool {
	var (
		marked bool

		pred, curr, succ *node
	)
	pred = skp.head
	for i := skp.maxLevel; i >= 0; i-- {
		curr = pred.forwards[i].getRef()
		for {
			succ = curr.forwards[i].get(&marked)
			for marked {
				curr = pred.forwards[i].getRef()
				succ = curr.forwards[i].get(&marked)
			}
			if bytes.Compare(curr.key, key) < 0 {
				pred = curr
				curr = succ
			} else {
				break
			}
		}
		preds[i] = pred
		succs[i] = curr
	}
	return bytes.Equal(curr.key, key)
}

func (skp *skipList) Set(key common.InternalKey, value []byte) {
	var (
		topLevel = randomLevel(skp.maxLevel)
		preds    = make([]*node, skp.maxLevel+1)
		succs    = make([]*node, skp.maxLevel+1)
		nd       = newNode(key.UserKey, value, topLevel)
	)

	for {
		found := skp.find(key.UserKey, preds, succs)
		if found {
			return
		}
		for i := 0; i < topLevel; i++ {
			succ := succs[i]
			nd.forwards[i].set(succ, false)
		}

		pred := preds[0]
		succ := succs[0]
		nd.forwards[0].set(succ, false)
		if !pred.forwards[0].compAndSet(succ, nd, false, false) {
			continue
		}
		for i := 1; i < topLevel; i++ {
			for {
				pred = preds[i]
				succ = succs[i]
				if pred.forwards[i].compAndSet(succ, nd, false, false) {
					break
				}
				_ = skp.find(key.UserKey, preds, succs)
			}
		}
		return
	}
}

func (skp *skipList) Delete(key common.InternalKey) bool {
	var (
		preds = make([]*node, skp.maxLevel+1)
		succs = make([]*node, skp.maxLevel+1)
		succ  *node
	)
	for {
		found := skp.find(key.UserKey, preds, succs)
		if !found {
			return false
		}
		toRemove := succs[0]
		marked := false
		for i := toRemove.level - 1; i >= 1; i-- {
			succ = toRemove.forwards[i].get(&marked)
			for !marked {
				toRemove.forwards[i].setMark(true)
				succ = toRemove.forwards[i].get(&marked)
			}
		}
		marked = false
		succ = toRemove.forwards[0].get(&marked)
		for {
			iMarkedIt := toRemove.forwards[0].compAndSet(succ, succ, false, true)
			if iMarkedIt {
				return true
			} else if marked {
				return false
			}
		}
	}
}

func (skp *skipList) Get(key []byte) ([]byte, error) {
	var (
		preds = make([]*node, skp.maxLevel+1)
		succs = make([]*node, skp.maxLevel+1)
	)

	found := skp.find(key, preds, succs)
	if found {
		return succs[0].value, nil
	}
	return nil, errors.New("not found")
}

func (skp *skipList) NewIter() common.InnerStorageIterator {
	return nil
}

func (skp *skipList) GetSize() uint64 {
	return 0
}

func randomLevel(maxLevel int) int {
	lvl := 1
	for lvl < maxLevel && rand.Intn(4) == 0 {
		lvl++
	}
	return lvl
}

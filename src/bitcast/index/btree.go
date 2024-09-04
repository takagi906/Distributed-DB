package index

import (
	"github.com/google/btree"
	"sync"
)

type Btree struct {
	bt   *btree.BTree
	lock *sync.RWMutex
}

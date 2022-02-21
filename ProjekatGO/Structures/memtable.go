package Structures

// Author: SV11/2020

import (
	"fmt"
)

type Memtable struct {
	maxSize  int
	skipList *SkipList
}

// NewMemtable returns a new Memtable.
func NewMemtable(maxSize int, skipList *SkipList) *Memtable {
	return &Memtable{maxSize: maxSize, skipList: skipList}
}

// MaxSize returns the max size of Memtable.
func (memtable *Memtable) MaxSize() int {
	return memtable.maxSize
}

// SkipList returns Memtable SkipList.
func (memtable *Memtable) SkipList() *SkipList {
	return memtable.skipList
}

// Add adds a node to SkipList, and returns sorted SkipListNode, SkipList head and SkipList tail.
func (memtable *Memtable) Add(node *SkipListNode) ([]*SkipListNode, *SkipListNode, *SkipListNode) {
	memtable.skipList.Add(node)
	if memtable.skipList.Size() == memtable.maxSize {
		return memtable.flush()
	}
	return nil, nil, nil
}

// flush empties SkipList, and returns sorted SkipListNode, SkipList head and SkipList tail.
func (memtable *Memtable) flush() ([]*SkipListNode, *SkipListNode, *SkipListNode) {
	var ret []*SkipListNode
	head := memtable.skipList.Head()
	tail := memtable.skipList.Tail()
	current := memtable.skipList.header[0]
	for {
		ret = append(ret, current)
		if len(current.linkedNodes) == 0 {
			break
		}
		current = current.linkedNodes[0]
	}
	memtable.skipList.header = []*SkipListNode{}
	memtable.skipList.height = 0
	memtable.skipList.size = 0
	return ret, head, tail
}

// Print prints out Memtable data.
func (memtable *Memtable) Print() {
	fmt.Println("======================")
	fmt.Println("Max size:", memtable.maxSize)
	memtable.skipList.Print()
}

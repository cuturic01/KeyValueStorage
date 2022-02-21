package Structures

// Author: SV11/2020

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type SkipListNode struct {
	key         string
	value       []byte
	linkedNodes []*SkipListNode
}

// Height returns current height of SkipList.
func (sn *SkipListNode) Height() int {
	return len(sn.linkedNodes)
}

// isLast checks if the node is last on the given level.
func (sn *SkipListNode) isLast(level int) bool {
	if level >= len(sn.linkedNodes) {
		return true
	}
	return false
}

// NewSkipListNode returns new SkipListNode.
func NewSkipListNode(key string, value []byte, linkedNodes []*SkipListNode) *SkipListNode {
	return &SkipListNode{key: key, value: value, linkedNodes: linkedNodes}
}

// Key returns SkipListNode key.
func (sn *SkipListNode) Key() string {
	return sn.key
}

// Value returns SkipListNode value.
func (sn *SkipListNode) Value() []byte {
	return sn.value
}

// deleteLinked unlinks the linked node on the current level.
func (sn *SkipListNode) deleteLinked(level int) []*SkipListNode {
	if level == sn.Height() {
		return sn.linkedNodes[:len(sn.linkedNodes)-1]
	} else {
		return append(sn.linkedNodes[:level], sn.linkedNodes[level+1:]...)
	}
}

type SkipList struct {
	maxHeight int
	height    int
	size      int
	header    []*SkipListNode
	tail      *SkipListNode
}

// NewSkipList returns new SkipList.
func NewSkipList(maxHeight int, data []*SkipListNode) *SkipList {
	rand.Seed(time.Now().UnixNano())
	if len(data) == 0 {
		return &SkipList{maxHeight: maxHeight, height: 0, size: 0, header: []*SkipListNode{}, tail: nil}
	}
	if len(data) > 1 {
		for i := 0; i < len(data)-1; i++ {
			data[i].linkedNodes = append(data[i].linkedNodes, data[i+1])
		}
	}
	header := []*SkipListNode{data[0]}

	temp := data
	if len(data) == 1 {
		return &SkipList{maxHeight: maxHeight, height: 1, size: 1, header: header, tail: data[len(data)-1]}
	}

	for i := 0; i < maxHeight-1; i++ {
		var nextLevel []*SkipListNode
		currentLevel := temp

		for _, node := range currentLevel {
			coinFlip := rand.Int31n(2)
			if coinFlip == 1 {
				if len(nextLevel) >= 1 {
					nextLevel[len(nextLevel)-1].linkedNodes = append(nextLevel[len(nextLevel)-1].linkedNodes, node)
				}
				nextLevel = append(nextLevel, node)
			}
		}
		if len(nextLevel) == 0 {
			break
		}
		temp = nextLevel
		header = append(header, nextLevel[0])

	}
	return &SkipList{maxHeight: maxHeight, height: len(header), size: len(data), header: header,
		tail: data[len(data)-1]}
}

// Size returns size of the SkipList.
func (sl *SkipList) Size() int {
	return sl.size
}

// Head returns first element of the SkipList.
func (sl *SkipList) Head() *SkipListNode {
	return sl.header[0]
}

// Tail returns last element of the SkipList.
func (sl *SkipList) Tail() *SkipListNode {
	return sl.tail
}

// isEmpty checks if the SkipList is empty.
func (sl *SkipList) isEmpty() bool {
	return sl.size == 0
}

// Add adds a new node to the SkipList
func (sl *SkipList) Add(sn *SkipListNode) {
	rand.Seed(time.Now().UnixNano())

	if sl.isEmpty() {
		sl.header = append(sl.header, sn)
		sl.tail = sn
		sl.size++
		sl.height++
		for {
			if sl.height == sl.maxHeight {
				break
			}
			coinFlip := rand.Int31n(2)
			if coinFlip == 1 {
				sl.header = append(sl.header, sn)
				sl.height++
			} else {
				break
			}
		}
		return
	}

	current := sl.header[len(sl.header)-1]
	var prev *SkipListNode
	level := len(sl.header) - 2
	var path []*SkipListNode
	for {
		if level == -1 && sn.key > current.key && len(current.linkedNodes) == 0 {
			path = append(path, current)
			break
		} else if level == -1 && current.key < sn.key && current.linkedNodes[level+1].key > sn.key {
			path = append(path, current)
			break
		} else if level == -1 && current.key > sn.key {
			path = append(path, current)
			break
		}

		if current.key == sn.key {
			current.value = sn.value
			return
		} else if sn.key < current.key {
			if prev != nil {
				path = append(path, prev)
				current = prev
				level--
			} else {
				path = append(path, current)
				current = sl.header[level]
				level--
			}
		} else if sn.key > current.key {
			if current.isLast(level + 1) {
				level--
				path = append(path, current)
				continue
			}
			prev = current
			current = current.linkedNodes[level+1]
		}
	}

	i := len(path) - 1
	for {
		if i < 0 {
			if sl.height == sl.maxHeight {
				break
			}
			sl.header = append(sl.header, sn)
			sl.height++
		} else {
			level = len(sl.header) - i - 1
			current = path[i]
			if sn.key > current.key {
				if !current.isLast(level) {
					next := current.linkedNodes[level]
					sn.linkedNodes = append(sn.linkedNodes, next)
					current.linkedNodes[level] = sn
				} else {
					current.linkedNodes = append(current.linkedNodes, sn)
				}
			} else if sn.key < current.key {
				sn.linkedNodes = append(sn.linkedNodes, current)
				if sl.header[level] == current {
					sl.header[level] = sn
				}
			}
		}
		i--
		coinFlip := rand.Int31n(2)
		if coinFlip == 1 {
			continue
		} else {
			break
		}
	}

	if len(sn.linkedNodes) == 0 {
		sl.tail = sn
	}

	sl.size++
}

// Delete deletes a node with the given key from the SkipList.
func (sl *SkipList) Delete(key string) (*SkipListNode, error) {
	current := sl.header[len(sl.header)-1]
	var prev *SkipListNode
	var ret *SkipListNode
	level := len(sl.header) - 2
	var path []*SkipListNode
	for {
		if level == -1 && key > current.key && len(current.linkedNodes) == 0 {
			return nil, errors.New("key doesn't exist")
		} else if level == -1 && current.key < key && current.linkedNodes[level+1].key > key {
			return nil, errors.New("key doesn't exist")
		} else if level == -1 && current.key > key {
			return nil, errors.New("key doesn't exist")
		}

		if key == current.key {
			if level == -1 && key == current.key {
				if prev != nil {
					path = append(path, prev)
					break
				}
				path = append(path, current)
				break
			}
			if prev != nil {
				path = append(path, prev)
				current = prev
				level--
			} else {
				path = append(path, current)
				current = sl.header[level]
				level--
			}
		} else if key < current.key {
			if prev != nil {
				current = prev
				level--
			} else {
				current = sl.header[level]
				level--
			}
		} else if key > current.key {
			if current.isLast(level + 1) {
				level--
				continue
			}
			prev = current
			current = current.linkedNodes[level+1]
		}
	}

	ret = &SkipListNode{key: current.key, value: current.value, linkedNodes: current.linkedNodes}

	help := len(path) - 1
	for i := len(path) - 1; i >= 0; i-- {
		level := help - i
		prev := path[i]
		var next *SkipListNode
		if level < current.Height() {
			next = current.linkedNodes[level]
		}
		if prev.key == current.key {
			if current.isLast(level) && sl.header[level] == current {
				sl.header = sl.header[:len(sl.header)-1]
				help--
				sl.height = len(sl.header)
			} else if !current.isLast(level) && sl.header[level] == current {
				sl.header[level] = next
			} else if current.isLast(level) && sl.header[level] != current {
				prev.linkedNodes = prev.deleteLinked(level)
			}
		} else if prev.key < current.key {
			if next != nil {
				prev.linkedNodes[level] = next
			} else {
				prev.linkedNodes = prev.deleteLinked(level)
				sl.tail = prev
			}
		}
	}
	sl.size--

	return ret, nil
}

// Find finds a node with the given key in the SkipList.
func (sl *SkipList) Find(key string) *SkipListNode {
	if sl.isEmpty() {
		return nil
	}

	current := sl.header[len(sl.header)-1]
	var prev *SkipListNode
	level := len(sl.header) - 2
	for {
		if level == -1 && key > current.key && len(current.linkedNodes) == 0 {
			return nil
		} else if level == -1 && current.key < key && current.linkedNodes[level+1].key > key {
			return nil
		} else if level == -1 && current.key > key {
			return nil
		}

		if current.key == key {
			if string(current.value[0]) == "1" {
				return nil
			}
			return current
		} else if key < current.key {
			if prev != nil {
				current = prev
				level--
			} else {
				current = sl.header[level]
				level--
			}
		} else if key > current.key {
			if current.isLast(level + 1) {
				level--
				continue
			}
			prev = current
			current = current.linkedNodes[level+1]
		}
	}
}

// Print prints out the SkipList data.
func (sl *SkipList) Print() {
	fmt.Println("Max height: ", sl.maxHeight)
	fmt.Println("Height: ", sl.height)
	fmt.Println("Size: ", sl.size)
	level := 0
	if len(sl.header) == 0 {
		return
	}
	current := sl.header[0]
	for level < sl.height {
		fmt.Print("Level ", level, ": ")
		for {
			if current.isLast(level) {
				fmt.Print("(", current.key, ",", string(current.value), ")", "\n")
				level++
				if level == sl.height {
					break
				}
				current = sl.header[level]
				break

			} else {
				fmt.Print("(", current.key, ",", string(current.value), ")", "----")
				current = current.linkedNodes[level]
			}
		}
	}
}

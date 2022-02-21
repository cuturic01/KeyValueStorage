package Structures

// Author: SV14/2020

import (
	"container/list"
	"errors"
	"fmt"
)

//NOTE: problems while decoding a []byte type value

type lruEntry struct {
	key   string
	value []byte
}

type CacheLRU struct {
	size        int
	listOfItems *list.List
	items       map[string]*list.Element
}

// NewCacheLRU creates a new structure, and we can start using it from then
func NewCacheLRU(size int) (*CacheLRU, error) {
	if size <= 0 {
		return nil, errors.New("size of cache must be grater than 0")
	}
	cLRU := &CacheLRU{
		size:        size,
		listOfItems: list.New(),
		items:       make(map[string]*list.Element),
	}
	return cLRU, nil
}

// AddToCache adds new item to cacheLRU and if item already exists it just moves it to the front of the list
func (cLRU *CacheLRU) AddToCache(key string, value []byte) (evicted bool) {
	if entry, ok := cLRU.items[key]; ok && string(value) == string(entry.Value.(*lruEntry).value) {
		cLRU.listOfItems.MoveToFront(entry)
		return false
	}

	ent := &lruEntry{key, value}
	entry := cLRU.listOfItems.PushFront(ent)
	cLRU.items[key] = entry

	evict := cLRU.listOfItems.Len() > cLRU.size
	if evict {
		cLRU.removeOldest()
	}
	return evict
}

// GetFromCache returns false if item does not exist, if item exists it returns its value and moves it to the front of
// the list
func (cLRU *CacheLRU) GetFromCache(key string) ([]byte, bool) {
	if entry, ok := cLRU.items[key]; ok {
		cLRU.listOfItems.MoveToFront(entry)
		if string(entry.Value.(*lruEntry).value[0]) == "1" {
			return nil, false
		}
		return entry.Value.(*lruEntry).value, true
	}
	return nil, false
}

// RemoveFromCache removes an item from the cache if it exists, if not, it returns false
func (cLRU *CacheLRU) RemoveFromCache(key string) (present bool) {
	if entry, ok := cLRU.items[key]; ok {
		cLRU.removeElement(entry)
		return true
	}
	return false
}

// removeOldest internal function which removes the oldest item from cache (the one used least recently)
func (cLRU *CacheLRU) removeOldest() {
	entry := cLRU.listOfItems.Back()
	if entry != nil {
		cLRU.removeElement(entry)
	}
}

// removeElement internal function which removes element from the list based on given element address
func (cLRU *CacheLRU) removeElement(entry *list.Element) {
	cLRU.listOfItems.Remove(entry)
	ent := entry.Value.(*lruEntry)
	delete(cLRU.items, ent.key)

}

// PrintList prints the list of keys in their exact order
func (cLRU *CacheLRU) PrintList() {
	for entry := cLRU.listOfItems.Front(); entry != nil; entry = entry.Next() {
		keyS := entry.Value.(*lruEntry).key
		fmt.Println(keyS)
	}
}

// PrintItems prints the keys and the values of all items in cache
func (cLRU *CacheLRU) PrintItems() {
	for key, val := range cLRU.items {
		fmt.Println("Key: ", key, "Value: ", string(val.Value.(*lruEntry).value))
	}
}

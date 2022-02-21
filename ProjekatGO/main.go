package main

import (
	"ProjekatGO/Structures"
	"fmt"
	"math"
)

// loadMemtable Loads memtable from WAL, if WAL is empty, returns empty memtable based on configuration.
func loadMemtable(config *Structures.Config) (*Structures.Wal, *Structures.Memtable) {
	directory := "Wal"
	empty, _ := Structures.IsEmptyDir(directory)
	w := Structures.Wal{
		DirectoryPath:      "Wal",
		MaxSegmentCapacity: int(config.WalSize),
	}
	var memtable *Structures.Memtable
	if empty {
		skipList := Structures.NewSkipList(int(math.Log2(float64(config.MemtableSize))), []*Structures.SkipListNode{})
		w.SetDefaultParameters()
		memtable = Structures.NewMemtable(int(config.MemtableSize), skipList)
	} else {
		memtable = w.ReadFromLastSegment(config)
	}
	return &w, memtable
}

// createCache Creates cache with size given in configuration file.
func createCache(config *Structures.Config) *Structures.CacheLRU {
	cl, err := Structures.NewCacheLRU(int(config.CacheSize))
	if err != nil {
		fmt.Println("Error while creating cache.")
	}
	return cl
}

// putDel Puts a record in the memtable and cache based on key and value. When memtable is full it flushes.
func putDel(key string, value []byte, tombstone string, wal *Structures.Wal, memtable *Structures.Memtable,
	cache *Structures.CacheLRU) {
	value = append([]byte(tombstone), value...)
	wal.AddWalRecord(key, value[1:], string(value[0]))
	node := Structures.NewSkipListNode(key, value, nil)
	skipListNodes, head, tail := memtable.Add(node)
	cache.AddToCache(key, value)
	// Flush
	if skipListNodes != nil {
		err := wal.RemoveAllSegments()
		if err != nil {
			return
		}
		_ = Structures.FormSSTable(skipListNodes, head.Key(), tail.Key(), 1)
	}
}

// put Calls putDel and sets tombstone to false.
func put(key string, value []byte, wal *Structures.Wal, memtable *Structures.Memtable,
	cache *Structures.CacheLRU) {
	putDel(key, value, "0", wal, memtable, cache)
}

// get Returns value for a given key. Path: memtable -> cache -> bloom -> summary -> index -> data
func get(key string, memtable *Structures.Memtable, cache *Structures.CacheLRU, config *Structures.Config) []byte {
	node := memtable.SkipList().Find(key)
	if node != nil {
		cache.AddToCache(key, node.Value())
		fmt.Println("Found in memtable.")
		return node.Value()[1:]
	}
	value, _ := cache.GetFromCache(key)
	if value != nil {
		fmt.Println("Found in cache.")
		return value[1:]
	}
	lsm := Structures.Lsm{}
	table := lsm.GetLatest(int(config.LSMLevels))
	_, value, f := Structures.GetRecord(table, key)
	if f {
		valueTomb := append([]byte("0"), value...)
		cache.AddToCache(key, valueTomb)
		fmt.Println("Found in SSTable.")
		return value
	}
	return nil
}

// del Calls putDel and sets tombstone to true.
func del(key string, wal *Structures.Wal, memtable *Structures.Memtable, cache *Structures.CacheLRU) {
	putDel(key, []byte("000"), "1", wal, memtable, cache)
}

// tryAgain If a user made too many requests it needs to wait for a set time rate.
func tryAgain() bool {
	var cnt string
	fmt.Println("-------------------")
	fmt.Println("No more tokens.")
	fmt.Println("1 Yes")
	fmt.Println("2 No")
	fmt.Print("Try again: ")
	_, err := fmt.Scanln(&cnt)
	if err != nil {
		return false
	}
	fmt.Println("-------------------")
	if cnt == "1" {
		return true
	} else {
		return false
	}
}

// loadCMSHLL Loads CMS and HLL structures from default path ("CMS_HLL/...")
func loadCMSHLL() (*Structures.CountMinSketch, *Structures.HyperLogLog) {
	var cms *Structures.CountMinSketch
	var hll *Structures.HyperLogLog
	empty, _ := Structures.IsEmptyDir("CMS_HLL")
	if empty {
		cms, _ = Structures.NewCMSWithEstimates(0.1, 0.1)
		hll = Structures.NewHyperLogLog(8)
	} else {
		cms = Structures.DeserializeCMS("CMS_HLL/cms.dat")
		hll = Structures.DeserializeHLL("CMS_HLL/hll.dat")
	}
	return cms, hll
}

// menu Main menu of the project.
func menu() {
	config := Structures.NewConfig("configuration.yaml")
	lsm := Structures.Lsm{}
	lsm.GenerateLevels(config)
	wal, memtable := loadMemtable(config)
	cache := createCache(config)
	bucket := Structures.NewBucket(config)
	cms, hll := loadCMSHLL()
	for {
		var key string
		var option string
		fmt.Println("1 Get")
		fmt.Println("2 Put")
		fmt.Println("3 Delete")
		fmt.Println("4 Compact")
		fmt.Println("5 Key frequency")
		fmt.Println("6 Distinct values")
		fmt.Println("7 Close")
		fmt.Print("Select option: ")
		_, err := fmt.Scanln(&option)
		if err != nil {
			fmt.Println(err)
			return
		}
		if option == "1" {
			if bucket.Check() {
				fmt.Print("Enter key: ")
				_, err := fmt.Scanln(&key)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("-------------------")
				value := get(key, memtable, cache, config)
				cms.Update(key)
				bucket.Remove()

				if value == nil {
					fmt.Println("Key doesn't exist.")
				} else {
					fmt.Println("Key:", key)
					fmt.Println("Value:", string(value))
				}
				fmt.Println("-------------------")
			} else {
				again := tryAgain()
				if again {
					continue
				} else {
					break
				}
			}

		} else if option == "2" {
			if bucket.Check() {
				var value []byte
				fmt.Print("Enter key: ")
				_, err := fmt.Scanln(&key)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Print("Enter value: ")
				_, err = fmt.Scanln(&value)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("-------------------")
				put(key, value, wal, memtable, cache)
				cms.Update(key)
				hll.Add(value)
				bucket.Remove()
			} else {
				again := tryAgain()
				if again {
					continue
				} else {
					break
				}
			}
		} else if option == "3" {
			if bucket.Check() {
				fmt.Print("Enter key: ")
				_, err := fmt.Scanln(&key)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("-------------------")
				del(key, wal, memtable, cache)
				cms.Update(key)
				bucket.Remove()
			} else {
				again := tryAgain()
				if again {
					continue
				} else {
					break
				}
			}
		} else if option == "4" {
			if bucket.Check() {
				Structures.CompactAll(config)
				bucket.Remove()
			} else {
				again := tryAgain()
				if again {
					continue
				} else {
					break
				}
			}
		} else if option == "5" {
			fmt.Print("Enter key: ")
			_, err := fmt.Scanln(&key)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(key, "frequency:", cms.Estimate(key))
			fmt.Println("-------------------")
		} else if option == "6" {
			fmt.Println("Distinct values:", hll.Estimate())
			fmt.Println("-------------------")
		} else if option == "7" {
			fmt.Println("-------------------")
			break

		} else {
			fmt.Println("Invalid option!")
			fmt.Println("-------------------")
		}
	}
	cms.SerializeCMS("CMS_HLL/cms.dat")
	hll.Serialize("CMS_HLL/hll.dat")
}

func main() {
	menu()
}

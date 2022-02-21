package Structures

// Author: SV14/2020

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

// Compact Performs a compaction between 2 SSTable-s.
func Compact(s1 *SSTable, s2 *SSTable, level int) {
	files1Data, errs1Data := os.OpenFile(s1.DirectoryPath+"/"+s1.DataPath, os.O_RDONLY|os.O_CREATE, 0666)
	if errs1Data != nil {
		log.Fatal(errs1Data)
	}

	files2Data, errs2Data := os.OpenFile(s2.DirectoryPath+"/"+s2.DataPath, os.O_RDONLY|os.O_CREATE, 0666)
	if errs2Data != nil {
		log.Fatal(errs2Data)
	}

	bf1 := DeserializeFilter(s1.DirectoryPath + "/" + s1.FilterPath)
	bf2 := DeserializeFilter(s2.DirectoryPath + "/" + s2.FilterPath)

	lsm := Lsm{}
	s3 := SSTable{}
	lsm.SetAttributes(&s3, level+1)
	errDir := os.Mkdir(s3.DirectoryPath, 0666)
	if errDir != nil {
		fmt.Println(errDir)
		return
	}
	files3Data, errs3Data := os.OpenFile(s3.DirectoryPath+"/"+s3.DataPath, os.O_WRONLY|os.O_CREATE, 0666)
	if errs1Data != nil {
		log.Fatal(errs3Data)
	}

	files3Index, errs3Index := os.OpenFile(s3.DirectoryPath+"/"+s3.IndexPath, os.O_WRONLY|os.O_CREATE, 0666)
	if errs3Index != nil {
		log.Fatal(errs3Index)
	}

	files3Summary, errs3Summary := os.OpenFile(s3.DirectoryPath+"/"+s3.SummaryPath, os.O_WRONLY|os.O_CREATE, 0666)
	if errs3Summary != nil {
		log.Fatal(errs3Summary)
	}

	key1, value1, tombstone1, timestamp1, n1 := ReadRecord(files1Data)
	key2, value2, tombstone2, timestamp2, n2 := ReadRecord(files2Data)

	firstIteration := true

	upper := ReturnUpperBound(s1.DirectoryPath+"/"+s1.SummaryPath, s2.DirectoryPath+"/"+s2.SummaryPath)

	offset := 0
	offsetSummary := 0

	bf3 := NewBloomFilter(int(bf1.N()+bf2.N()), 0.0001)
	var contents3 []Content

	for {
		if n1 == 0 && n2 == 0 {
			break
		} else if n1 == 0 && n2 != 0 {
			for {
				if n2 == 0 {
					break
				}
				if tombstone2 == "0" {
					valueTomb := append([]byte(tombstone2), value2...)
					offset, offsetSummary = WriteRecord(files3Data, files3Index, files3Summary, key2, valueTomb,
						offset, offsetSummary, timestamp2)
					bf3.Add(key2)
					myContent := MyContent{key: key2, value: value2}
					contents3 = append(contents3, myContent)
				}
				key2, value2, tombstone2, timestamp2, n2 = ReadRecord(files2Data)
			}
			break
		} else if n1 != 0 && n2 == 0 {
			for {
				if n1 == 0 {
					break
				}
				if tombstone1 == "0" {
					valueTomb := append([]byte(tombstone1), value1...)
					offset, offsetSummary = WriteRecord(files3Data, files3Index, files3Summary, key1, valueTomb,
						offset, offsetSummary, timestamp1)
					bf3.Add(key1)
					myContent := MyContent{key: key1, value: value1}
					contents3 = append(contents3, myContent)
				}
				key1, value1, tombstone1, timestamp1, n1 = ReadRecord(files1Data)
			}
			break
		} else {
			if key1 == key2 {
				if timestamp1 > timestamp2 {
					if tombstone1 == "0" {
						if firstIteration {
							lower := makeLowerBound(key1)
							insertHeader(files3Summary, lower, upper)
							firstIteration = false
						}
						valueTomb := append([]byte(tombstone1), value1...)
						offset, offsetSummary = WriteRecord(files3Data, files3Index, files3Summary, key1, valueTomb,
							offset, offsetSummary, timestamp1)
						bf3.Add(key1)
						myContent := MyContent{key: key1, value: value1}
						contents3 = append(contents3, myContent)
					}
				} else {
					if tombstone2 == "0" {
						if firstIteration {
							lower := makeLowerBound(key2)
							insertHeader(files3Summary, lower, upper)
							firstIteration = false
						}
						valueTomb := append([]byte(tombstone2), value2...)
						offset, offsetSummary = WriteRecord(files3Data, files3Index, files3Summary, key2, valueTomb,
							offset, offsetSummary, timestamp2)
						bf3.Add(key2)
						myContent := MyContent{key: key2, value: value2}
						contents3 = append(contents3, myContent)
					}
				}
				key1, value1, tombstone1, timestamp1, n1 = ReadRecord(files1Data)
				key2, value2, tombstone2, timestamp2, n2 = ReadRecord(files2Data)
			} else if key1 < key2 {
				if tombstone1 == "0" {
					if firstIteration {
						lower := makeLowerBound(key1)
						insertHeader(files3Summary, lower, upper)
						firstIteration = false
					}
					valueTomb := append([]byte(tombstone1), value1...)
					offset, offsetSummary = WriteRecord(files3Data, files3Index, files3Summary, key1, valueTomb,
						offset, offsetSummary, timestamp1)
					bf3.Add(key1)
					myContent := MyContent{key: key1, value: value1}
					contents3 = append(contents3, myContent)
				}
				key1, value1, tombstone1, timestamp1, n1 = ReadRecord(files1Data)
			} else {
				if tombstone2 == "0" {
					if firstIteration {
						lower := makeLowerBound(key2)
						insertHeader(files3Summary, lower, upper)
						firstIteration = false
					}
					valueTomb := append([]byte(tombstone2), value2...)
					offset, offsetSummary = WriteRecord(files3Data, files3Index, files3Summary, key2, valueTomb,
						offset, offsetSummary, timestamp2)
					bf3.Add(key2)
					myContent := MyContent{key: key2, value: value2}
					contents3 = append(contents3, myContent)
				}
				key2, value2, tombstone2, timestamp2, n2 = ReadRecord(files2Data)
			}
		}

	}

	files3Merkle, errs3Merkle := os.OpenFile(s3.DirectoryPath+"/"+s3.MerklePath, os.O_WRONLY|os.O_CREATE, 0666)
	if errs3Merkle != nil {
		log.Fatal(errs3Merkle)
	}
	merkleTree, _ := NewTree(contents3)
	SerializeTree(merkleTree.root, files3Merkle, -1)

	bf3.Serialize(s3.DirectoryPath + "/" + s3.FilterPath)
	err := files1Data.Close()
	if err != nil {
		return
	}
	err = files2Data.Close()
	if err != nil {
		return
	}
	err = files3Data.Close()
	if err != nil {
		return
	}
	err = files3Index.Close()
	if err != nil {
		return
	}
	err = files3Summary.Close()
	if err != nil {
		return
	}
	err = files3Merkle.Close()
	if err != nil {
		return
	}

	err = os.RemoveAll(s1.DirectoryPath)
	if err != nil {
		return
	}
	err = os.RemoveAll(s2.DirectoryPath)
	if err != nil {
		return
	}

}

// makeLowerBound Based on 2 summaries, returns the optimal lower bound.
func makeLowerBound(key string) []byte {
	var keyFix [KeySizeSize]byte
	keyBytes := []byte(key)

	keySize := len(keyBytes)
	keySizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(keySizeBytes, uint32(keySize))

	copy(keyFix[(KeySizeSize-len(keySizeBytes)):], keySizeBytes)
	lowerRecord := append(keyFix[:], keyBytes...)
	return lowerRecord
}

// insertHeader Inserts header into summary based on file.
func insertHeader(fileSummary *os.File, lower []byte, upper []byte) {
	record := append(lower, upper...)
	_, errWriteSummary := fileSummary.Write(record)
	if errWriteSummary != nil {
		log.Fatal(errWriteSummary)
	}

}

// CompactAll Calls Compact for each 2 SSTable-s on all levels expect the last one.
func CompactAll(config *Config) {
	for i := 1; i < int(config.LSMLevels)-1; i++ {
		lvlPath := "LSM/C" + strconv.Itoa(i)
		dirs, err := ioutil.ReadDir(lvlPath)
		if err != nil {
			_, err2 := fmt.Fprintln(os.Stderr, err)
			if err2 != nil {
				return
			}
		}
		if len(dirs) < config.LvlTables[i] {
			continue
		}

		sort.Slice(dirs, func(i, j int) bool {
			return dirs[i].ModTime().Before(dirs[j].ModTime())
		})

		for j := 0; j < len(dirs); j += 2 {
			if j+1 == len(dirs) {
				break
			}
			s1 := tableFPath(lvlPath + "/" + dirs[j].Name())
			s2 := tableFPath(lvlPath + "/" + dirs[j+1].Name())
			Compact(s1, s2, i)
		}
	}
}

// tableFPath Returns SSTable based on given path.
func tableFPath(path string) *SSTable {
	s := SSTable{
		DirectoryPath: path,
		DataPath:      "sstable-data.dat",
		IndexPath:     "sstable-index.dat",
		SummaryPath:   "sstable-summary.dat",
		FilterPath:    "sstable-filter.dat",
		MerklePath:    "metadata.dat"}
	return &s
}

package Structures

// Author: SV11/2020

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

type Tree interface {
	SetAttributes(s *SSTable, level int)
	GetLatest(maxLevel int) *SSTable
	GenerateLevels(c *Config)
}

type Lsm struct{}

// SetAttributes sets SSTable attributes.
func (lsm Lsm) SetAttributes(s *SSTable, level int) {
	levelPath := "LSM/C" + strconv.Itoa(level)
	empty, _ := IsEmptyDir(levelPath)
	var numSSTable int
	if empty {
		numSSTable = 1
	} else {
		dirs, err := ioutil.ReadDir(levelPath)
		if err != nil {
			_, err2 := fmt.Fprintln(os.Stderr, err)
			if err2 != nil {
				return
			}
		}
		var modTime time.Time
		name := ""
		for _, dir := range dirs {
			if dir.Mode().IsDir() {
				if !dir.ModTime().Before(modTime) {
					if dir.ModTime().After(modTime) {
						name = ""
					}
					name = dir.Name()
				}
			}
		}
		if name != "" {
			numSSTable, _ = strconv.Atoi(name[7:])
			numSSTable++
		}
	}

	directoryPath := levelPath + "/SSTable" + strconv.Itoa(numSSTable)
	s.DirectoryPath = directoryPath
	s.DataPath = "sstable-data.dat"
	s.IndexPath = "sstable-index.dat"
	s.SummaryPath = "sstable-summary.dat"
	s.FilterPath = "sstable-filter.dat"
	s.MerklePath = "metadata.dat"

}

// GetLatest returns the newest SSTable on the highest level.
func (lsm Lsm) GetLatest(maxLevel int) *SSTable {
	s := SSTable{}
	levelPath := "LSM/C" + strconv.Itoa(maxLevel-1)
	for {
		empty, _ := IsEmptyDir(levelPath)
		if !empty {
			dirs, err := ioutil.ReadDir(levelPath)
			if err != nil {
				_, err2 := fmt.Fprintln(os.Stderr, err)
				if err2 != nil {
					return nil
				}
			}
			var modTime time.Time
			name := ""
			for _, dir := range dirs {
				if dir.Mode().IsDir() {
					if !dir.ModTime().Before(modTime) {
						if dir.ModTime().After(modTime) {
							name = ""
						}
						name = dir.Name()
					}
				}
			}
			s.DirectoryPath = levelPath + "/" + name
			s.DataPath = "sstable-data.dat"
			s.IndexPath = "sstable-index.dat"
			s.SummaryPath = "sstable-summary.dat"
			s.FilterPath = "sstable-filter.dat"
			s.MerklePath = "metadata.dat"
			break
		} else {
			maxLevel--
			if maxLevel == 0 {
				return nil
			}
			levelPath = "LSM/C" + strconv.Itoa(maxLevel)
			continue
		}
	}
	return &s

}

// GenerateLevels generates LSM levels based on configuration.
func (lsm Lsm) GenerateLevels(c *Config) {
	for i := 1; i < int(c.LSMLevels); i++ {
		err := os.Mkdir("LSM/C"+strconv.Itoa(i), 0666)
		if err != nil {
			continue
		}
	}
}

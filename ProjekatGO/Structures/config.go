package Structures

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

const (
	DirectoryPath = "Configuration"
)

type Config struct {
	WalSize      uint64      `yaml:"wal_size"`
	MemtableSize uint64      `yaml:"memtable_size"`
	LSMLevels    uint64      `yaml:"lsm_levels"`
	CacheSize    uint64      `yaml:"cache_size"`
	Threshold    uint8       `yaml:"threshold"`
	TimeRate     int         `yaml:"time_rate"`
	LvlTables    map[int]int `yaml:"lvl_tables"`
}

// NewConfig returns a new Config from given configuration file.
func NewConfig(fileName string) (config *Config) {
	b, _ := IsEmptyDir(DirectoryPath)
	if b {
		return defaultConfig()
	}

	configData, err := ioutil.ReadFile(DirectoryPath + "/" + fileName)
	if err != nil {
		log.Fatal(err)
	}
	err1 := yaml.Unmarshal(configData, &config)
	if err1 != nil {
		return nil
	}
	_, err2 := yaml.Marshal(config)
	if err2 != nil {
		log.Fatal(err2)
	}
	return
}

// defaultConfig creates default Config.
func defaultConfig() (config *Config) {
	return &Config{
		WalSize:      5,
		MemtableSize: 10,
		LSMLevels:    4,
		CacheSize:    5,
		Threshold:    5,
		TimeRate:     30,
		LvlTables:    map[int]int{1: 4, 2: 2, 3: 1}}
}

// Info prints Config data.
func (c *Config) Info() {
	fmt.Println("WalSize: ", c.WalSize)
	fmt.Println("MemtableSize: ", c.MemtableSize)
	fmt.Println("LSMLevels: ", c.LSMLevels)
	fmt.Println("CacheSize: ", c.CacheSize)
	fmt.Println("Threshold: ", c.Threshold)
	fmt.Println("LvlTables: ", c.LvlTables)
}

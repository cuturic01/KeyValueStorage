package Structures

// Author: SV46/2020

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	CrcSize            = 4
	TimestampSize      = 16
	TombstoneSize      = 1
	KeySizeSize        = 8
	ValueSizeSize      = 8
	OffsetSize         = 16
	DefaultSegmentPath = "wal_0001.log"
)

type Wal struct {
	DirectoryPath             string
	ActiveSegmentPath         string
	MaxSegmentCapacity        int
	NumOfActiveSegmentRecords int
}

// AddWalRecord is used to add a new record to the last WAL segment.
func (w *Wal) AddWalRecord(key string, value []byte, tombstone string) {
	keyBytes := []byte(key)

	crc := CRC32(append(keyBytes, value...))
	crcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBytes, crc)

	timestamp := time.Now().Unix()
	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(timestamp))

	tombstoneBytes := []byte(tombstone)

	keySize := len(keyBytes)
	keySizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(keySizeBytes, uint32(keySize))

	valueSize := len(value)
	valueSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(valueSizeBytes, uint32(valueSize))

	var crcFix [CrcSize]byte
	var timestampFix [TimestampSize]byte
	var tombstoneFix [TombstoneSize]byte
	var keySizeFix [KeySizeSize]byte
	var valueSizeFix [ValueSizeSize]byte

	copy(crcFix[(CrcSize-len(crcBytes)):], crcBytes)
	copy(timestampFix[(TimestampSize-len(timestampBytes)):], timestampBytes)
	copy(tombstoneFix[(TombstoneSize-len(tombstoneBytes)):], tombstoneBytes)
	copy(keySizeFix[(KeySizeSize-len(keySizeBytes)):], keySizeBytes)
	copy(valueSizeFix[(ValueSizeSize-len(valueSizeBytes)):], valueSizeBytes)
	_ = os.Mkdir(w.DirectoryPath, 0666)

	if w.NumOfActiveSegmentRecords == w.MaxSegmentCapacity {
		w.ActiveSegmentPath = generateNextPath(w.ActiveSegmentPath)
		w.NumOfActiveSegmentRecords = 0
	}

	file, err := os.OpenFile(w.DirectoryPath+"/"+w.ActiveSegmentPath, os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	record := append(crcFix[:], timestampFix[:]...)
	record = append(record, tombstoneFix[:]...)
	record = append(record, keySizeFix[:]...)
	record = append(record, valueSizeFix[:]...)
	record = append(record, keyBytes...)
	record = append(record, value...)

	_, errWrite := file.Write(record)
	if errWrite != nil {
		log.Fatal(errWrite)
	}

	w.NumOfActiveSegmentRecords += 1

}

// CRC32 is function taken from helper file that calculates checksum of given data.
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// generateNextPath is used to create filename for new WAL segment when previous segment is filled.
func generateNextPath(currentPath string) (newPath string) {
	intPath := currentPath[4:8]
	segmentNumber, _ := strconv.Atoi(intPath)
	segmentNumber += 1
	if segmentNumber > 9999 {
		log.Fatal("You've reached maximum number of segments, 9999")
	}
	newPath = "wal_" + fmt.Sprintf("%04d", segmentNumber) + ".log"
	return
}

// IsEmptyDir is function that checks if given directory is empty.
func IsEmptyDir(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// ReadFromWalSegment reads data from given file, writes data to Mem table, and returns Mem table.
func (w *Wal) ReadFromWalSegment(segmentPath string, config *Config) *Memtable {
	skipList := NewSkipList(int(math.Log2(float64(config.MemtableSize))), []*SkipListNode{})
	file, err := os.OpenFile(w.DirectoryPath+"/"+segmentPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	for {
		crcBytes := make([]byte, CrcSize)
		n, _ := file.Read(crcBytes)
		if n == 0 {
			break
		}

		timestampBytes := make([]byte, TimestampSize)
		_, _ = file.Read(timestampBytes)

		tombstoneBytes := make([]byte, TombstoneSize)
		_, _ = file.Read(tombstoneBytes)

		keySizeBytes := make([]byte, KeySizeSize)
		_, _ = file.Read(keySizeBytes)
		keySize := binary.LittleEndian.Uint32(keySizeBytes)

		valueSizeBytes := make([]byte, ValueSizeSize)
		_, _ = file.Read(valueSizeBytes)
		valueSize := binary.LittleEndian.Uint32(valueSizeBytes)

		keyBytes := make([]byte, keySize)
		_, _ = file.Read(keyBytes)

		valueBytes := make([]byte, valueSize)
		_, _ = file.Read(valueBytes)

		valueBytes = append(tombstoneBytes, valueBytes...)

		key := string(keyBytes)
		node := NewSkipListNode(key, valueBytes, nil)
		skipList.Add(node)

	}
	memtable := NewMemtable(int(config.MemtableSize), skipList)
	return memtable
}

// GetLastSegment is used to find the path to last WAL segment. Data from this segment will be written into Mem table
// if needed.
func (w *Wal) GetLastSegment() (segmentPath string) {
	files, err := ioutil.ReadDir(w.DirectoryPath)
	if err != nil {
		_, err2 := fmt.Fprintln(os.Stderr, err)
		if err2 != nil {
			return
		}
	}
	var modTime time.Time
	name := ""
	for _, fi := range files {
		if fi.Mode().IsRegular() {
			if !fi.ModTime().Before(modTime) {
				if fi.ModTime().After(modTime) {
					name = ""
				}
				name = fi.Name()
			}
		}
	}
	if name != "" {
		segmentPath = name
	} else {
		fmt.Println("Directory is empty.")
	}
	return
}

// ReadFromLastSegment sets values for active WAL segment path and number of records in active WAL segment. Active path
// was found by checking the latest segment that was created (GetLastSegment). While data from last segment was
// read, reading function (ReadFromWalSegment) also kept track of number of records in last segment.
func (w *Wal) ReadFromLastSegment(config *Config) *Memtable {
	lastSegmentPath := w.GetLastSegment()
	memtable := w.ReadFromWalSegment(lastSegmentPath, config)
	w.ActiveSegmentPath = lastSegmentPath
	w.NumOfActiveSegmentRecords = memtable.SkipList().Size()
	return memtable
}

// SetDefaultParameters sets default values for active WAL segment path and number of records in active WAL segment.
func (w *Wal) SetDefaultParameters() {
	w.ActiveSegmentPath = DefaultSegmentPath
	w.NumOfActiveSegmentRecords = 0
}

// RemoveAllSegments removes all files from parent directory.
func (w *Wal) RemoveAllSegments() error {
	files, err := filepath.Glob(filepath.Join(w.DirectoryPath, "*"))
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	w.SetDefaultParameters()
	return nil
}

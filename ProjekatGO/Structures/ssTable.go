package Structures

// Author: SV46/2020

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"
)

type SSTable struct {
	DirectoryPath string
	FilterPath    string
	DataPath      string
	IndexPath     string
	SummaryPath   string
	TOCPath       string
	MerklePath    string
}

// FormSSTable forms a new SSTable with data from memtable.
func FormSSTable(memtableData []*SkipListNode, lowerBound string, upperBound string, level int) SSTable {
	s := SSTable{}
	lsm := Lsm{}
	lsm.SetAttributes(&s, level)

	errDir := os.Mkdir(s.DirectoryPath, 0666)
	if errDir != nil {
		return SSTable{}
	}

	fileData, errData := os.OpenFile(s.DirectoryPath+"/"+s.DataPath, os.O_WRONLY|os.O_CREATE, 0666)
	if errData != nil {
		log.Fatal(errData)
	}
	fileIndex, errIndex := os.OpenFile(s.DirectoryPath+"/"+s.IndexPath, os.O_WRONLY|os.O_CREATE, 0666)
	if errIndex != nil {
		log.Fatal(errIndex)
	}
	fileSummary, errSummary := os.OpenFile(s.DirectoryPath+"/"+s.SummaryPath, os.O_WRONLY|os.O_CREATE, 0666)
	if errSummary != nil {
		log.Fatal(errSummary)
	}
	fileMerkle, errMerkle := os.OpenFile(s.DirectoryPath+"/"+s.MerklePath, os.O_WRONLY|os.O_CREATE, 0666)
	if errMerkle != nil {
		log.Fatal(errMerkle)
	}

	bf := NewBloomFilter(len(memtableData), 0.001)

	lowerBoundBytes := []byte(lowerBound)
	upperBoundBytes := []byte(upperBound)

	lowerBoundSize := len(lowerBoundBytes)
	lowerBoundSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(lowerBoundSizeBytes, uint32(lowerBoundSize))

	upperBoundSize := len(upperBoundBytes)
	upperBoundSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(upperBoundSizeBytes, uint32(upperBoundSize))

	var lowerFix [KeySizeSize]byte
	var upperFix [KeySizeSize]byte

	copy(lowerFix[(KeySizeSize-len(lowerBoundSizeBytes)):], lowerBoundSizeBytes)
	copy(upperFix[(KeySizeSize-len(upperBoundSizeBytes)):], upperBoundSizeBytes)

	headerRecord := append(lowerFix[:], lowerBoundBytes...)
	headerRecord = append(headerRecord, upperFix[:]...)
	headerRecord = append(headerRecord, upperBoundBytes...)

	_, errWriteSummary := fileSummary.Write(headerRecord)
	if errWriteSummary != nil {
		log.Fatal(errWriteSummary)
	}

	offset := 0
	offsetSummary := 0

	var contents []Content

	for _, node := range memtableData {
		key := node.Key()
		value := node.Value()

		myContent := MyContent{key: key, value: value[1:]}
		contents = append(contents, myContent)

		bf.Add(key)
		keyBytes := []byte(key)

		crc := CRC32(append(keyBytes, value[1:]...))
		crcBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcBytes, crc)

		timestamp := time.Now().Unix()
		timestampBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestampBytes, uint64(timestamp))

		tombstoneBytes := value[:1]

		keySize := len(keyBytes)
		keySizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(keySizeBytes, uint32(keySize))

		valueSize := len(value) - 1
		valueSizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(valueSizeBytes, uint32(valueSize))

		offsetBytes := make([]byte, OffsetSize)
		binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))

		offsetSummaryBytes := make([]byte, OffsetSize)
		binary.LittleEndian.PutUint64(offsetSummaryBytes, uint64(offsetSummary))

		var crcFix [CrcSize]byte
		var timestampFix [TimestampSize]byte
		var tombstoneFix [TombstoneSize]byte
		var keySizeFix [KeySizeSize]byte
		var valueSizeFix [ValueSizeSize]byte
		var offsetFix [OffsetSize]byte
		var offsetSummaryFix [OffsetSize]byte

		copy(crcFix[(CrcSize-len(crcBytes)):], crcBytes)
		copy(timestampFix[(TimestampSize-len(timestampBytes)):], timestampBytes)
		copy(tombstoneFix[(TombstoneSize-len(tombstoneBytes)):], tombstoneBytes)
		copy(keySizeFix[(KeySizeSize-len(keySizeBytes)):], keySizeBytes)
		copy(valueSizeFix[(ValueSizeSize-len(valueSizeBytes)):], valueSizeBytes)
		copy(offsetFix[(OffsetSize-len(offsetBytes)):], offsetBytes)
		copy(offsetSummaryFix[(OffsetSize-len(offsetSummaryBytes)):], offsetSummaryBytes)

		recordData := append(crcFix[:], timestampFix[:]...)
		recordData = append(recordData, tombstoneFix[:]...)
		recordData = append(recordData, keySizeFix[:]...)
		recordData = append(recordData, valueSizeFix[:]...)
		recordData = append(recordData, keyBytes...)
		recordData = append(recordData, value[1:]...)

		_, errWriteData := fileData.Write(recordData)
		if errWriteData != nil {
			log.Fatal(errWriteData)
		}

		indexRecord := append(keySizeFix[:], keyBytes...)
		summaryRecord := indexRecord
		indexRecord = append(indexRecord, offsetFix[:]...)

		_, errWriteIndex := fileIndex.Write(indexRecord)
		if errWriteIndex != nil {
			log.Fatal(errWriteIndex)
		}

		offset += len(recordData)

		summaryRecord = append(summaryRecord, offsetSummaryFix[:]...)

		_, errWriteSummary1 := fileSummary.Write(summaryRecord)
		if errWriteSummary1 != nil {
			log.Fatal(errWriteSummary1)
		}

		offsetSummary += len(indexRecord)

	}

	merkleTree, _ := NewTree(contents)
	SerializeTree(merkleTree.root, fileMerkle, -1)
	bf.Serialize(s.DirectoryPath + "/" + s.FilterPath)

	err := fileData.Close()
	if err != nil {
		return SSTable{}
	}
	err1 := fileIndex.Close()
	if err1 != nil {
		return SSTable{}
	}
	err2 := fileSummary.Close()
	if err2 != nil {
		return SSTable{}
	}
	err3 := fileMerkle.Close()
	if err3 != nil {
		return SSTable{}
	}

	return s
}

// GetRecord returns record with the given key from SSTable.
func GetRecord(s *SSTable, keyGiven string) (string, []byte, bool) {
	bf := DeserializeFilter(s.DirectoryPath + "/" + s.FilterPath)
	if !bf.Check(keyGiven) {
		return "", nil, false
	}
	fileSummary, errSummary := os.OpenFile(s.DirectoryPath+"/"+s.SummaryPath, os.O_RDONLY, 0666)
	if errSummary != nil {
		log.Fatal(errSummary)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(fileSummary)

	lowerBoundSizeBytes := make([]byte, KeySizeSize)
	x, _ := fileSummary.Read(lowerBoundSizeBytes)
	if x == 0 {
		return "", nil, false
	}

	lowerBoundSize := binary.LittleEndian.Uint32(lowerBoundSizeBytes)
	lowerBoundBytes := make([]byte, lowerBoundSize)
	_, _ = fileSummary.Read(lowerBoundBytes)
	lowerBound := string(lowerBoundBytes)
	if keyGiven < lowerBound {
		return "", nil, false
	}

	upperBoundSizeBytes := make([]byte, KeySizeSize)
	y, _ := fileSummary.Read(upperBoundSizeBytes)
	if y == 0 {
		return "", nil, false
	}

	upperBoundSize := binary.LittleEndian.Uint32(upperBoundSizeBytes)
	upperBoundBytes := make([]byte, upperBoundSize)
	_, _ = fileSummary.Read(upperBoundBytes)
	upperBound := string(upperBoundBytes)
	if keyGiven > upperBound {
		return "", nil, false
	}

	for {
		keySizeBytes := make([]byte, KeySizeSize)
		n, _ := fileSummary.Read(keySizeBytes)
		if n == 0 {
			break
		}
		keySize := binary.LittleEndian.Uint32(keySizeBytes)
		keyBytes := make([]byte, keySize)
		_, _ = fileSummary.Read(keyBytes)
		keyIndex := string(keyBytes)
		if keyIndex == keyGiven {
			offsetIndexBytes := make([]byte, OffsetSize)
			_, _ = fileSummary.Read(offsetIndexBytes)
			offsetIndex := binary.LittleEndian.Uint64(offsetIndexBytes)

			fileIndex, errIndex := os.OpenFile(s.DirectoryPath+"/"+s.IndexPath, os.O_RDONLY, 0666)
			if errIndex != nil {
				log.Fatal(errIndex)
			}
			_, errSeek := fileIndex.Seek(int64(offsetIndex), 0)
			if errSeek != nil {
				return "", nil, false
			}
			keySizeBytesIndex := make([]byte, KeySizeSize)
			_, _ = fileIndex.Read(keySizeBytesIndex)
			keySizeIndex := binary.LittleEndian.Uint32(keySizeBytesIndex)
			keyBytesIndex := make([]byte, keySizeIndex)
			_, _ = fileIndex.Read(keyBytesIndex)

			offsetDataBytes := make([]byte, OffsetSize)
			_, _ = fileIndex.Read(offsetDataBytes)
			offsetData := binary.LittleEndian.Uint64(offsetDataBytes)

			errCloseIndex := fileIndex.Close()
			if errCloseIndex != nil {
				return "", nil, false
			}

			fileData, errData := os.OpenFile(s.DirectoryPath+"/"+s.DataPath, os.O_RDONLY, 0666)
			if errData != nil {
				log.Fatal(errData)
			}
			_, errSeek2 := fileData.Seek(int64(offsetData), 0)
			if errSeek2 != nil {
				return "", nil, false
			}

			crcBytes := make([]byte, CrcSize)
			_, _ = fileData.Read(crcBytes)

			timestampBytes := make([]byte, TimestampSize)
			_, _ = fileData.Read(timestampBytes)

			tombstoneBytes := make([]byte, TombstoneSize)
			_, _ = fileData.Read(tombstoneBytes)

			keySizeBytesData := make([]byte, KeySizeSize)
			_, _ = fileData.Read(keySizeBytesData)
			keySizeData := binary.LittleEndian.Uint32(keySizeBytesData)

			valueSizeBytes := make([]byte, ValueSizeSize)
			_, _ = fileData.Read(valueSizeBytes)
			valueSize := binary.LittleEndian.Uint32(valueSizeBytes)

			keyBytesData := make([]byte, keySizeData)
			_, _ = fileData.Read(keyBytesData)

			valueBytes := make([]byte, valueSize)
			_, _ = fileData.Read(valueBytes)

			errData1 := fileData.Close()
			if errData1 != nil {
				return "", nil, false
			}

			return string(keyBytesData), valueBytes, true

		} else {
			_, errSeek := fileSummary.Seek(OffsetSize, 1)
			if errSeek != nil {
				return "", nil, false
			}
		}

	}
	return "", nil, false
}

// Info prints out SSTable data.
func (s *SSTable) Info() {
	fmt.Println(s.DirectoryPath)
	fmt.Println(s.DataPath)
	fmt.Println(s.IndexPath)
	fmt.Println(s.SummaryPath)
	fmt.Println(s.FilterPath)
	fmt.Println(s.MerklePath)
}

// ReadRecord reads one record from the given file. Used in compaction.
func ReadRecord(fileData *os.File) (string, []byte, string, int64, int) {
	crcBytes := make([]byte, CrcSize)
	n, _ := fileData.Read(crcBytes)

	timestampBytes := make([]byte, TimestampSize)
	_, _ = fileData.Read(timestampBytes)

	tombstoneBytes := make([]byte, TombstoneSize)
	_, _ = fileData.Read(tombstoneBytes)

	keySizeBytesData := make([]byte, KeySizeSize)
	_, _ = fileData.Read(keySizeBytesData)
	keySizeData := binary.LittleEndian.Uint32(keySizeBytesData)

	valueSizeBytes := make([]byte, ValueSizeSize)
	_, _ = fileData.Read(valueSizeBytes)
	valueSize := binary.LittleEndian.Uint32(valueSizeBytes)

	keyBytesData := make([]byte, keySizeData)
	_, _ = fileData.Read(keyBytesData)

	valueBytes := make([]byte, valueSize)
	_, _ = fileData.Read(valueBytes)

	timestamp := binary.LittleEndian.Uint64(timestampBytes)
	return string(keyBytesData), valueBytes, string(tombstoneBytes), int64(timestamp), n
}

// WriteRecord writes one record to the given file. Used in compaction.
func WriteRecord(fileData *os.File, fileIndex *os.File, fileSummary *os.File, key string, value []byte, offset int,
	offsetSummary int, timestamp int64) (int, int) {
	keyBytes := []byte(key)
	crc := CRC32(append(keyBytes, value[1:]...))
	crcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBytes, crc)

	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(timestamp))

	tombstoneBytes := value[:1]

	keySize := len(keyBytes)
	keySizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(keySizeBytes, uint32(keySize))

	valueSize := len(value) - 1
	valueSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(valueSizeBytes, uint32(valueSize))

	offsetBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))

	offsetSummaryBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(offsetSummaryBytes, uint64(offsetSummary))

	var crcFix [CrcSize]byte
	var timestampFix [TimestampSize]byte
	var tombstoneFix [TombstoneSize]byte
	var keySizeFix [KeySizeSize]byte
	var valueSizeFix [ValueSizeSize]byte
	var offsetFix [OffsetSize]byte
	var offsetSummaryFix [OffsetSize]byte

	copy(crcFix[(CrcSize-len(crcBytes)):], crcBytes)
	copy(timestampFix[(TimestampSize-len(timestampBytes)):], timestampBytes)
	copy(tombstoneFix[(TombstoneSize-len(tombstoneBytes)):], tombstoneBytes)
	copy(keySizeFix[(KeySizeSize-len(keySizeBytes)):], keySizeBytes)
	copy(valueSizeFix[(ValueSizeSize-len(valueSizeBytes)):], valueSizeBytes)
	copy(offsetFix[(OffsetSize-len(offsetBytes)):], offsetBytes)
	copy(offsetSummaryFix[(OffsetSize-len(offsetSummaryBytes)):], offsetSummaryBytes)

	recordData := append(crcFix[:], timestampFix[:]...)
	recordData = append(recordData, tombstoneFix[:]...)
	recordData = append(recordData, keySizeFix[:]...)
	recordData = append(recordData, valueSizeFix[:]...)
	recordData = append(recordData, keyBytes...)
	recordData = append(recordData, value[1:]...)

	_, errWriteData := fileData.Write(recordData)
	if errWriteData != nil {
		log.Fatal(errWriteData)
	}

	indexRecord := append(keySizeFix[:], keyBytes...)
	summaryRecord := indexRecord
	indexRecord = append(indexRecord, offsetFix[:]...)

	_, errWriteIndex := fileIndex.Write(indexRecord)
	if errWriteIndex != nil {
		log.Fatal(errWriteIndex)
	}

	offset += len(recordData)

	summaryRecord = append(summaryRecord, offsetSummaryFix[:]...)

	_, errWriteSummary1 := fileSummary.Write(summaryRecord)
	if errWriteSummary1 != nil {
		log.Fatal(errWriteSummary1)
	}

	offsetSummary += len(indexRecord)

	return offset, offsetSummary
}

// ReturnUpperBound Based on 2 summaries, returns the optimal upper bound.
func ReturnUpperBound(summPath1, summPath2 string) []byte {
	fileSummary1, errSummary1 := os.OpenFile(summPath1, os.O_RDONLY, 0666)
	if errSummary1 != nil {
		log.Fatal(errSummary1)
	}
	fileSummary2, errSummary1 := os.OpenFile(summPath2, os.O_RDONLY, 0666)
	if errSummary1 != nil {
		log.Fatal(errSummary1)
	}

	lowerBoundSizeBytes1 := make([]byte, KeySizeSize)
	_, _ = fileSummary1.Read(lowerBoundSizeBytes1)

	lowerBoundSize1 := binary.LittleEndian.Uint32(lowerBoundSizeBytes1)
	lowerBoundBytes1 := make([]byte, lowerBoundSize1)
	_, _ = fileSummary1.Read(lowerBoundBytes1)

	upperBoundSizeBytes1 := make([]byte, KeySizeSize)
	_, _ = fileSummary1.Read(upperBoundSizeBytes1)

	upperBoundSize1 := binary.LittleEndian.Uint32(upperBoundSizeBytes1)
	upperBoundBytes1 := make([]byte, upperBoundSize1)
	_, _ = fileSummary1.Read(upperBoundBytes1)
	upperBound1 := string(upperBoundBytes1)

	lowerBoundSizeBytes2 := make([]byte, KeySizeSize)
	_, _ = fileSummary2.Read(lowerBoundSizeBytes2)

	lowerBoundSize2 := binary.LittleEndian.Uint32(lowerBoundSizeBytes2)
	lowerBoundBytes2 := make([]byte, lowerBoundSize2)
	_, _ = fileSummary2.Read(lowerBoundBytes2)

	upperBoundSizeBytes2 := make([]byte, KeySizeSize)
	_, _ = fileSummary2.Read(upperBoundSizeBytes2)

	upperBoundSize2 := binary.LittleEndian.Uint32(upperBoundSizeBytes2)
	upperBoundBytes2 := make([]byte, upperBoundSize2)
	_, _ = fileSummary2.Read(upperBoundBytes2)
	upperBound2 := string(upperBoundBytes2)

	err1 := fileSummary1.Close()
	if err1 != nil {
		return nil
	}
	err2 := fileSummary2.Close()
	if err2 != nil {
		return nil
	}

	if upperBound1 > upperBound2 {
		upperBoundSizeBytes1 = append(upperBoundSizeBytes1, upperBoundBytes1...)
		return upperBoundSizeBytes1
	}
	upperBoundSizeBytes2 = append(upperBoundSizeBytes2, upperBoundBytes2...)
	return upperBoundSizeBytes2
}

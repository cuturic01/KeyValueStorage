package Structures

// Author: SV38/2020

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
)

type CountMinSketch struct {
	d      uint32
	w      uint32
	table  [][]uint64
	hashes []hash.Hash32
}

// NewCMS Initializing a CMS with d as number of hash functions and w as number of columns
func NewCMS(d uint32, w uint32) (s *CountMinSketch, err error) {
	if d <= 0 || w <= 0 {
		return nil, errors.New("countminsketch: values of d and w should both be greater than 0")
	}

	s = &CountMinSketch{
		d:      d,
		w:      w,
		hashes: CreateCmsHashFunctions(d),
	}
	s.table = make([][]uint64, d)
	for r := uint32(0); r < d; r++ {
		s.table[r] = make([]uint64, w)
	}

	return s, nil
}

// NewCMSWithEstimates  Calculating d and w based on ε and δ where
//							- error in answering a query is within a factor of ε
//							- with probability δ.
func NewCMSWithEstimates(epsilon, delta float64) (*CountMinSketch, error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("countminsketch: value of epsilon should be in range of (0, 1)")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("countminsketch: value of delta should be in range of (0, 1)")
	}

	w := uint32(math.Ceil(math.E / epsilon))
	d := uint32(math.Ceil(math.Log(math.E / delta)))
	// fmt.Printf("ε: %f, δ: %f -> d: %d, w: %d\n", epsilon, delta, d, w)
	return NewCMS(d, w)
}

// D returns the number of hashing functions.
func (s *CountMinSketch) D() uint32 {
	return s.d
}

// W returns the size of hashing functions.
func (s *CountMinSketch) W() uint32 {
	return s.w
}

// CreateCmsHashFunctions Generates d hash functions.
func CreateCmsHashFunctions(d uint32) []hash.Hash32 {
	var h []hash.Hash32
	for i := uint32(0); i < d; i++ {
		h = append(h, murmur3.New32WithSeed(i+1))
	}
	return h
}

// locations Generates Locations for a given key (this function should not be visible outside this module).
func (s *CountMinSketch) locations(key []byte) (locations []uint32) {
	locations = make([]uint32, s.d)
	for i, hashFunction := range s.hashes {
		_, err := hashFunction.Write(key)
		if err != nil {
			return nil
		}
		hashValue := hashFunction.Sum32()
		column := hashValue % s.w
		locations[i] = column
		hashFunction.Reset()
	}
	return
}

// Update updates a table in CMS for given key.
func (s *CountMinSketch) Update(key string) {
	for row, column := range s.locations([]byte(key)) {
		s.table[row][column] += 1
	}
}

// Estimate estimates the frequency of a key in our CMS.
func (s *CountMinSketch) Estimate(key string) uint64 {
	var min uint64
	for row, column := range s.locations([]byte(key)) {
		if row == 0 || s.table[row][column] < min {
			min = s.table[row][column]
		}
	}
	return min
}

// SerializeCMS Serializes CMS structure to the given file.
func (s *CountMinSketch) SerializeCMS(fileName string) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes[:4], s.d)
	binary.LittleEndian.PutUint32(bytes[4:8], s.w)
	for _, slc := range s.table {
		for _, num := range slc {
			bytesEl := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytesEl, num)
			bytes = append(bytes, bytesEl...)
		}
	}
	_, err = file.Write(bytes)
	if err != nil {
		return
	}
	err = file.Close()
	if err != nil {
		return
	}
}

// DeserializeCMS Deserializes CMS structure from the given file.
func DeserializeCMS(fileName string) *CountMinSketch {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	d := binary.LittleEndian.Uint32(bytes[:4])
	w := binary.LittleEndian.Uint32(bytes[4:8])
	cms, _ := NewCMS(d, w)
	curr := 8
	for i := uint32(0); i < d; i++ {
		for j := uint32(0); j < w; j++ {
			cms.table[i][j] = binary.LittleEndian.Uint64(bytes[curr : curr+8])
			curr += 8
		}
	}

	return cms
}

// InfoCMS Prints out information about the structure.
func (s *CountMinSketch) InfoCMS() {
	fmt.Println("d:", s.d)
	fmt.Println("w", s.w)
	for _, slc := range s.table {
		fmt.Println(slc)
	}
}

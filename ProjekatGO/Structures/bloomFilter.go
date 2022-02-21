package Structures

// Author: SV11/2020

import (
	"encoding/binary"
	"fmt"
	"github.com/Workiva/go-datastructures/bitarray"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
)

// CalculateM calculates m based of expected elements and false-positive rate.
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2),
		float64(2))))
}

// CalculateK calculates k based of expected elements and m.
func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

// CreateBloomHashFunctions creates k different hash functions.
func CreateBloomHashFunctions(k uint) []hash.Hash32 {
	var h []hash.Hash32
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(i+1)))
	}
	return h
}

type BloomFilter struct {
	m   uint32
	set bitarray.BitArray
	k   uint32
	n   uint32
	p   float64
}

// NewBloomFilter returns a new BloomFilter.
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	set := bitarray.NewBitArray(uint64(m))
	return &BloomFilter{set: set, k: uint32(k), m: uint32(m), n: uint32(uint(expectedElements)), p: falsePositiveRate}
}

// N returns n
func (bf *BloomFilter) N() uint32 {
	return bf.n
}

// Info prints out BloomFilter data.
func (bf *BloomFilter) Info() {
	fmt.Println("m:", bf.m)
	fmt.Println("k:", bf.k)
	fmt.Println("n:", bf.n)
	fmt.Println("p:", bf.p)
	fmt.Println("set:", bf.set)
}

// Add adds a new item to BloomFilter.
func (bf *BloomFilter) Add(item string) {
	k := bf.k
	m := bf.m
	set := bf.set
	h := CreateBloomHashFunctions(uint(k))
	for _, hi := range h {
		_, err := hi.Write([]byte(item))
		if err != nil {
			return
		}
		index := int(hi.Sum32()) % int(m)
		err = set.SetBit(uint64(index))
		if err != nil {
			return
		}
	}
}

// Check checks for item existence in BloomFilter.
func (bf *BloomFilter) Check(item string) bool {
	k := bf.k
	set := bf.set
	m := bf.m
	h := CreateBloomHashFunctions(uint(k))
	for _, hi := range h {
		_, err := hi.Write([]byte(item))
		if err != nil {
			return false
		}
		index := int(hi.Sum32()) % int(m)
		bit, err := set.GetBit(uint64(index))
		if err != nil {
			return false
		}
		if !bit {
			return false
		}
	}
	return true
}

// Serialize serializes BloomFilter to the given file.
func (bf *BloomFilter) Serialize(fileName string) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bytes := make([]byte, 20)
	binary.LittleEndian.PutUint32(bytes[:4], bf.m)
	binary.LittleEndian.PutUint32(bytes[4:8], bf.k)
	binary.LittleEndian.PutUint32(bytes[8:12], bf.n)
	binary.LittleEndian.PutUint64(bytes[12:20], math.Float64bits(bf.p))
	setBytes, err := bitarray.Marshal(bf.set)
	if err != nil {
		fmt.Println(err)
		return
	}
	bytes = append(bytes, setBytes...)
	_, err = file.Write(bytes)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = file.Close()
	if err != nil {
		return
	}
}

// DeserializeFilter creates a new BloomFilter from the given file.
func DeserializeFilter(fileName string) *BloomFilter {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	m := binary.LittleEndian.Uint32(bytes[:4])
	k := binary.LittleEndian.Uint32(bytes[4:8])
	n := binary.LittleEndian.Uint32(bytes[8:12])
	p := math.Float64frombits(binary.LittleEndian.Uint64(bytes[12:20]))
	setByte := bytes[20:]
	set, err1 := bitarray.Unmarshal(setByte)
	if err1 != nil {
		fmt.Println(err1)
		return nil
	}

	return &BloomFilter{m: m, set: set, k: k, n: n, p: p}
}

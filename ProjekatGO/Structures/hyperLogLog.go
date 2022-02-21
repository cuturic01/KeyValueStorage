package Structures

// Author: SV38/2020

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// Estimate estimates the number of different values.
func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return math.Ceil(estimation)
}

type HyperLogLog struct {
	m   uint64
	p   uint32
	reg []uint8
}

// NewHyperLogLog returns a new HyperLogLog.
func NewHyperLogLog(precision uint32) *HyperLogLog {
	p := precision
	m := math.Pow(2, float64(p))
	reg := make([]uint8, int(m))
	return &HyperLogLog{m: uint64(m), p: p, reg: reg}
}

// Add adds item into HyperLogLog.
func (hll *HyperLogLog) Add(item []byte) {
	hash := fnv.New32()
	_, err := hash.Write(item)
	if err != nil {
		return
	}
	num := hash.Sum32()

	k := 32 - hll.p
	value := bits.TrailingZeros32(num)
	index := num >> k
	if uint8(value) > hll.reg[index] {
		hll.reg[index] = uint8(value)
	}
}

// Info prints out HyperLogLog data.
func (hll *HyperLogLog) Info() {
	fmt.Println("m:", hll.m)
	fmt.Println("p:", hll.p)
	fmt.Println("reg:", hll.reg)
}

// Serialize serializes HyperLogLog to the given file.
func (hll *HyperLogLog) Serialize(fileName string) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bytes := make([]byte, 12)
	binary.LittleEndian.PutUint64(bytes[:8], hll.m)
	binary.LittleEndian.PutUint32(bytes[8:12], hll.p)
	bytes = append(bytes, hll.reg...)

	_, err = file.Write(bytes)
	if err != nil {
		return
	}

	err = file.Close()
	if err != nil {
		return
	}
}

// DeserializeHLL creates HyperLogLog from the given file.
func DeserializeHLL(fileName string) *HyperLogLog {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	m := binary.LittleEndian.Uint64(bytes[:8])
	p := binary.LittleEndian.Uint32(bytes[8:12])
	reg := bytes[12 : 12+m]
	return &HyperLogLog{m: m, p: p, reg: reg}
}

package Structures

// Author: SV38/2020

import (
	"time"
)

type Bucket struct {
	capacity  int
	tokens    int
	timeRate  int
	timeStamp int64
}

// Tokens returns current available tokens.
func (b *Bucket) Tokens() int {
	return b.tokens
}

// Remove removes one token from Bucket.
func (b *Bucket) Remove() {
	b.tokens--
}

// fill fills Bucket and changes timeStamp.
func (b *Bucket) fill() {
	b.timeStamp = time.Now().Unix()
	b.tokens = b.capacity
}

// Check checks if Bucket needs to be filled, and checks if there are available tokens.
func (b *Bucket) Check() bool {
	if time.Now().Unix()-b.timeStamp >= int64(b.timeRate) {
		b.fill()
		return true
	} else {
		if b.tokens > 0 {
			return true
		}
		return false
	}
}

// NewBucket returns a new Bucket.
func NewBucket(config *Config) *Bucket {
	b := &Bucket{}
	b.capacity = int(config.Threshold)
	b.tokens = b.capacity
	b.timeRate = config.TimeRate
	b.timeStamp = time.Now().Unix()
	return b
}

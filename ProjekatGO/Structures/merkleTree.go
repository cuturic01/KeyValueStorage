package Structures

// Author: SV14/2020

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

//NOTE: check the serialization of the tree

type Content interface {
	CalculateHash() ([]byte, error)
}

type MyContent struct {
	key   string
	value []byte
}

func (mc MyContent) CalculateHash() ([]byte, error) {
	keyBytes := []byte(mc.key)
	keyBytes = append(keyBytes, mc.value...)
	hashVal := Hash(keyBytes)
	return hashVal[:], nil
}

type Node struct {
	data   []byte
	parent *Node
	left   *Node
	right  *Node
	leaf   bool
}

type MerkleTree struct {
	root         *Node
	leafs        []*Node
	hashFunction func(data []byte) [20]byte
}

// NewTree returns new MerkleTree based on given content
func NewTree(cs []Content) (*MerkleTree, error) {
	t := &MerkleTree{
		hashFunction: Hash,
	}
	root, leafs, err := buildWithContent(cs, t)
	if err != nil {
		return nil, err
	}
	t.root = root
	t.leafs = leafs
	return t, nil
}

// buildWithContent builds leafs and calls other function to build intermediate nodes
func buildWithContent(cs []Content, t *MerkleTree) (*Node, []*Node, error) {
	if len(cs) == 0 {
		return nil, nil, errors.New("error: cannot construct tree with no content")
	}
	var leafs []*Node
	for _, c := range cs {
		hash, err := c.CalculateHash()
		if err != nil {
			return nil, nil, err
		}

		leafs = append(leafs, &Node{
			data: hash,
			leaf: true,
		})
	}
	if len(leafs)%2 == 1 {
		duplicate := &Node{
			data: leafs[len(leafs)-1].data,
			leaf: true,
		}
		leafs = append(leafs, duplicate)
	}
	root, err := buildIntermediate(leafs, t)
	if err != nil {
		return nil, nil, err
	}

	return root, leafs, nil
}

// buildIntermediate builds intermediate nodes and a root
func buildIntermediate(nl []*Node, t *MerkleTree) (*Node, error) {
	var nodes []*Node
	for i := 0; i < len(nl); i += 2 {
		var left, right int = i, i + 1
		if i+1 == len(nl) {
			right = i
		}
		chash := append(nl[left].data, nl[right].data...)
		data20byte := t.hashFunction(chash)
		rightNode := nl[right]
		if left == right {
			rightNode = nil
		}
		n := &Node{
			left:  nl[left],
			right: rightNode,
			data:  data20byte[:],
		}
		nodes = append(nodes, n)
		nl[left].parent = n
		nl[right].parent = n
		if len(nl) == 2 {
			return n, nil
		}
	}
	return buildIntermediate(nodes, t)
}

// SerializeTree serializes a tree with the given root node and file where we want to serialize it
func SerializeTree(root *Node, file *os.File, marker int) {
	if root == nil {
		_ = binary.Write(file, binary.LittleEndian, marker)
		return
	}
	_ = binary.Write(file, binary.LittleEndian, root.data)
	SerializeTree(root.left, file, marker)
	SerializeTree(root.right, file, marker)
}

// PrintTree used for testing if the tree was well-made
func (t *MerkleTree) PrintTree() {
	stringTraversal(t.root)
}
func (t *MerkleTree) PrintLeafs() {
	fmt.Println(len(t.leafs))
}

func stringTraversal(root *Node) {
	queue := make([]*Node, 0)
	queue = append(queue, root)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		fmt.Println(node.leaf)
		if node.left != nil {
			queue = append(queue, node.left)
		}
		if node.right != nil {
			queue = append(queue, node.right)
		}
	}
}

// Hash HashFunction that returns hashed value in bytes
func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

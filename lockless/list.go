// Package lockless package contains a lockless linked list implementation
package lockless

import "sync/atomic"

type Timestamp struct {
	Tx  bool
	Ts  uint64
	Inf bool
}

type Head[T any] struct {
	Next *Node[T]
}

type Node[T any] struct {
	BeginTs Timestamp
	EndTs   Timestamp
	Value   T

	Next     *Node[T]
	nextLock uint32
}

func NewList[T any]() *Head[T] {
	return &Head[T]{}
}

func (node *Node[T]) Append(next *Node[T]) bool {
	if atomic.CompareAndSwapUint32(&node.nextLock, 0, 1) {
		node.Next = next
		return true
	}
	return false
}

func NewNode[T any]() *Node[T] {
	return &Node[T]{}
}

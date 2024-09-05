package eight

import (
	"bytes"
	"iter"
)

const (
	numBuckets        = 1 << 17 // Number of hash buckets (must be a power of 2)
	initialBucketSize = numBuckets / 2
	offset64          = 14695981039346656037
	prime64           = 1099511628211
)

type stats struct {
	min, max, count int32
	sum             int64
}

type item struct {
	key  []byte
	stat *stats
}

type hashTable struct {
	items []item
	size  int
}

// newHashTable initializes a new hash table with a fixed number of buckets.
func newHashTable() *hashTable {
	return &hashTable{
		items: make([]item, numBuckets),
		size:  0,
	}
}

// hashFnv1a computes the FNV-1a hash for a given byte slice.
func hashFnv1a(key []byte) uint64 {
	hash := uint64(offset64)
	for _, c := range key {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

func (ht *hashTable) get(station []byte) *stats {
	hash := hashFnv1a(station)
	hashIndex := int(hash & uint64(numBuckets-1))

	for {
		if ht.items[hashIndex].key == nil {
			return nil
		}

		if bytes.Equal(ht.items[hashIndex].key, station) {
			return ht.items[hashIndex].stat
		}

		// Linear probing: try the next slot.
		hashIndex++
		if hashIndex >= numBuckets {
			hashIndex = 0
		}
	}
}

// insertOrUpdate inserts a new item into the hash table or updates the stats if the key already exists.
func (ht *hashTable) insertOrUpdate(station []byte, temp int32) {
	hash := hashFnv1a(station)
	hashIndex := int(hash & uint64(numBuckets-1))

	for {
		if ht.items[hashIndex].key == nil {
			// Found an empty slot, add a new item (copying the key).
			keyCopy := make([]byte, len(station))
			copy(keyCopy, station)
			ht.items[hashIndex] = item{
				key: keyCopy,
				stat: &stats{
					min:   temp,
					max:   temp,
					sum:   int64(temp),
					count: 1,
				},
			}
			ht.size++
			if ht.size > initialBucketSize {
				panic("too many items in the hash table")
			}
			break
		}

		if bytes.Equal(ht.items[hashIndex].key, station) {
			// Matching key found, update stats.
			s := ht.items[hashIndex].stat
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += int64(temp)
			s.count++
			break
		}

		// Linear probing: try the next slot.
		hashIndex++
		if hashIndex >= numBuckets {
			hashIndex = 0
		}
	}
}

func (ht *hashTable) Keys() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, item := range ht.items {
			if item.key != nil {
				if !yield(item.key) {
					break
				}
			}
		}
	}
}

func (ht *hashTable) All() iter.Seq2[[]byte, *stats] {
	return func(yield func([]byte, *stats) bool) {
		for _, item := range ht.items {
			if item.key != nil {
				if !yield(item.key, item.stat) {
					break
				}
			}
		}
	}
}

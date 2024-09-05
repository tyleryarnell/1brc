package lphash

import (
	"bytes"
)

// Stats represents temperature statistics for a station.
type Stats struct {
	Min, Max, Sum int32
	Count         int64
}

// Table represents a hash table using linear probing for collision resolution.
type Table struct {
	items []hashItem
	size  int
}

type hashItem struct {
	Key   []byte
	Stats *Stats
}

const numBuckets = 1 << 17

// NewTable creates a new hash table with linear probing
func NewTable() *Table {
	return &Table{
		items: make([]hashItem, numBuckets),
		size:  0,
	}
}

func hashFnv1a(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, c := range key {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

// InsertOrUpdate adds a new station or updates its stats
func (ht *Table) InsertOrUpdate(station []byte, temp int32) {
	hash := hashFnv1a(station)
	hashIndex := int(hash & uint64(numBuckets-1))

	for {
		if ht.items[hashIndex].Key == nil {
			key := make([]byte, len(station))
			copy(key, station)
			ht.items[hashIndex] = hashItem{
				Key: key,
				Stats: &Stats{
					Min:   temp,
					Max:   temp,
					Sum:   temp,
					Count: 1,
				},
			}
			ht.size++
			break
		}
		if bytes.Equal(ht.items[hashIndex].Key, station) {
			s := ht.items[hashIndex].Stats
			if temp < s.Min {
				s.Min = temp
			}
			if temp > s.Max {
				s.Max = temp
			}
			s.Sum += temp
			s.Count++
			break
		}
		hashIndex++
		if hashIndex >= numBuckets {
			hashIndex = 0
		}
	}
}

// Iterate returns all non-empty hash table entries
func (ht *Table) Iterate() []struct {
	Key   string
	Stats *Stats
} {
	entries := make([]struct {
		Key   string
		Stats *Stats
	}, 0, ht.size)
	for _, item := range ht.items {
		if item.Key != nil {
			entries = append(entries, struct {
				Key   string
				Stats *Stats
			}{
				Key:   string(item.Key),
				Stats: item.Stats,
			})
		}
	}
	return entries
}

// parseLine parses the station and temperature from a line
func parseLine(line []byte) (station string, value int32) {
	idx := bytes.IndexByte(line, ';')
	if idx == -1 {
		return "", 0 // Handle invalid line
	}
	station = string(line[:idx])

	// Convert the value (second part) directly from []byte to int32
	// Value corresponds to the second part of the line (after the ';')
	value = 0
	var decimalFactor int32 = 1
	for i := idx + 1; i < len(line); i++ {
		if line[i] == '-' {
			value = -value
		} else if line[i] == '.' {
			decimalFactor = 10
		} else {
			value = value*10 + int32(line[i]-'0')
			if decimalFactor > 1 {
				decimalFactor *= 10
			}
		}
	}

	return station, value
}

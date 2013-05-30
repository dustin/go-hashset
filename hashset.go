// Package hashset implements a "set" type store for hashes.
package hashset

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type hashSorter struct {
	buf, hashes []byte
	size        int
}

func (p *hashSorter) Len() int {
	return len(p.hashes) / p.size
}

func (p *hashSorter) Less(i, j int) bool {
	return bytes.Compare(p.hashes[i:i+p.size], p.hashes[j:j+p.size]) < 0
}

func (p *hashSorter) Swap(i, j int) {
	copy(p.buf, p.hashes[i:i+p.size])
	copy(p.hashes[i:i+p.size], p.hashes[j:j+p.size])
	copy(p.hashes[j:j+p.size], p.buf)
}

// Store the hashes.
type Hashset struct {
	things  [65536][]byte
	sortbuf []byte
	size    int
}

// Add a hash to the Hashset.Add
//
// This is the []byte representation of a hash.  You *can* hex encode
// it, but you probably shouldn't.
func (hs *Hashset) Add(h []byte) {
	if hs.Contains(h) {
		return
	}

	if hs.size == 0 {
		hs.size = len(h)
		hs.sortbuf = make([]byte, hs.size)
	} else if hs.size != len(h) {
		panic("inconsistent size")
	}

	n := int(binary.BigEndian.Uint16(h))

	hs.things[n] = append(hs.things[n], h[2:]...)
	sorter := hashSorter{hs.sortbuf, hs.things[n], hs.size - 2}
	sort.Sort(&sorter)
}

// Return true if the given hash is in this Hashset.
func (hs *Hashset) Contains(h []byte) bool {
	n := int(binary.BigEndian.Uint16(h))
	bin := hs.things[n]
	sub := h[2:]
	pos := sort.Search(len(bin)/(hs.size-2), func(i int) bool {
		return bytes.Compare(bin[i:i+hs.size-2], sub) <= 0
	})
	return bytes.Equal(sub, bin[pos:])
}

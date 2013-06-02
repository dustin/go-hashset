// Package hashset implements a "set" type store for hashes.
package hashset

import (
	"bytes"
	"encoding/binary"
	"io"
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
	ioff := i * p.size
	joff := j * p.size
	return bytes.Compare(p.hashes[ioff:ioff+p.size], p.hashes[joff:joff+p.size]) < 0
}

func (p *hashSorter) Swap(i, j int) {
	ioff := i * p.size
	joff := j * p.size
	copy(p.buf, p.hashes[ioff:ioff+p.size])
	copy(p.hashes[ioff:ioff+p.size], p.hashes[joff:joff+p.size])
	copy(p.hashes[joff:joff+p.size], p.buf)
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
	if len(bin) == 0 {
		return false
	}
	sub := h[2:]
	l := hs.size - 2
	pos := sort.Search(len(bin)/l, func(i int) bool {
		off := i * l
		return bytes.Compare(bin[off:off+l], sub) >= 0
	})
	off := pos * l
	return off < len(bin) && bytes.Equal(sub, bin[off:off+l])
}

// How many things we've got.
func (hs *Hashset) Len() int {
	rv := 0
	for _, a := range hs.things {
		rv += (len(a) / (hs.size - 2))
	}
	return rv
}

// Return a channel that emits all stored hashes.
//
// As this returns a channel, the caller is expected to drain the
// channel completely.
func (hs *Hashset) Iter() <-chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		l := hs.size - 2
		for pre, p := range hs.things {
			for i := 0; i < len(p)/l; i++ {
				off := i * l
				rv := make([]byte, hs.size)
				binary.BigEndian.PutUint16(rv, uint16(pre))
				copy(rv[2:], p[off:])
				ch <- rv
			}
		}
	}()
	return ch
}

// Persist this hashset to the given writer.
//
// Returns the number of bytes written and an error if any.
func (hs *Hashset) Write(w io.Writer) (int64, error) {
	var written int64
	l := hs.size - 2
	buf := make([]byte, hs.size)
	for pre, p := range hs.things {
		for i := 0; i < len(p)/l; i++ {
			off := i * l
			binary.BigEndian.PutUint16(buf, uint16(pre))
			copy(buf[2:], p[off:])
			n, e := w.Write(buf)
			written += int64(n)
			if e != nil {
				return written, e
			}
		}
	}
	return written, nil
}

// Load hashes from a reader.
//
// The size of the hash must be known ahead of time.
func Load(size int, r io.Reader) (*Hashset, error) {
	hs := &Hashset{}
	for {
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF {
				err = nil
			}
			return hs, err
		}
		hs.Add(buf)
	}
}

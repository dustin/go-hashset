// Package hashset implements a "set" type store for hashes.
package hashset

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
)

// Hashset stores a set of fixed size hash values.
type Hashset struct {
	things  [65536][]byte
	sorted  [65536]bool
	sortbuf []byte
	size    int
}

// Add a hash to the Hashset.
//
// This is the []byte representation of a hash.  You *can* hex encode
// it, but you probably shouldn't.
func (hs *Hashset) Add(h []byte) {
	if hs.Contains(h) {
		return
	}

	hs.UnsafeAdd(h)
}

// UnsafeAdd adds a hash to the Hashset without confirming it's there
// already.  Note that this isn't unsafe in the sense that presence
// checking will fail, but if you give it duplicates, they will be
// emitted on iteration and it will obviously take more RAM.
//
// This is the []byte representation of a hash.  You *can* hex encode
// it, but you probably shouldn't.
func (hs *Hashset) UnsafeAdd(h []byte) {
	if hs.size == 0 {
		hs.size = len(h)
		hs.sortbuf = make([]byte, hs.size)
	} else if hs.size != len(h) {
		panic("inconsistent size")
	}

	n := int(binary.BigEndian.Uint16(h))

	hs.things[n] = append(hs.things[n], h[2:]...)
	hs.sorted[n] = false
}

func (hs *Hashset) ensureSorted(bin int) {
	if !hs.sorted[bin] {
		sorter := hashSorter{hs.sortbuf, hs.things[bin], hs.size - 2}
		sorter.Sort()
		hs.sorted[bin] = true
	}
}

const sortThreshold = 100

// Contains returns true if the given hash is in this Hashset.
func (hs *Hashset) Contains(h []byte) bool {
	n := int(binary.BigEndian.Uint16(h))
	bin := hs.things[n]
	if len(bin) == 0 {
		return false
	}
	sub := h[2:]
	l := hs.size - 2

	if len(bin)/l > sortThreshold {
		hs.ensureSorted(n)
		pos := sort.Search(len(bin)/l, func(i int) bool {
			off := i * l
			return bytes.Compare(bin[off:off+l], sub) >= 0
		})
		off := pos * l
		return off < len(bin) && bytes.Equal(sub, bin[off:off+l])
	}

	for i := 0; i < len(bin)/l; i++ {
		off := i * l
		if bytes.Equal(sub, bin[off:off+l]) {
			return true
		}
	}

	return false
}

// Len returns the number of hashes contained in this Hashset.
func (hs *Hashset) Len() int {
	rv := 0
	for _, a := range hs.things {
		rv += (len(a) / (hs.size - 2))
	}
	return rv
}

// Iter returns a channel that emits all stored hashes.
//
// As this returns a channel, the caller is expected to drain the
// channel completely.
func (hs *Hashset) Iter() <-chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		l := hs.size - 2
		for pre, p := range hs.things {
			hs.ensureSorted(pre)
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
		hs.ensureSorted(pre)
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

// Load hashes from a reader.  This is meant to load hashes that were
// dumped by Write, so it makes some assumptions that will not be
// valid in other cases.  In particular, if the input has duplicates,
// so will the Hashset.
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
		hs.UnsafeAdd(buf)
	}
}

// AddAll updates this Hashset by adding all hashes from another
// Hashset.
//
// Both hashsets are required to be representing the same size hashes.
func (hs *Hashset) AddAll(o *Hashset) {
	if o.size == 0 {
		return
	}
	if hs.size == 0 {
		hs.size = o.size
	}
	if hs.size != o.size {
		panic("hash size mismatch")
	}
	l := hs.size - 2
	for pre, bin := range hs.things {
		thisSize := len(bin) / l
		hs.ensureSorted(pre)
		for i := 0; i < len(o.things[pre])/l; i++ {
			off := i * l
			sub := o.things[pre][off : off+l]
			pos := sort.Search(thisSize, func(p int) bool {
				o := p * l
				return bytes.Compare(bin[o:o+l], sub) >= 0
			})
			off = pos * l
			if !(off < (thisSize*l) && bytes.Equal(sub, bin[off:off+l])) {
				hs.things[pre] = append(hs.things[pre], sub...)
			}
		}
		sorter := hashSorter{hs.sortbuf, hs.things[pre], l}
		sorter.Sort()
	}
}

// Intersection computes the intersection of a bunch of Hashsets.
//
// The returned Hashset contains all of the hashes that exist in every
// input Hashset.
func Intersection(base *Hashset, sets ...*Hashset) *Hashset {
	rv := &Hashset{size: base.size, sortbuf: make([]byte, base.size)}

	l := base.size - 2
	for pre, bin := range base.things {
		base.ensureSorted(pre)
		for i := 0; i < len(bin)/l; i++ {
			found := true
			off := i * l
			sub := bin[off : off+l]
			for _, hs := range sets {
				hs.ensureSorted(pre)
				hbin := hs.things[pre]
				thisSize := len(hbin) / l
				pos := sort.Search(thisSize, func(p int) bool {
					o := p * l
					return bytes.Compare(hbin[o:o+l], sub) >= 0
				})
				off = pos * l
				if !(off < (thisSize*l) &&
					bytes.Equal(sub, hbin[off:off+l])) {
					found = false
					break
				}
			}
			if found {
				rv.things[pre] = append(rv.things[pre], sub...)
			}
		}
	}

	return rv
}

// Copy returns a new Hashset that's a deep copy of this Hashset.
func (hs *Hashset) Copy() *Hashset {
	rv := &Hashset{sortbuf: make([]byte, hs.size), size: hs.size}
	for i, p := range hs.things {
		rv.sorted[i] = hs.sorted[i]
		rv.things[i] = make([]byte, len(p))
		copy(rv.things[i], p)
	}
	return rv
}

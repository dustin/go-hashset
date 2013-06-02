package hashset

import (
	"bytes"
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
	return bytes.Compare(p.hashes[ioff:ioff+p.size],
		p.hashes[joff:joff+p.size]) < 0
}

func (p *hashSorter) Swap(i, j int) {
	ioff := i * p.size
	joff := j * p.size
	copy(p.buf, p.hashes[ioff:ioff+p.size])
	copy(p.hashes[ioff:ioff+p.size], p.hashes[joff:joff+p.size])
	copy(p.hashes[joff:joff+p.size], p.buf)
}

func (p *hashSorter) Sort() {
	sort.Sort(p)
}

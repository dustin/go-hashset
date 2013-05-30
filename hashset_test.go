package hashset

import (
	"encoding/hex"
	"io"
	"math/rand"
	"testing"
)

var samples [][]byte

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}
}

func TestSet(t *testing.T) {
	samples := [][]byte{}

	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}

	// Enough hashes to surely hit all the cases
	buf := make([]byte, 20)
	for i := 0; i < 65535*5; i++ {
		_, err := io.ReadFull(randomSrc, buf)
		if err != nil {
			panic(err)
		}
		samples = append(samples, buf)
	}

	hs := Hashset{}
	for _, h := range samples {
		if hs.Contains(h) {
			t.Errorf("Expected not to have example %s",
				hex.EncodeToString(h))
		}
	}

	for _, h := range samples {
		hs.Add(h)
		if !hs.Contains(h) {
			t.Errorf("Expected to have example %s",
				hex.EncodeToString(h))
		}
	}
	for _, h := range samples {
		hs.Add(h)
		if !hs.Contains(h) {
			t.Errorf("Expected to have example %s (pass 2)",
				hex.EncodeToString(h))
		}
	}

}

func BenchmarkSet(b *testing.B) {
	hs := Hashset{}
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	b.ResetTimer()

	h := make([]byte, 20)
	for i := 0; i < b.N; i++ {
		_, err := io.ReadFull(randomSrc, h)
		if err != nil {
			panic(err)
		}
		hs.Add(h)
	}
}

func BenchmarkFind(b *testing.B) {
	hs := Hashset{}
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}

	h := make([]byte, 20)
	for i := 0; i < 1e6; i++ {
		_, err := io.ReadFull(randomSrc, h)
		if err != nil {
			panic(err)
		}
		hs.Add(h)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := io.ReadFull(randomSrc, h)
		if err != nil {
			panic(err)
		}
		hs.Contains(h)
	}
}

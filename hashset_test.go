package hashset

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
)

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

func d(h string) []byte {
	rv, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	return rv
}

func TestLen(t *testing.T) {
	hs := Hashset{}
	if hs.Len() != 0 {
		t.Fatalf("Expected len == 0, got %v", hs.Len())
	}

	hs.Add(d("c9adad8f9201c0cdcf68d0023b16f4979eb799c0"))
	if hs.Len() != 1 {
		t.Fatalf("Expected len == 1, got %v", hs.Len())
	}

	hs.Add(d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"))
	if hs.Len() != 2 {
		t.Fatalf("Expected len == 2, got %v", hs.Len())
	}

	hs.Add(d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"))
	if hs.Len() != 2 {
		t.Fatalf("Expected len == 2, got %v", hs.Len())
	}

	hs.Add(d("d9adad8f9201c0cdcf68d2023b16f4979eb799c0"))
	if hs.Len() != 3 {
		t.Fatalf("Expected len == 2, got %v", hs.Len())
	}
}

func TestIter(t *testing.T) {
	someHashes := [][]byte{
		d("c9adad8f9201c0cdcf68d0023b16f4979eb799c0"),
		d("d9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
		d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
		d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
	}

	exp := [][]byte{
		d("c9adad8f9201c0cdcf68d0023b16f4979eb799c0"),
		d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
		d("d9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
	}

	hs := Hashset{}
	for _, h := range someHashes {
		hs.Add(h)
	}

	chi := hs.Iter()
	for i, e := range exp {
		got := <-chi
		if !bytes.Equal(e, got) {
			t.Errorf("Expected %x at %v, got %x", e, i, got)
		}
	}

	got, ok := <-chi
	if ok {
		t.Fatalf("Expected failure to read after iter, got %x",
			got)
	}
}

func TestWrite(t *testing.T) {
	someHashes := [][]byte{
		d("c9adad8f9201c0cdcf68d0023b16f4979eb799c0"),
		d("d9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
		d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
	}

	hs := Hashset{}
	for _, h := range someHashes {
		hs.Add(h)
	}

	buf := &bytes.Buffer{}
	n, err := hs.Write(buf)
	if err != nil {
		t.Fatalf("Error writing buffer: %v", err)
	}
	if n != 60 {
		t.Fatalf("Expected to write 60 bytes, wrote %v", n)
	}

	exp := []byte{}
	for b := range hs.Iter() {
		exp = append(exp, b...)
	}

	if !bytes.Equal(buf.Bytes(), exp) {
		t.Fatalf("Expected\n%x, got\n%x", exp, buf.Bytes())
	}
}

func TestRead(t *testing.T) {
	someHashes := [][]byte{
		d("c9adad8f9201c0cdcf68d0023b16f4979eb799c0"),
		d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
		d("d9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
	}
	stuff := []byte{}
	for _, b := range someHashes {
		stuff = append(stuff, b...)
	}
	hs, err := Load(20, bytes.NewReader(stuff))
	if err != nil {
		t.Fatalf("Error reading hashes: %v", err)
	}
	if hs.Len() != 3 {
		t.Fatalf("Expected 3 hashes, got %v", hs.Len())
	}

	chi := hs.Iter()
	for i, e := range someHashes {
		got := <-chi
		if !bytes.Equal(e, got) {
			t.Errorf("Expected %x at %v, got %x", e, i, got)
		}
	}
}

func TestSet(t *testing.T) {
	samples := [][]byte{}

	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}

	// Enough hashes to surely hit all the cases
	for i := 0; i < 65535*5; i++ {
		buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
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
			n := int(binary.BigEndian.Uint16(h))
			bin := hs.things[n]
			l := len(h) - 2
			for i := 0; i < len(bin)/l; i++ {
				off := i * l
				log.Printf("  not %x", bin[off:off+l])
			}
			t.FailNow()

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

var aBigHashset *Hashset

func initTestHashset() {
	if aBigHashset != nil {
		return
	}

	aBigHashset = &Hashset{}
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}

	h := make([]byte, 20)
	for i := 0; i < 1e6; i++ {
		_, err := io.ReadFull(randomSrc, h)
		if err != nil {
			panic(err)
		}
		aBigHashset.Add(h)
	}
}

func BenchmarkFind(b *testing.B) {
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	initTestHashset()
	b.ResetTimer()

	h := make([]byte, 20)
	for i := 0; i < b.N; i++ {
		_, err := io.ReadFull(randomSrc, h)
		if err != nil {
			panic(err)
		}
		aBigHashset.Contains(h)
	}
}

func BenchmarkWrite(b *testing.B) {
	initTestHashset()
	b.SetBytes(int64(aBigHashset.Len() * 20))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		aBigHashset.Write(ioutil.Discard)
	}
}

func BenchmarkRead(b *testing.B) {
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	b.SetBytes(int64(20 * b.N))
	_, err := Load(20, io.LimitReader(randomSrc, int64(20*b.N)))
	if err != nil {
		b.Fatalf("Error reading at %v", b.N)
	}
}

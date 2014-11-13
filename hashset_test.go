package hashset

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

func TestFuncIter(t *testing.T) {
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

	i := 0
	hs.FuncIter(func(got []byte) {
		e := exp[i]
		if !bytes.Equal(e, got) {
			t.Errorf("Expected %x at %v, got %x", e, i, got)
		}
		i++
	})
	if i != len(exp) {
		t.Errorf("Expected %d items, got %d", len(exp), i)
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

func TestContains(t *testing.T) {
	hash := md5.New()
	hashes := [][]byte{}
	for i := 0; i < 50000; i++ {
		hash.Reset()
		fmt.Fprintf(hash, "%d", i)
		hashes = append(hashes, append([]byte{0}, hash.Sum(nil)...))
	}
	hs := Hashset{}
	for _, h := range hashes {
		if hs.Contains(h) {
			t.Errorf("Unexpected had %x before adding it", h)
		}
		hs.UnsafeAdd(h)
		if !hs.Contains(h) {
			t.Errorf("Failed to find %x", h)
		}
	}
}

func benchContains(b *testing.B, n int) {
	hash := md5.New()
	hashes := [][]byte{}
	for i := 0; i < n; i++ {
		hash.Reset()
		fmt.Fprintf(hash, "%d", i)
		hashes = append(hashes, append([]byte{0}, hash.Sum(nil)...))
	}
	b.ResetTimer()
	for i := 0; i < b.N/len(hashes); i++ {
		hs := Hashset{}
		for _, h := range hashes {
			hs.UnsafeAdd(h)
			if !hs.Contains(h) {
				b.Errorf("Failed to find %x", h)
			}
		}
	}
}

func BenchmarkContainsSmall(b *testing.B) {
	benchContains(b, 100)
}

func BenchmarkContainsMedium(b *testing.B) {
	benchContains(b, 1000)
}

func BenchmarkContainsLarge(b *testing.B) {
	benchContains(b, 100000)
}

type ErrorWriter struct{ error }

func (ew *ErrorWriter) Write(b []byte) (int, error) {
	return 19, error(ew.error)
}

func TestWriteError(t *testing.T) {
	initTestHashset()
	ew := &ErrorWriter{io.ErrUnexpectedEOF}
	n, err := aBigHashset.Write(ew)
	if n != 19 {
		t.Errorf("Expected to write 19 bytes, wrote %v", n)
	}
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected the unexpected, got %v", err)
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

func TestUnion(t *testing.T) {
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	samples := [][]byte{}
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

	hss := []*Hashset{&Hashset{}, &Hashset{}}
	for i, h := range samples {
		hss[i%len(hss)].Add(h)
	}

	hss[0].AddAll(&Hashset{}) // noop
	hss[0].AddAll(hss[1])

	for _, h := range samples {
		if !hss[0].Contains(h) {
			t.Fatalf("Missing %x", h)
		}
	}
}

func TestEmptyUnion(t *testing.T) {
	hs := &Hashset{}
	someHashes := [][]byte{
		d("c9adad8f9201c0cdcf68d0023b16f4979eb799c0"),
		d("c9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
		d("d9adad8f9201c0cdcf68d2023b16f4979eb799c0"),
	}
	for _, h := range someHashes {
		hs.Add(h)
	}

	h2 := &Hashset{}
	h2.AddAll(hs)

	for _, h := range someHashes {
		if !h2.Contains(h) {
			t.Errorf("Was missing %x", h)
		}
	}
}

func TestIntersection(t *testing.T) {
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	hss := []*Hashset{&Hashset{}, &Hashset{},
		&Hashset{}, &Hashset{}}

	// Enough hashes to surely hit all the cases
	for i := 0; i < 65535*5; i++ {
		buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		_, err := io.ReadFull(randomSrc, buf)
		if err != nil {
			panic(err)
		}
		for j := 0; j <= rand.Intn(len(hss)); j++ {
			hss[j].Add(buf)
		}
	}

	intersected := Intersection(hss[0], hss[1:]...)
	if intersected.Len() == 0 {
		t.Fatalf("Intersected to 0")
	}

	for h := range intersected.Iter() {
		for i, hs := range hss {
			if !hs.Contains(h) {
				t.Errorf("%x is missing from the %vth", h, i)
			}
		}
	}
}

func BenchmarkIntersection(b *testing.B) {
	b.StopTimer()

	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	hss := []*Hashset{&Hashset{}, &Hashset{},
		&Hashset{}, &Hashset{}}

	// Enough hashes to surely hit all the cases
	for i := 0; i < 65535*5; i++ {
		buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		_, err := io.ReadFull(randomSrc, buf)
		if err != nil {
			panic(err)
		}
		for j := 0; j <= rand.Intn(len(hss)); j++ {
			hss[j].Add(buf)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Intersection(hss[0], hss[1:]...)
	}
}

func TestCopy(t *testing.T) {
	initTestHashset()

	h2 := aBigHashset.Copy()
	for h := range aBigHashset.Iter() {
		if !h2.Contains(h) {
			t.Errorf("Copy doesn't have %x", h)
		}
	}
}

func TestAddMismatch(t *testing.T) {
	var err interface{}
	func() {
		defer func() { err = recover() }()
		h := &Hashset{}
		h.Add(make([]byte, 4))
		h.Add(make([]byte, 6))
	}()
	if err == nil {
		t.Errorf("Expected error adding different-sized []byte")
	}
}

func TestAddAllMismatch(t *testing.T) {
	var err interface{}
	func() {
		defer func() { err = recover() }()
		h1 := &Hashset{}
		h1.Add(make([]byte, 4))
		h2 := &Hashset{}
		h2.Add(make([]byte, 6))
		h1.AddAll(h2)
	}()
	if err == nil {
		t.Errorf("Expected error adding different-sized Hashset")
	}
}

func BenchmarkUnion(b *testing.B) {
	b.StopTimer()
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	samples := [][]byte{}
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

	hss := []*Hashset{&Hashset{}, &Hashset{}}
	for i, h := range samples {
		hss[i%len(hss)].Add(h)
	}

	for i := 0; i < b.N; i++ {
		tmp := hss[0].Copy()

		b.StartTimer()
		tmp.AddAll(hss[1])
		b.StopTimer()
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

func BenchmarkSetNoCheck(b *testing.B) {
	hs := Hashset{}
	randomSrc := &randomDataMaker{rand.NewSource(1028890720402726901)}
	b.ResetTimer()

	h := make([]byte, 20)
	for i := 0; i < b.N; i++ {
		_, err := io.ReadFull(randomSrc, h)
		if err != nil {
			panic(err)
		}
		hs.UnsafeAdd(h)
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

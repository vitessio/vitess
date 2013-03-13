package cgzip

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"hash/adler32"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/rand"
	"os/exec"
	"sync"
	"testing"
	"time"
)

type prettyTimer struct {
	name   string
	before time.Time
}

func newPrettyTimer(name string) *prettyTimer {
	return &prettyTimer{name, time.Now()}
}

func (pt *prettyTimer) stopAndPrintCompress(t *testing.T, size, processed int) {
	durationMs := int(int64(time.Now().Sub(pt.before)) / 1000)
	t.Log(pt.name + ":")
	t.Log("  size :", size)
	t.Log("  time :", durationMs, "ms")
	t.Log("  speed:", processed*1000/durationMs, "KB/s")
}

func (pt *prettyTimer) stopAndPrintUncompress(t *testing.T, processed int) {
	durationMs := int(int64(time.Now().Sub(pt.before)) / 1000)
	t.Log("     " + pt.name + ":")
	t.Log("       time :", durationMs, "ms")
	t.Log("       speed:", processed*1000/durationMs, "KB/s")
}

func compareCompressedBuffer(t *testing.T, source []byte, compressed *bytes.Buffer) {
	// compare using go's gunzip
	toGunzip := bytes.NewBuffer(compressed.Bytes())
	gunzip, err := gzip.NewReader(toGunzip)
	if err != nil {
		t.Errorf("gzip.NewReader failed: %v", err)
	}
	uncompressed := &bytes.Buffer{}
	pt := newPrettyTimer("go unzip")
	_, err = io.Copy(uncompressed, gunzip)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	pt.stopAndPrintUncompress(t, uncompressed.Len())
	if !bytes.Equal(source, uncompressed.Bytes()) {
		t.Errorf("Bytes are not equal")
	}

	// compare using cgzip gunzip
	toGunzip = bytes.NewBuffer(compressed.Bytes())
	cgunzip, err := NewReader(toGunzip)
	if err != nil {
		t.Errorf("cgzip.NewReader failed: %v", err)
	}
	uncompressed = &bytes.Buffer{}
	pt = newPrettyTimer("cgzip unzip")
	_, err = io.Copy(uncompressed, cgunzip)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	pt.stopAndPrintUncompress(t, uncompressed.Len())
	if !bytes.Equal(source, uncompressed.Bytes()) {
		t.Errorf("Bytes are not equal")
	}
}

func testChecksums(t *testing.T, data []byte) {
	t.Log("Checksums:")

	// crc64 with go library
	goCrc64 := crc64.New(crc64.MakeTable(crc64.ECMA))
	toChecksum := bytes.NewBuffer(data)
	pt := newPrettyTimer("go crc64")
	_, err := io.Copy(goCrc64, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	pt.stopAndPrintUncompress(t, len(data))

	// adler32 with go library
	goAdler32 := adler32.New()
	toChecksum = bytes.NewBuffer(data)
	pt = newPrettyTimer("go adler32")
	_, err = io.Copy(goAdler32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	goResult := goAdler32.Sum32()
	pt.stopAndPrintUncompress(t, len(data))
	t.Log("       sum  :", goResult)

	// adler32 with cgzip library
	cgzipAdler32 := NewAdler32()
	toChecksum = bytes.NewBuffer(data)
	pt = newPrettyTimer("cgzip adler32")
	_, err = io.Copy(cgzipAdler32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	cgzipResult := cgzipAdler32.Sum32()
	pt.stopAndPrintUncompress(t, len(data))
	t.Log("       sum  :", cgzipResult)

	// test both results are the same
	if goResult != cgzipResult {
		t.Errorf("go and cgzip adler32 mismatch")
	}

	// now test partial checksuming also works with adler32
	cutoff := len(data) / 3
	toChecksum = bytes.NewBuffer(data[0:cutoff])
	cgzipAdler32.Reset()
	_, err = io.Copy(cgzipAdler32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	adler1 := cgzipAdler32.Sum32()
	t.Log("   a1   :", adler1)
	t.Log("   len1 :", cutoff)

	toChecksum = bytes.NewBuffer(data[cutoff:])
	cgzipAdler32.Reset()
	_, err = io.Copy(cgzipAdler32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	adler2 := cgzipAdler32.Sum32()
	t.Log("   a2   :", adler2)
	t.Log("   len2 :", len(data)-cutoff)

	adlerCombined := Adler32Combine(adler1, adler2, len(data)-cutoff)
	t.Log("   comb :", adlerCombined)

	if cgzipResult != adlerCombined {
		t.Errorf("full and combined adler32 mismatch")
	}

	// crc32 with go library
	goCrc32 := crc32.New(crc32.MakeTable(crc32.IEEE))
	toChecksum = bytes.NewBuffer(data)
	pt = newPrettyTimer("go crc32")
	_, err = io.Copy(goCrc32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	goResult = goCrc32.Sum32()
	pt.stopAndPrintUncompress(t, len(data))
	t.Log("       sum  :", goResult)

	// crc32 with cgzip library
	cgzipCrc32 := NewCrc32()
	toChecksum = bytes.NewBuffer(data)
	pt = newPrettyTimer("cgzip crc32")
	_, err = io.Copy(cgzipCrc32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	cgzipResult = cgzipCrc32.Sum32()
	pt.stopAndPrintUncompress(t, len(data))
	t.Log("       sum  :", cgzipResult)

	// test both results are the same
	if goResult != cgzipResult {
		t.Errorf("go and cgzip crc32 mismatch")
	}

	// now test partial checksuming also works with crc32
	toChecksum = bytes.NewBuffer(data[0:cutoff])
	cgzipCrc32.Reset()
	_, err = io.Copy(cgzipCrc32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	crc1 := cgzipCrc32.Sum32()
	t.Log("   crc1 :", crc1)
	t.Log("   len1 :", cutoff)

	toChecksum = bytes.NewBuffer(data[cutoff:])
	cgzipCrc32.Reset()
	_, err = io.Copy(cgzipCrc32, toChecksum)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	crc2 := cgzipCrc32.Sum32()
	t.Log("   crc2 :", crc2)
	t.Log("   len2 :", len(data)-cutoff)

	crcCombined := Crc32Combine(crc1, crc2, len(data)-cutoff)
	t.Log("   comb :", crcCombined)

	if cgzipResult != crcCombined {
		t.Errorf("full and combined crc32 mismatch")
	}
}

func runCompare(t *testing.T, testSize int, level int) {

	// create a test chunk, put semi-random bytes in there
	// (so compression actually will compress some)
	toEncode := make([]byte, testSize)
	where := 0
	for where < testSize {
		toFill := rand.Intn(16)
		filler := 0x61 + rand.Intn(24)
		for i := 0; i < toFill && where < testSize; i++ {
			toEncode[where] = byte(filler)
			where++
		}
	}
	t.Log("Original size:", len(toEncode))

	// now time a regular gzip writer to a Buffer
	compressed := &bytes.Buffer{}
	reader := bytes.NewBuffer(toEncode)
	pt := newPrettyTimer("Go gzip")
	gz, err := gzip.NewWriterLevel(compressed, level)
	_, err = io.Copy(gz, reader)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	gz.Close()
	pt.stopAndPrintCompress(t, compressed.Len(), len(toEncode))
	compareCompressedBuffer(t, toEncode, compressed)

	// now time a forked gzip
	compressed2 := &bytes.Buffer{}
	reader = bytes.NewBuffer(toEncode)
	cmd := exec.Command("gzip", fmt.Sprintf("-%v", level), "-c")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Errorf("StdoutPipe failed: %v", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Errorf("StdinPipe failed: %v", err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		io.Copy(compressed2, stdout)
		wg.Done()
	}()
	if err = cmd.Start(); err != nil {
		t.Errorf("Start failed: %v", err)
	}
	pt = newPrettyTimer("Forked gzip")
	_, err = io.Copy(stdin, reader)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	stdin.Close()
	wg.Wait()
	if err := cmd.Wait(); err != nil {
		t.Errorf("Wait failed: %v", err)
	}
	pt.stopAndPrintCompress(t, compressed2.Len(), len(toEncode))
	compareCompressedBuffer(t, toEncode, compressed2)

	// and time the cgo version
	compressed3 := &bytes.Buffer{}
	reader = bytes.NewBuffer(toEncode)
	pt = newPrettyTimer("cgzip")
	cgz, err := NewWriterLevel(compressed3, level)
	if err != nil {
		t.Errorf("NewWriterLevel failed: %v", err)
	}
	_, err = io.Copy(cgz, reader)
	if err != nil {
		t.Errorf("Copy failed: %v", err)
	}
	if err := cgz.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}
	if err := cgz.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	pt.stopAndPrintCompress(t, compressed3.Len(), len(toEncode))
	compareCompressedBuffer(t, toEncode, compressed3)

	testChecksums(t, toEncode)
}

// use 'go test -v' and bigger sizes to show meaningful rates
func TestCompare(t *testing.T) {
	runCompare(t, 1*1024*1024, 1)
}

func TestCompareBest(t *testing.T) {
	runCompare(t, 1*1024*1024, 9)
}

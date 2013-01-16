package cgzip

import (
	"bytes"
	"compress/gzip"
	"fmt"
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
	durationMs := int(int64(time.Now().Sub(pt.before)) / 1000000)
	t.Log(pt.name + ":")
	t.Log("  size :", size)
	t.Log("  time :", durationMs, "ms")
	t.Log("  speed:", processed/durationMs, "KB/s")
}

func (pt *prettyTimer) stopAndPrintUncompress(t *testing.T, processed int) {
	durationMs := int(int64(time.Now().Sub(pt.before)) / 1000000)
	t.Log("     " + pt.name + ":")
	t.Log("       time :", durationMs, "ms")
	t.Log("       speed:", processed/durationMs, "KB/s")
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
	if err := cmd.Wait(); err != nil {
		t.Errorf("Wait failed: %v", err)
	}
	wg.Wait()
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
}

// use 'go test -v' and bigger sizes to show meaningful rates
func TestCompare(t *testing.T) {
	runCompare(t, 1*1024*1024, 1)
}

func TestCompareBest(t *testing.T) {
	runCompare(t, 1*1024*1024, 9)
}

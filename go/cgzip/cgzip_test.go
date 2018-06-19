/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	duration := time.Since(pt.before)
	t.Log(pt.name + ":")
	t.Log("  size :", size)
	t.Log("  time :", duration.String())
	if duration != 0 {
		t.Logf("  speed: %.0f KB/s", float64(processed)/duration.Seconds()/1024.0)
	} else {
		t.Log("  processed:", processed, "B")
	}
}

func (pt *prettyTimer) stopAndPrintUncompress(t *testing.T, processed int) {
	duration := time.Since(pt.before)
	t.Log("     " + pt.name + ":")
	t.Log("       time :", duration.String())
	if duration != 0 {
		t.Logf("       speed: %.0f KB/s", float64(processed)/duration.Seconds()/1024.0)
	} else {
		t.Log("       processed:", processed, "B")
	}
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
	testSize := 1 * 1024 * 1024
	if testing.Short() {
		testSize /= 10
	}
	runCompare(t, testSize, 1)
}

func TestCompareBest(t *testing.T) {
	testSize := 1 * 1024 * 1024
	if testing.Short() {
		testSize /= 10
	}
	runCompare(t, testSize, 9)
}

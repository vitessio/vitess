package mysql

import (
	"bufio"
	"bytes"
	"io"
	"sync"
	"testing"
)

func TestWriter(t *testing.T) {
	testWriter := bytes.NewBuffer(nil)
	tw := newWriter(testWriter)

	expectedWriter := bytes.NewBuffer(nil)
	ew := bufio.NewWriterSize(expectedWriter, connBufferSize)

	testString := []byte("test string for test writer")
	for i := 0; i < 10000; i++ {
		n, err := tw.Write(testString)
		if err != nil {
			t.Fatal("write error", err)
		}
		if n != len(testString) {
			t.Fatalf("invalid write length: %d, expected %d", n, len(testString))
		}
		ew.Write(testString)
		if i%4 == 0 {
			if err := tw.Flush(); err != nil {
				t.Fatal("flush error", err)
			}
			if tw.(*poolBufioWriter).bw != nil {
				t.Fatal("bufio.Writer wasn't recycled on flush")
			}
		}
		if i%8 == 0 {
			tw.Reset(testWriter)
			if tw.(*poolBufioWriter).bw != nil {
				t.Fatal("bufio.Writer wasn't recycled on reset")
			}
		}
	}
	tw.Flush()
	ew.Flush()
	if testWriter.String() != expectedWriter.String() {
		t.Fatalf("strings from bufioWriter and *bufio.Writer are different")
	}
}

func TestWriterParallel(t *testing.T) {
	writersCount := 8

	var bufioWriters []bufioWriter
	var testWriters []*bytes.Buffer
	for i := 0; i < writersCount; i++ {
		tw := bytes.NewBuffer(nil)
		bw := newWriter(tw)

		testWriters = append(testWriters, tw)
		bufioWriters = append(bufioWriters, bw)
	}

	testString := []byte("test string for test writer")
	writeCount := 10000

	writeStrings := func(w bufioWriter, testWriter io.Writer) error {
		for i := 0; i < writeCount; i++ {
			if _, err := w.Write(testString); err != nil {
				return err
			}
			if i%4 == 0 {
				if err := w.Flush(); err != nil {
					return err
				}
			}
			if i%8 == 0 {
				w.Reset(testWriter)
			}
		}
		w.Flush()
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(bufioWriters))
	for i, w := range bufioWriters {
		wg.Add(1)
		tw := testWriters[i]
		go func(w bufioWriter) {
			errCh <- writeStrings(w, tw)
			wg.Done()
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal("error write strings", err)
		}
	}
	for _, w := range testWriters {
		data := w.Bytes()
		if len(data) != len(testString)*writeCount {
			t.Fatal("invalid data length in buffer")
		}
		if bytes.Count(data, testString) != writeCount {
			t.Fatal("unexpected data in buffer")
		}
	}
}

package cgzip

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

// specialReader is a test class that will return bytes it reads from a file,
// returning EOF and data in the last chunk.
type specialReader struct {
	t        *testing.T
	contents []byte
	sent     int
}

func newSpecialReader(t *testing.T, filename string) *specialReader {
	filename = testfiles.Locate(filename)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("Cannot read file %v: %v", filename, err)
	}
	return &specialReader{t, b, 0}
}

// Read is the implementation of Reader
func (sr *specialReader) Read(p []byte) (int, error) {
	if len(p) > len(sr.contents)-sr.sent {
		toCopy := len(sr.contents) - sr.sent
		sr.t.Logf("Sending %v bytes and EOF", toCopy)
		sr.sent += copy(p, sr.contents[sr.sent:])
		return toCopy, io.EOF
	}
	toCopy := len(p)
	sr.sent += copy(p, sr.contents[sr.sent:sr.sent+toCopy])
	sr.t.Logf("Sending %v bytes", toCopy)
	return toCopy, nil
}

// TestEofAndData is the main test here: if we return data and EOF,
// it needs to be fully processed.
// The file is a 55k file, that uncompresses into a 10 MB file.
// So it will be read as 32k + 22k, and decompressed into 2MB + 2MB + 1M and
// then 2MB + 2MB + 1M again. So it's a great test for corner cases.
func TestEofAndData(t *testing.T) {
	r := newSpecialReader(t, "cgzip_eof.gz")
	gz, err := NewReader(r)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	n := 0
	dst := make([]byte, 2*1024*1024)
	for {
		nRead, err := gz.Read(dst)
		t.Logf("Got: %v %v", nRead, err)
		n += nRead
		switch err {
		case nil:
		case io.EOF:
			if n != 10485760 {
				t.Fatalf("Read wrong number of bytes: got %v expected 10485760", n)
			}

			// test we also get 0 / EOF if we read again
			nRead, err = gz.Read(dst)
			if nRead != 0 || err != io.EOF {
				t.Fatalf("After-EOF read got %v %v", nRead, err)
			}
			return
		default:
			t.Fatalf("Unexpected error: %v", err)
		}
	}
}

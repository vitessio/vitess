package cgzip

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

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
			return
		default:
			t.Fatalf("Unexpected error: %v", err)
		}
	}
}

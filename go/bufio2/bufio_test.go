// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bufio2

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

const minReadBufferSize = 16

// Reads from a reader and rot13s the result.
type rot13Reader struct {
	r io.Reader
}

func newRot13Reader(r io.Reader) *rot13Reader {
	r13 := new(rot13Reader)
	r13.r = r
	return r13
}

func (r13 *rot13Reader) Read(p []byte) (int, error) {
	n, err := r13.r.Read(p)
	if err != nil {
		return n, err
	}
	for i := 0; i < n; i++ {
		c := p[i] | 0x20 // lowercase byte
		if 'a' <= c && c <= 'm' {
			p[i] += 13
		} else if 'n' <= c && c <= 'z' {
			p[i] -= 13
		}
	}
	return n, nil
}

// Call ReadByte to accumulate the text of a file
func readBytes(buf *bufio.Reader) string {
	var b [1000]byte
	nb := 0
	for {
		c, err := buf.ReadByte()
		if err == io.EOF {
			break
		}
		if err == nil {
			b[nb] = c
			nb++
		} else if err != iotest.ErrTimeout {
			panic("Data: " + err.Error())
		}
	}
	return string(b[0:nb])
}

func TestReaderSimple(t *testing.T) {
	data := "hello world"
	b := bufio.NewReader(bytes.NewBufferString(data))
	if s := readBytes(b); s != "hello world" {
		t.Errorf("simple hello world test failed: got %q", s)
	}

	b = bufio.NewReader(newRot13Reader(bytes.NewBufferString(data)))
	if s := readBytes(b); s != "uryyb jbeyq" {
		t.Errorf("rot13 hello world test failed: got %q", s)
	}
}

type readMaker struct {
	name string
	fn   func(io.Reader) io.Reader
}

var readMakers = []readMaker{
	{"full", func(r io.Reader) io.Reader { return r }},
	{"byte", iotest.OneByteReader},
	{"half", iotest.HalfReader},
	{"data+err", iotest.DataErrReader},
	{"timeout", iotest.TimeoutReader},
}

// Call ReadString (which ends up calling everything else)
// to accumulate the text of a file.
func readLines(b *bufio.Reader) string {
	s := ""
	for {
		s1, err := b.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil && err != iotest.ErrTimeout {
			panic("GetLines: " + err.Error())
		}
		s += s1
	}
	return s
}

// Call Read to accumulate the text of a file
func reads(buf *bufio.Reader, m int) string {
	var b [1000]byte
	nb := 0
	for {
		n, err := buf.Read(b[nb : nb+m])
		nb += n
		if err == io.EOF {
			break
		}
	}
	return string(b[0:nb])
}

type bufReader struct {
	name string
	fn   func(*bufio.Reader) string
}

var bufreaders = []bufReader{
	{"1", func(b *bufio.Reader) string { return reads(b, 1) }},
	{"2", func(b *bufio.Reader) string { return reads(b, 2) }},
	{"3", func(b *bufio.Reader) string { return reads(b, 3) }},
	{"4", func(b *bufio.Reader) string { return reads(b, 4) }},
	{"5", func(b *bufio.Reader) string { return reads(b, 5) }},
	{"7", func(b *bufio.Reader) string { return reads(b, 7) }},
	{"bytes", readBytes},
	{"lines", readLines},
}

var bufsizes = []int{
	minReadBufferSize, 23, 32, 46, 64, 93, 128, 1024, 4096,
}

func TestReader(t *testing.T) {
	var texts [31]string
	str := ""
	all := ""
	for i := 0; i < len(texts)-1; i++ {
		texts[i] = str + "\n"
		all += texts[i]
		str += string(i%26 + 'a')
	}
	texts[len(texts)-1] = all

	for h := 0; h < len(texts); h++ {
		text := texts[h]
		for i := 0; i < len(readMakers); i++ {
			for j := 0; j < len(bufreaders); j++ {
				for k := 0; k < len(bufsizes); k++ {
					readmaker := readMakers[i]
					bufreader := bufreaders[j]
					bufsize := bufsizes[k]
					read := readmaker.fn(bytes.NewBufferString(text))
					buf := bufio.NewReaderSize(read, bufsize)
					s := bufreader.fn(buf)
					if s != text {
						t.Errorf("reader=%s fn=%s bufsize=%d want=%q got=%q",
							readmaker.name, bufreader.name, bufsize, text, s)
					}
				}
			}
		}
	}
}

// A StringReader delivers its data one string segment at a time via Read.
type StringReader struct {
	data []string
	step int
}

func (r *StringReader) Read(p []byte) (n int, err error) {
	if r.step < len(r.data) {
		s := r.data[r.step]
		n = copy(p, s)
		r.step++
	} else {
		err = io.EOF
	}
	return
}

func readRuneSegments(t *testing.T, segments []string) {
	got := ""
	want := strings.Join(segments, "")
	r := bufio.NewReader(&StringReader{data: segments})
	for {
		r, _, err := r.ReadRune()
		if err != nil {
			if err != io.EOF {
				return
			}
			break
		}
		got += string(r)
	}
	if got != want {
		t.Errorf("segments=%v got=%s want=%s", segments, got, want)
	}
}

var segmentList = [][]string{
	{},
	{""},
	{"日", "本語"},
	{"\u65e5", "\u672c", "\u8a9e"},
	{"\U000065e5", "\U0000672c", "\U00008a9e"},
	{"\xe6", "\x97\xa5\xe6", "\x9c\xac\xe8\xaa\x9e"},
	{"Hello", ", ", "World", "!"},
	{"Hello", ", ", "", "World", "!"},
}

func TestReadRune(t *testing.T) {
	for _, s := range segmentList {
		readRuneSegments(t, s)
	}
}

func TestUnreadRune(t *testing.T) {
	got := ""
	segments := []string{"Hello, world:", "日本語"}
	data := strings.Join(segments, "")
	r := bufio.NewReader(&StringReader{data: segments})
	// Normal execution.
	for {
		r1, _, err := r.ReadRune()
		if err != nil {
			if err != io.EOF {
				t.Error("unexpected EOF")
			}
			break
		}
		got += string(r1)
		// Put it back and read it again
		if err = r.UnreadRune(); err != nil {
			t.Error("unexpected error on UnreadRune:", err)
		}
		r2, _, err := r.ReadRune()
		if err != nil {
			t.Error("unexpected error reading after unreading:", err)
		}
		if r1 != r2 {
			t.Errorf("incorrect rune after unread: got %c wanted %c", r1, r2)
		}
	}
	if got != data {
		t.Errorf("want=%q got=%q", data, got)
	}
}

// Test that UnreadRune fails if the preceding operation was not a ReadRune.
func TestUnreadRuneError(t *testing.T) {
	buf := make([]byte, 3) // All runes in this test are 3 bytes long
	r := bufio.NewReader(&StringReader{data: []string{"日本語日本語日本語"}})
	if r.UnreadRune() == nil {
		t.Error("expected error on UnreadRune from fresh buffer")
	}
	_, _, err := r.ReadRune()
	if err != nil {
		t.Error("unexpected error on ReadRune (1):", err)
	}
	if err = r.UnreadRune(); err != nil {
		t.Error("unexpected error on UnreadRune (1):", err)
	}
	if r.UnreadRune() == nil {
		t.Error("expected error after UnreadRune (1)")
	}
	// Test error after Read.
	_, _, err = r.ReadRune() // reset state
	if err != nil {
		t.Error("unexpected error on ReadRune (2):", err)
	}
	_, err = r.Read(buf)
	if err != nil {
		t.Error("unexpected error on Read (2):", err)
	}
	if r.UnreadRune() == nil {
		t.Error("expected error after Read (2)")
	}
	// Test error after ReadByte.
	_, _, err = r.ReadRune() // reset state
	if err != nil {
		t.Error("unexpected error on ReadRune (2):", err)
	}
	for range buf {
		_, err = r.ReadByte()
		if err != nil {
			t.Error("unexpected error on ReadByte (2):", err)
		}
	}
	if r.UnreadRune() == nil {
		t.Error("expected error after ReadByte")
	}
	// Test error after UnreadByte.
	_, _, err = r.ReadRune() // reset state
	if err != nil {
		t.Error("unexpected error on ReadRune (3):", err)
	}
	_, err = r.ReadByte()
	if err != nil {
		t.Error("unexpected error on ReadByte (3):", err)
	}
	err = r.UnreadByte()
	if err != nil {
		t.Error("unexpected error on UnreadByte (3):", err)
	}
	if r.UnreadRune() == nil {
		t.Error("expected error after UnreadByte (3)")
	}
}

func TestUnreadRuneAtEOF(t *testing.T) {
	// UnreadRune/ReadRune should error at EOF (was a bug; used to panic)
	r := bufio.NewReader(strings.NewReader("x"))
	r.ReadRune()
	r.ReadRune()
	r.UnreadRune()
	_, _, err := r.ReadRune()
	if err == nil {
		t.Error("expected error at EOF")
	} else if err != io.EOF {
		t.Error("expected EOF; got", err)
	}
}

func TestReadWriteRune(t *testing.T) {
	const NRune = 1000
	byteBuf := new(bytes.Buffer)
	w := NewAsyncWriter(byteBuf)
	// Write the runes out using WriteRune
	buf := make([]byte, utf8.UTFMax)
	for r := rune(0); r < NRune; r++ {
		size := utf8.EncodeRune(buf, r)
		nbytes, err := w.WriteRune(r)
		if err != nil {
			t.Fatalf("WriteRune(0x%x) error: %s", r, err)
		}
		if nbytes != size {
			t.Fatalf("WriteRune(0x%x) expected %d, got %d", r, size, nbytes)
		}
	}
	w.Flush()

	r := bufio.NewReader(byteBuf)
	// Read them back with ReadRune
	for r1 := rune(0); r1 < NRune; r1++ {
		size := utf8.EncodeRune(buf, r1)
		nr, nbytes, err := r.ReadRune()
		if nr != r1 || nbytes != size || err != nil {
			t.Fatalf("ReadRune(0x%x) got 0x%x,%d not 0x%x,%d (err=%s)", r1, nr, nbytes, r1, size, err)
		}
	}
}

func TestWriter(t *testing.T) {
	var data [8192]byte

	for i := 0; i < len(data); i++ {
		data[i] = byte(' ' + i%('~'-' '))
	}
	w := new(bytes.Buffer)
	for i := 0; i < len(bufsizes); i++ {
		for j := 0; j < len(bufsizes); j++ {
			nwrite := bufsizes[i]
			bs := bufsizes[j]

			// Write nwrite bytes using buffer size bs.
			// Check that the right amount makes it out
			// and that the data is correct.

			w.Reset()
			buf := NewAsyncWriterSize(w, bs, defaultBufCount)
			context := fmt.Sprintf("nwrite=%d bufsize=%d", nwrite, bs)
			n, e1 := buf.Write(data[0:nwrite])
			if e1 != nil || n != nwrite {
				t.Errorf("%s: buf.Write %d = %d, %v", context, nwrite, n, e1)
				continue
			}
			if e := buf.Flush(); e != nil {
				t.Errorf("%s: buf.Flush = %v", context, e)
			}

			written := w.Bytes()
			if len(written) != nwrite {
				t.Errorf("%s: %d bytes written", context, len(written))
			}
			for l := 0; l < len(written); l++ {
				if written[i] != data[i] {
					t.Errorf("wrong bytes written")
					t.Errorf("want=%q", data[0:len(written)])
					t.Errorf("have=%q", written)
				}
			}
		}
	}
}

// Check that write errors are returned properly.

type errorWriterTest struct {
	n, m   int
	err    error
	expect error
}

func (w errorWriterTest) Write(p []byte) (int, error) {
	return len(p) * w.n / w.m, w.err
}

var errorWriterTests = []errorWriterTest{
	{0, 1, nil, io.ErrShortWrite},
	{1, 2, nil, io.ErrShortWrite},
	{1, 1, nil, nil},
	{0, 1, io.ErrClosedPipe, io.ErrClosedPipe},
	{1, 2, io.ErrClosedPipe, io.ErrClosedPipe},
	{1, 1, io.ErrClosedPipe, io.ErrClosedPipe},
}

func TestWriteErrors(t *testing.T) {
	for _, w := range errorWriterTests {
		buf := NewAsyncWriter(w)
		_, e := buf.Write([]byte("hello world"))
		if e != nil {
			t.Errorf("Write hello to %v: %v", w, e)
			continue
		}
		e = buf.Flush()
		if e != w.expect {
			t.Errorf("Flush %v: got %v, wanted %v", w, e, w.expect)
		}
	}
}

func TestNewReaderSizeIdempotent(t *testing.T) {
	const BufSize = 1000
	b := bufio.NewReaderSize(bytes.NewBufferString("hello world"), BufSize)
	// Does it recognize itself?
	b1 := bufio.NewReaderSize(b, BufSize)
	if b1 != b {
		t.Error("bufio.NewReaderSize did not detect underlying Reader")
	}
	// Does it wrap if existing buffer is too small?
	b2 := bufio.NewReaderSize(b, 2*BufSize)
	if b2 == b {
		t.Error("bufio.NewReaderSize did not enlarge buffer")
	}
}

func TestNewWriterSizeIdempotent(t *testing.T) {
	const BufSize = 1000
	b := NewAsyncWriterSize(new(bytes.Buffer), BufSize, defaultBufCount)
	// Does it recognize itself?
	b1 := NewAsyncWriterSize(b, BufSize, defaultBufCount)
	if b1 != b {
		t.Error("NewAsyncWriterSize did not detect underlying Writer")
	}
	// Does it wrap if existing buffer is too small?
	b2 := NewAsyncWriterSize(b, 2*BufSize, defaultBufCount)
	if b2 == b {
		t.Error("NewAsyncWriterSize did not enlarge buffer")
	}
}

func TestWriteString(t *testing.T) {
	const BufSize = 8
	buf := new(bytes.Buffer)
	b := NewAsyncWriterSize(buf, BufSize, defaultBufCount)
	b.WriteString("0")                         // easy
	b.WriteString("123456")                    // still easy
	b.WriteString("7890")                      // easy after flush
	b.WriteString("abcdefghijklmnopqrstuvwxy") // hard
	b.WriteString("z")
	if err := b.Flush(); err != nil {
		t.Error("WriteString", err)
	}
	s := "01234567890abcdefghijklmnopqrstuvwxyz"
	if string(buf.Bytes()) != s {
		t.Errorf("WriteString wants %q gets %q", s, string(buf.Bytes()))
	}
}

func TestBufferFull(t *testing.T) {
	const longString = "And now, hello, world! It is the time for all good men to come to the aid of their party"
	buf := bufio.NewReaderSize(strings.NewReader(longString), minReadBufferSize)
	line, err := buf.ReadSlice('!')
	if string(line) != "And now, hello, " || err != bufio.ErrBufferFull {
		t.Errorf("first ReadSlice(,) = %q, %v", line, err)
	}
	line, err = buf.ReadSlice('!')
	if string(line) != "world!" || err != nil {
		t.Errorf("second ReadSlice(,) = %q, %v", line, err)
	}
}

func TestPeek(t *testing.T) {
	p := make([]byte, 10)
	// string is 16 (minReadBufferSize) long.
	buf := bufio.NewReaderSize(strings.NewReader("abcdefghijklmnop"), minReadBufferSize)
	if s, err := buf.Peek(1); string(s) != "a" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "a", string(s), err)
	}
	if s, err := buf.Peek(4); string(s) != "abcd" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "abcd", string(s), err)
	}
	if _, err := buf.Peek(32); err != bufio.ErrBufferFull {
		t.Fatalf("want ErrBufFull got %v", err)
	}
	if _, err := buf.Read(p[0:3]); string(p[0:3]) != "abc" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "abc", string(p[0:3]), err)
	}
	if s, err := buf.Peek(1); string(s) != "d" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "d", string(s), err)
	}
	if s, err := buf.Peek(2); string(s) != "de" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "de", string(s), err)
	}
	if _, err := buf.Read(p[0:3]); string(p[0:3]) != "def" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "def", string(p[0:3]), err)
	}
	if s, err := buf.Peek(4); string(s) != "ghij" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "ghij", string(s), err)
	}
	if _, err := buf.Read(p[0:]); string(p[0:]) != "ghijklmnop" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "ghijklmnop", string(p[0:minReadBufferSize]), err)
	}
	if s, err := buf.Peek(0); string(s) != "" || err != nil {
		t.Fatalf("want %q got %q, err=%v", "", string(s), err)
	}
	if _, err := buf.Peek(1); err != io.EOF {
		t.Fatalf("want EOF got %v", err)
	}

	// Test for issue 3022, not exposing a reader's error on a successful Peek.
	buf = bufio.NewReaderSize(dataAndEOFReader("abcd"), 32)
	if s, err := buf.Peek(2); string(s) != "ab" || err != nil {
		t.Errorf(`Peek(2) on "abcd", EOF = %q, %v; want "ab", nil`, string(s), err)
	}
	if s, err := buf.Peek(4); string(s) != "abcd" || err != nil {
		t.Errorf(`Peek(4) on "abcd", EOF = %q, %v; want "abcd", nil`, string(s), err)
	}
	if n, err := buf.Read(p[0:5]); string(p[0:n]) != "abcd" || err != nil {
		t.Fatalf("Read after peek = %q, %v; want abcd, EOF", p[0:n], err)
	}
	if n, err := buf.Read(p[0:1]); string(p[0:n]) != "" || err != io.EOF {
		t.Fatalf(`second Read after peek = %q, %v; want "", EOF`, p[0:n], err)
	}
}

type dataAndEOFReader string

func (r dataAndEOFReader) Read(p []byte) (int, error) {
	return copy(p, r), io.EOF
}

func TestPeekThenUnreadRune(t *testing.T) {
	// This sequence used to cause a crash.
	r := bufio.NewReader(strings.NewReader("x"))
	r.ReadRune()
	r.Peek(1)
	r.UnreadRune()
	r.ReadRune() // Used to panic here
}

var testOutput = []byte("0123456789abcdefghijklmnopqrstuvwxy")
var testInput = []byte("012\n345\n678\n9ab\ncde\nfgh\nijk\nlmn\nopq\nrst\nuvw\nxy")
var testInputrn = []byte("012\r\n345\r\n678\r\n9ab\r\ncde\r\nfgh\r\nijk\r\nlmn\r\nopq\r\nrst\r\nuvw\r\nxy\r\n\n\r\n")

// TestReader wraps a []byte and returns reads of a specific length.
type testReader struct {
	data   []byte
	stride int
}

func (t *testReader) Read(buf []byte) (n int, err error) {
	n = t.stride
	if n > len(t.data) {
		n = len(t.data)
	}
	if n > len(buf) {
		n = len(buf)
	}
	copy(buf, t.data)
	t.data = t.data[n:]
	if len(t.data) == 0 {
		err = io.EOF
	}
	return
}

func testReadLine(t *testing.T, input []byte) {
	//for stride := 1; stride < len(input); stride++ {
	for stride := 1; stride < 2; stride++ {
		done := 0
		reader := testReader{input, stride}
		l := bufio.NewReaderSize(&reader, len(input)+1)
		for {
			line, isPrefix, err := l.ReadLine()
			if len(line) > 0 && err != nil {
				t.Errorf("ReadLine returned both data and error: %s", err)
			}
			if isPrefix {
				t.Errorf("ReadLine returned prefix")
			}
			if err != nil {
				if err != io.EOF {
					t.Fatalf("Got unknown error: %s", err)
				}
				break
			}
			if want := testOutput[done : done+len(line)]; !bytes.Equal(want, line) {
				t.Errorf("Bad line at stride %d: want: %x got: %x", stride, want, line)
			}
			done += len(line)
		}
		if done != len(testOutput) {
			t.Errorf("ReadLine didn't return everything: got: %d, want: %d (stride: %d)", done, len(testOutput), stride)
		}
	}
}

func TestReadLine(t *testing.T) {
	testReadLine(t, testInput)
	testReadLine(t, testInputrn)
}

func TestLineTooLong(t *testing.T) {
	data := make([]byte, 0)
	for i := 0; i < minReadBufferSize*5/2; i++ {
		data = append(data, '0'+byte(i%10))
	}
	buf := bytes.NewBuffer(data)
	l := bufio.NewReaderSize(buf, minReadBufferSize)
	line, isPrefix, err := l.ReadLine()
	if !isPrefix || !bytes.Equal(line, data[:minReadBufferSize]) || err != nil {
		t.Errorf("bad result for first line: got %q want %q %v", line, data[:minReadBufferSize], err)
	}
	data = data[len(line):]
	line, isPrefix, err = l.ReadLine()
	if !isPrefix || !bytes.Equal(line, data[:minReadBufferSize]) || err != nil {
		t.Errorf("bad result for second line: got %q want %q %v", line, data[:minReadBufferSize], err)
	}
	data = data[len(line):]
	line, isPrefix, err = l.ReadLine()
	if isPrefix || !bytes.Equal(line, data[:minReadBufferSize/2]) || err != nil {
		t.Errorf("bad result for third line: got %q want %q %v", line, data[:minReadBufferSize/2], err)
	}
	line, isPrefix, err = l.ReadLine()
	if isPrefix || err == nil {
		t.Errorf("expected no more lines: %x %s", line, err)
	}
}

func TestReadAfterLines(t *testing.T) {
	line1 := "this is line1"
	restData := "this is line2\nthis is line 3\n"
	inbuf := bytes.NewBuffer([]byte(line1 + "\n" + restData))
	outbuf := new(bytes.Buffer)
	maxLineLength := len(line1) + len(restData)/2
	l := bufio.NewReaderSize(inbuf, maxLineLength)
	line, isPrefix, err := l.ReadLine()
	if isPrefix || err != nil || string(line) != line1 {
		t.Errorf("bad result for first line: isPrefix=%v err=%v line=%q", isPrefix, err, string(line))
	}
	n, err := io.Copy(outbuf, l)
	if int(n) != len(restData) || err != nil {
		t.Errorf("bad result for Read: n=%d err=%v", n, err)
	}
	if outbuf.String() != restData {
		t.Errorf("bad result for Read: got %q; expected %q", outbuf.String(), restData)
	}
}

func TestReadEmptyBuffer(t *testing.T) {
	l := bufio.NewReaderSize(new(bytes.Buffer), minReadBufferSize)
	line, isPrefix, err := l.ReadLine()
	if err != io.EOF {
		t.Errorf("expected EOF from ReadLine, got '%s' %t %s", line, isPrefix, err)
	}
}

func TestLinesAfterRead(t *testing.T) {
	l := bufio.NewReaderSize(bytes.NewBuffer([]byte("foo")), minReadBufferSize)
	_, err := ioutil.ReadAll(l)
	if err != nil {
		t.Error(err)
		return
	}

	line, isPrefix, err := l.ReadLine()
	if err != io.EOF {
		t.Errorf("expected EOF from ReadLine, got '%s' %t %s", line, isPrefix, err)
	}
}

func TestReadLineNonNilLineOrError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("line 1\n"))
	for i := 0; i < 2; i++ {
		l, _, err := r.ReadLine()
		if l != nil && err != nil {
			t.Fatalf("on line %d/2; ReadLine=%#v, %v; want non-nil line or Error, but not both",
				i+1, l, err)
		}
	}
}

type readLineResult struct {
	line     []byte
	isPrefix bool
	err      error
}

var readLineNewlinesTests = []struct {
	input  string
	expect []readLineResult
}{
	{"012345678901234\r\n012345678901234\r\n", []readLineResult{
		{[]byte("012345678901234"), true, nil},
		{nil, false, nil},
		{[]byte("012345678901234"), true, nil},
		{nil, false, nil},
		{nil, false, io.EOF},
	}},
	{"0123456789012345\r012345678901234\r", []readLineResult{
		{[]byte("0123456789012345"), true, nil},
		{[]byte("\r012345678901234"), true, nil},
		{[]byte("\r"), false, nil},
		{nil, false, io.EOF},
	}},
}

func TestReadLineNewlines(t *testing.T) {
	for _, e := range readLineNewlinesTests {
		testReadLineNewlines(t, e.input, e.expect)
	}
}

func testReadLineNewlines(t *testing.T, input string, expect []readLineResult) {
	b := bufio.NewReaderSize(strings.NewReader(input), minReadBufferSize)
	for i, e := range expect {
		line, isPrefix, err := b.ReadLine()
		if !bytes.Equal(line, e.line) {
			t.Errorf("%q call %d, line == %q, want %q", input, i, line, e.line)
			return
		}
		if isPrefix != e.isPrefix {
			t.Errorf("%q call %d, isPrefix == %v, want %v", input, i, isPrefix, e.isPrefix)
			return
		}
		if err != e.err {
			t.Errorf("%q call %d, err == %v, want %v", input, i, err, e.err)
			return
		}
	}
}

func createTestInput(n int) []byte {
	input := make([]byte, n)
	for i := range input {
		// 101 and 251 are arbitrary prime numbers.
		// The idea is to create an input sequence
		// which doesn't repeat too frequently.
		input[i] = byte(i % 251)
		if i%101 == 0 {
			input[i] ^= byte(i / 101)
		}
	}
	return input
}

func TestReaderWriteTo(t *testing.T) {
	input := createTestInput(8192)
	r := bufio.NewReader(onlyReader{bytes.NewBuffer(input)})
	w := new(bytes.Buffer)
	if n, err := r.WriteTo(w); err != nil || n != int64(len(input)) {
		t.Fatalf("r.WriteTo(w) = %d, %v, want %d, nil", n, err, len(input))
	}

	for i, val := range w.Bytes() {
		if val != input[i] {
			t.Errorf("after write: out[%d] = %#x, want %#x", i, val, input[i])
		}
	}
}

type errorWriterToTest struct {
	rn, wn     int
	rerr, werr error
	expected   error
}

func (r errorWriterToTest) Read(p []byte) (int, error) {
	return len(p) * r.rn, r.rerr
}

func (w errorWriterToTest) Write(p []byte) (int, error) {
	return len(p) * w.wn, w.werr
}

var errorWriterToTests = []errorWriterToTest{
	{1, 0, nil, io.ErrClosedPipe, io.ErrClosedPipe},
	{0, 1, io.ErrClosedPipe, nil, io.ErrClosedPipe},
	{0, 0, io.ErrUnexpectedEOF, io.ErrClosedPipe, io.ErrClosedPipe},
	{0, 1, io.EOF, nil, nil},
}

func TestReaderWriteToErrors(t *testing.T) {
	for i, rw := range errorWriterToTests {
		r := bufio.NewReader(rw)
		if _, err := r.WriteTo(rw); err != rw.expected {
			t.Errorf("r.WriteTo(errorWriterToTests[%d]) = _, %v, want _,%v", i, err, rw.expected)
		}
	}
}

func TestWriterReadFrom(t *testing.T) {
	ws := []func(io.Writer) io.Writer{
		func(w io.Writer) io.Writer { return onlyWriter{w} },
		func(w io.Writer) io.Writer { return w },
	}

	rs := []func(io.Reader) io.Reader{
		iotest.DataErrReader,
		func(r io.Reader) io.Reader { return r },
	}

	for ri, rfunc := range rs {
		for wi, wfunc := range ws {
			input := createTestInput(8192)
			b := new(bytes.Buffer)
			w := NewAsyncWriter(wfunc(b))
			r := rfunc(bytes.NewBuffer(input))
			if n, err := w.ReadFrom(r); err != nil || n != int64(len(input)) {
				t.Errorf("ws[%d],rs[%d]: w.ReadFrom(r) = %d, %v, want %d, nil", wi, ri, n, err, len(input))
				continue
			}
			w.WaitForWrites()
			if got, want := b.String(), string(input); got != want {
				t.Errorf("ws[%d], rs[%d]:\ngot  %q\nwant %q\n", wi, ri, got, want)
			}
		}
	}
}

type errorReaderFromTest struct {
	rn, wn     int
	rerr, werr error
	expected   error
}

func (r errorReaderFromTest) Read(p []byte) (int, error) {
	return len(p) * r.rn, r.rerr
}

func (w errorReaderFromTest) Write(p []byte) (int, error) {
	return len(p) * w.wn, w.werr
}

var errorReaderFromTests = []errorReaderFromTest{
	{0, 1, io.EOF, nil, nil},
	{1, 1, io.EOF, nil, nil},
	{0, 1, io.ErrClosedPipe, nil, io.ErrClosedPipe},
	{0, 0, io.ErrClosedPipe, io.ErrShortWrite, io.ErrClosedPipe},
	{1, 0, nil, io.ErrShortWrite, io.ErrShortWrite},
}

func TestWriterReadFromErrors(t *testing.T) {
	for i, rw := range errorReaderFromTests {
		w := NewAsyncWriter(rw)
		if _, err := w.ReadFrom(rw); err != rw.expected {
			t.Errorf("w.ReadFrom(errorReaderFromTests[%d]) = _, %v, want _,%v", i, err, rw.expected)
		}
	}
}

// TestWriterReadFromCounts tests that using io.Copy to copy into a
// bufio.Writer does not prematurely flush the buffer. For example, when
// buffering writes to a network socket, excessive network writes should be
// avoided.
func TestWriterReadFromCounts(t *testing.T) {
	var w0 writeCountingDiscard
	b0 := NewAsyncWriterSize(&w0, 1234, defaultBufCount)
	b0.WriteString(strings.Repeat("x", 1000))
	if w0 != 0 {
		t.Fatalf("write 1000 'x's: got %d writes, want 0", w0)
	}
	b0.WriteString(strings.Repeat("x", 200))
	if w0 != 0 {
		t.Fatalf("write 1200 'x's: got %d writes, want 0", w0)
	}
	io.Copy(b0, onlyReader{strings.NewReader(strings.Repeat("x", 30))})
	if w0 != 0 {
		t.Fatalf("write 1230 'x's: got %d writes, want 0", w0)
	}
	io.Copy(b0, onlyReader{strings.NewReader(strings.Repeat("x", 9))})
	b0.WaitForWrites()
	if w0 != 1 {
		t.Fatalf("write 1239 'x's: got %d writes, want 1", w0)
	}

	var w1 writeCountingDiscard
	b1 := NewAsyncWriterSize(&w1, 1234, defaultBufCount)
	b1.WriteString(strings.Repeat("x", 1200))
	b1.Flush()
	if w1 != 1 {
		t.Fatalf("flush 1200 'x's: got %d writes, want 1", w1)
	}
	b1.WriteString(strings.Repeat("x", 89))
	if w1 != 1 {
		t.Fatalf("write 1200 + 89 'x's: got %d writes, want 1", w1)
	}
	io.Copy(b1, onlyReader{strings.NewReader(strings.Repeat("x", 700))})
	if w1 != 1 {
		t.Fatalf("write 1200 + 789 'x's: got %d writes, want 1", w1)
	}
	io.Copy(b1, onlyReader{strings.NewReader(strings.Repeat("x", 600))})
	b1.WaitForWrites()
	if w1 != 2 {
		t.Fatalf("write 1200 + 1389 'x's: got %d writes, want 2", w1)
	}
	b1.Flush()
	if w1 != 3 {
		t.Fatalf("flush 1200 + 1389 'x's: got %d writes, want 3", w1)
	}
}

// A writeCountingDiscard is like ioutil.Discard and counts the number of times
// Write is called on it.
type writeCountingDiscard int

func (w *writeCountingDiscard) Write(p []byte) (int, error) {
	*w++
	return len(p), nil
}

type negativeReader int

func (r *negativeReader) Read([]byte) (int, error) { return -1, nil }

func TestNegativeRead(t *testing.T) {
	// should panic with a description pointing at the reader, not at itself.
	// (should NOT panic with slice index error, for example.)
	b := bufio.NewReader(new(negativeReader))
	defer func() {
		switch err := recover().(type) {
		case nil:
			t.Fatal("read did not panic")
		case error:
			if !strings.Contains(err.Error(), "reader returned negative count from Read") {
				t.Fatalf("wrong panic: %v", err)
			}
		default:
			t.Fatalf("unexpected panic value: %T(%v)", err, err)
		}
	}()
	b.Read(make([]byte, 100))
}

// An onlyReader only implements io.Reader, no matter what other methods the underlying implementation may have.
type onlyReader struct {
	r io.Reader
}

func (r onlyReader) Read(b []byte) (int, error) {
	return r.r.Read(b)
}

// An onlyWriter only implements io.Writer, no matter what other methods the underlying implementation may have.
type onlyWriter struct {
	w io.Writer
}

func (w onlyWriter) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func BenchmarkReaderCopyOptimal(b *testing.B) {
	// Optimal case is where the underlying reader implements io.WriterTo
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := bufio.NewReader(bytes.NewBuffer(make([]byte, 8192)))
		dst := onlyWriter{new(bytes.Buffer)}
		b.StartTimer()
		io.Copy(dst, src)
	}
}

func BenchmarkReaderCopyUnoptimal(b *testing.B) {
	// Unoptimal case is where the underlying reader doesn't implement io.WriterTo
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := bufio.NewReader(onlyReader{bytes.NewBuffer(make([]byte, 8192))})
		dst := onlyWriter{new(bytes.Buffer)}
		b.StartTimer()
		io.Copy(dst, src)
	}
}

func BenchmarkReaderCopyNoWriteTo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := onlyReader{bufio.NewReader(bytes.NewBuffer(make([]byte, 8192)))}
		dst := onlyWriter{new(bytes.Buffer)}
		b.StartTimer()
		io.Copy(dst, src)
	}
}

func BenchmarkWriterCopyOptimal(b *testing.B) {
	// Optimal case is where the underlying writer implements io.ReaderFrom
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := onlyReader{bytes.NewBuffer(make([]byte, 8192))}
		dst := NewAsyncWriter(new(bytes.Buffer))
		b.StartTimer()
		io.Copy(dst, src)
	}
}

func BenchmarkWriterCopyUnoptimal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := onlyReader{bytes.NewBuffer(make([]byte, 8192))}
		dst := NewAsyncWriter(onlyWriter{new(bytes.Buffer)})
		b.StartTimer()
		io.Copy(dst, src)
	}
}

func BenchmarkWriterCopyNoReadFrom(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := onlyReader{bytes.NewBuffer(make([]byte, 8192))}
		dst := onlyWriter{NewAsyncWriter(new(bytes.Buffer))}
		b.StartTimer()
		io.Copy(dst, src)
	}
}

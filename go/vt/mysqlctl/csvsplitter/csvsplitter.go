package csvsplitter

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"code.google.com/p/vitess/go/vt/key"
)

type KeyspaceCSVReader struct {
	*bufio.Reader
	delim byte
}

func NewKeyspaceCSVReader(r io.Reader, delim byte) *KeyspaceCSVReader {
	return &KeyspaceCSVReader{Reader: bufio.NewReader(r), delim: delim}
}

// ReadRecord returns a keyspaceId and a line from which it was
// extracted, with the keyspaceId stripped.
func (r KeyspaceCSVReader) ReadRecord() (err error, keyspaceId key.KeyspaceId, line []byte) {
	k, err := r.ReadString(r.delim)
	if err != nil {
		return err, key.KeyspaceId(""), []byte{}
	}
	kid, err := strconv.ParseUint(k[:len(k)-1], 10, 64)
	if err != nil {
		return err, key.KeyspaceId(""), []byte{}
	}
	keyspaceId = key.Uint64Key(kid).KeyspaceId()

	buffer := bytes.NewBuffer([]byte{})
	escaped := false
	inQuote := false
	for {
		b, err := r.ReadByte()
		if err != nil {
			// Assumption: the csv file ends with a
			// newline. Otherwise io.EOF should be treated
			// separately.
			return err, key.KeyspaceId(""), []byte{}
		}

		buffer.WriteByte(b)

		if escaped {
			escaped = false
			continue
		}
		switch b {
		case '\\':
			escaped = true
		case '"':
			inQuote = !inQuote
		case '\n':
			if !inQuote {
				return nil, keyspaceId, buffer.Bytes()
			}
		}
	}
	panic("unreachable")
}

// Iterator returns an iterator that yields all lines in r.
func (r KeyspaceCSVReader) Iterator() iterator {
	return iterator{r: r}
}

type iterator struct {
	r          KeyspaceCSVReader
	KeyspaceId key.KeyspaceId
	Line       []byte
	Error      error
}

// Next advances the iterator and returns true as long as there are
// items it can yield and no errors have occurred. i.KeyspaceId will
// contain the keyspac id, and i.Line the line itself (with the
// keyspace id part stripped). You should check i.Error for any errors
// that might have occurred.
func (i *iterator) Next() bool {
	err, k, line := i.r.ReadRecord()

	if err != nil {
		if err != io.EOF {
			i.Error = err
		}
		return false
	}

	i.KeyspaceId = k
	i.Line = line

	return true
}

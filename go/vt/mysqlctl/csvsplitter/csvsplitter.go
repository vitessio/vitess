package csvsplitter

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"code.google.com/p/vitess/go/vt/key"
)

type KeyspaceCSVReader struct {
	reader *bufio.Reader
	delim  byte
	buf    *bytes.Buffer
}

func NewKeyspaceCSVReader(r io.Reader, delim byte) *KeyspaceCSVReader {
	return &KeyspaceCSVReader{reader: bufio.NewReader(r), delim: delim, buf: bytes.NewBuffer(make([]byte, 0, 1024))}
}

// ReadRecord returns a keyspaceId and a line from which it was
// extracted, with the keyspaceId stripped.
func (r KeyspaceCSVReader) ReadRecord() (keyspaceId key.KeyspaceId, line []byte, err error) {
	k, err := r.reader.ReadString(r.delim)
	if err != nil {
		return key.MinKey, nil, err
	}
	kid, err := strconv.ParseUint(k[:len(k)-1], 10, 64)
	if err != nil {
		return key.MinKey, nil, err
	}
	keyspaceId = key.Uint64Key(kid).KeyspaceId()

	defer r.buf.Reset()

	escaped := false
	inQuote := false
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			// Assumption: the csv file ends with a
			// newline. Otherwise io.EOF should be treated
			// separately.
			return key.MinKey, nil, err
		}

		r.buf.WriteByte(b)

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
				return keyspaceId, r.buf.Bytes(), nil
			}
		}
	}
	panic("unreachable")
}

package csvsplitter

import (
	"bufio"
	"bytes"
	"io"
)

type CSVReader struct {
	reader *bufio.Reader
	delim  byte
	buf    *bytes.Buffer
}

func NewCSVReader(r io.Reader, delim byte) *CSVReader {
	return &CSVReader{
		reader: bufio.NewReader(r),
		delim:  delim,
		buf:    bytes.NewBuffer(make([]byte, 0, 1024)),
	}
}

// ReadRecord returns a keyspaceId and a line from which it was
// extracted, with the keyspaceId stripped.
func (r CSVReader) ReadRecord() (line []byte, err error) {
	defer r.buf.Reset()

	escaped := false
	inQuote := false
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			// Assumption: the csv file ends with a
			// newline. Otherwise io.EOF should be treated
			// separately.
			return nil, err
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
				return r.buf.Bytes(), nil
			}
		}
	}
}

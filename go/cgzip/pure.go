// +build !cgo

// A slower, pure go alternative to cgzip to allow for cross compilation.

package cgzip

import (
	"compress/gzip"
	"hash/adler32"
	"hash/crc32"
)

// Writer is an io.WriteCloser. Writes to a Writer are compressed.
type Writer = gzip.Writer

var (
	Z_BEST_SPEED   = gzip.BestSpeed
	NewWriterLevel = gzip.NewWriterLevel
	NewReader      = gzip.NewReader
	NewCrc32       = crc32.NewIEEE
	NewAdler32     = adler32.New
)

package grpcclient

import (
	"flag"
	"io"

	"github.com/golang/snappy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

var (
	compression = flag.String("grpc_compression", "", "how to compress gRPC, default: nothing, supported: snappy")
)

// SnappyCompressor is a gRPC compressor using the Snappy algorithm.
type SnappyCompressor struct{}

// Name is "snappy"
func (s SnappyCompressor) Name() string {
	return "snappy"
}

// Compress wraps with a SnappyReader
func (s SnappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return snappy.NewBufferedWriter(w), nil
}

// Decompress wraps with a SnappyReader
func (s SnappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	return snappy.NewReader(r), nil
}

func appendCompression(opts []grpc.DialOption) ([]grpc.DialOption, error) {
	if *compression == "snappy" {
		compression := grpc.WithDefaultCallOptions(grpc.UseCompressor("snappy"))
		opts = append(opts, compression)
	}

	return opts, nil
}

func init() {
	encoding.RegisterCompressor(SnappyCompressor{})
	RegisterGRPCDialOptions(appendCompression)
}

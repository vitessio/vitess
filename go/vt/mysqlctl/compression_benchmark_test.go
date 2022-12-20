package mysqlctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
)

type (
	benchmarkCompressArgs struct {
		b        *testing.B
		builtin  string
		external string
	}

	benchmarkCompressEnv struct {
		benchmarkCompressArgs
	}
)

const (
	usage = `
This benchmark tests the performance of built-in and external compressors,
using mysqlctl.newBuiltinDecompressor and mysqlctl.newExternalCompressor
to obtain compressors.

You must supply a file to compress by setting this environment variable:

    export VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_PATH=path/to/data.xml

We recommend using a data set that compresses well, such as:

 - The enwik9 dataset: http://mattmahoney.net/dc/textdata
 - A larger Wikipedia dataset from: https://en.wikipedia.org/wiki/Wikipedia:Database_download

Once you've set that environment variable, you can run the tests like this:

    go test -bench=BenchmarkCompressZstd ./go/vt/mysqlctl -run=NONE -timeout=2h -benchtime=300s

Set -timeout and -benchtime to taste (or don't set them at all).
`

	userDefinedDataPathEnvVar = "VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_PATH"
)

func setupBenchmarkCompressEnv(args benchmarkCompressArgs) benchmarkCompressEnv {
	bce := benchmarkCompressEnv{
		benchmarkCompressArgs: args,
	}
	bce.validate()
	return bce
}

func (bce *benchmarkCompressEnv) benchmark() {
	bce.b.StopTimer()
	bce.b.ResetTimer()

	for i := 0; i < bce.b.N; i++ {
		logger := logutil.NewMemoryLogger()

		compressor := bce.compressor(logger)
		defer compressor.Close()

		reader := bce.reader()
		defer reader.Close()

		// Only time the part we care about.
		bce.b.StartTimer()
		_, err := io.Copy(compressor, reader)
		bce.b.StopTimer()

		require.Nil(bce.b, err, logger.Events)
	}
}

func (bce *benchmarkCompressEnv) compressor(logger logutil.Logger) io.WriteCloser {
	var compressor io.WriteCloser
	var err error

	if bce.builtin != "" {
		compressor, err = newBuiltinCompressor(bce.builtin, io.Discard, logger)
	} else if bce.external != "" {
		compressor, err = newExternalCompressor(context.Background(), bce.external, io.Discard, logger)
	}

	require.Nil(bce.b, err, "Failed to create compressor.")
	return compressor
}

func (bce *benchmarkCompressEnv) reader() io.ReadCloser {
	dataPath := os.Getenv(userDefinedDataPathEnvVar)
	f, err := os.Open(dataPath)
	require.Nil(bce.b, err, "Failed to open data path: %s", dataPath)
	return f
}

func (bce *benchmarkCompressEnv) validate() {
	if bce.external != "" {
		cmdArgs := strings.Split(bce.external, " ")

		_, err := validateExternalCmd(cmdArgs[0])
		if err != nil {
			bce.b.Skipf("Command %q not available in this host: %v; skipping...", cmdArgs[0], err)
		}
	}

	if bce.builtin == "" && bce.external == "" {
		require.Fail(bce.b, "Either builtin or external compressor must be specified.")
	}

	dataPath := os.Getenv(userDefinedDataPathEnvVar)
	if dataPath == "" {
		require.Fail(bce.b, fmt.Sprintf(
			"Required environment variable %q is not set.\n\nUSAGE:\n%s",
			userDefinedDataPathEnvVar,
			usage,
		))
	}
}

func BenchmarkCompressLz4Builtin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: Lz4Compressor,
	})
	env.benchmark()
}

func BenchmarkCompressPargzipBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: PargzipCompressor,
	})
	env.benchmark()
}

func BenchmarkCompressPgzipBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: PgzipCompressor,
	})
	env.benchmark()
}

func BenchmarkCompressZstdBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: ZstdCompressor,
	})
	env.benchmark()
}

func BenchmarkCompressZstdExternal(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d", compressionLevel),
	})
	env.benchmark()
}

func BenchmarkCompressZstdExternalFast4(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d --fast=4", compressionLevel),
	})
	env.benchmark()
}

func BenchmarkCompressZstdExternalT0(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -T0", compressionLevel),
	})
	env.benchmark()
}

func BenchmarkCompressZstdExternalT4(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -T4", compressionLevel),
	})
	env.benchmark()
}

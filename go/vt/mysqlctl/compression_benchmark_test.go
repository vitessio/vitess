package mysqlctl

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
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
		dataLocalPath string
		dataURL       string
	}
)

const (
	// This is the default file which will be downloaded, gunzipped, and used
	// by the compression benchmarks in this suite. It's a ~60GB gzipped InnoDB
	// file, which was built from this Wikipedia dataset:
	//
	// https://dumps.wikimedia.org/enwiki/20221220/enwiki-20221220-externallinks.sql.gz
	defaultDataURL = "https://archive.org/download/enwiki-20221220-externallinks.ibd/enwiki-20221220-externallinks.ibd.gz"

	// Users may specify an alternate gzipped URL. This option is intended
	// purely for development and debugging purposes. For example:
	//
	//     export VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_URL=https://wiki.mozilla.org/images/f/ff/Example.json.gz
	userDefinedDataURLEnvVar = "VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_URL"
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
		reader, err := bce.reader()
		require.Nil(bce.b, err, "Failed to get data reader.")

		// Only time the part we care about.
		bce.b.StartTimer()
		_, err = io.Copy(compressor, reader)
		bce.b.StopTimer()

		// Don't defer closing, otherwise we can exhaust open file limit.
		reader.Close()
		compressor.Close()
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

func (bce *benchmarkCompressEnv) reader() (io.ReadCloser, error) {
	url := defaultDataURL

	// Use user-defined URL, if specified.
	if udURL := os.Getenv(userDefinedDataURLEnvVar); udURL != "" {
		url = udURL
	}

	// Compute a local path for a file by hashing the URL.
	localPath := path.Join(os.TempDir(), fmt.Sprintf("%x.dat", md5.Sum([]byte(url))))

	if _, err := os.Stat(localPath); errors.Is(err, os.ErrNotExist) {
		bce.b.Logf("File not found at local path: %s\n", localPath)

		// If the local path does not exist, download the file from the URL.
		httpClient := http.Client{
			CheckRedirect: func(r *http.Request, via []*http.Request) error {
				r.URL.Opaque = r.URL.Path
				return nil
			},
		}

		resp, err := httpClient.Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to get data at URL %q: %v", url, err)
		}
		defer resp.Body.Close()

		// Assume the data we're downloading is gzipped.
		gzr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to gunzip data at URL %q: %v", url, err)
		}
		defer gzr.Close()

		// Create a local file to write the HTTP response to.
		file, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		// Write the gunzipped  HTTP response to local path.
		if _, err := io.Copy(file, gzr); err != nil {
			return nil, err
		}
	}

	return os.Open(localPath)
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

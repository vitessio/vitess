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
		b          *testing.B
		builtin    string
		external   string
		saveToFile bool
	}

	benchmarkCompressEnv struct {
		benchmarkCompressArgs
		dataLocalPath string
		dataURL       string
	}

	nopWriteCloser struct {
		io.Writer
	}

	meteredReader struct {
		count int64
		r     io.Reader
	}

	meteredWriter struct {
		after  func()
		before func()
		count  int64
		w      io.Writer
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

func discardWriteCloser() io.WriteCloser {
	return &nopWriteCloser{io.Discard}
}

func (dwc *nopWriteCloser) Close() error {
	return nil
}

func setupBenchmarkCompressEnv(args benchmarkCompressArgs) benchmarkCompressEnv {
	bce := benchmarkCompressEnv{
		benchmarkCompressArgs: args,
	}
	bce.validate()
	return bce
}

func (bce *benchmarkCompressEnv) compress() {
	var numUncompressedBytes, numCompressedBytes int64

	bce.b.StopTimer()
	bce.b.ResetTimer()

	for i := 0; i < bce.b.N; i++ {
		logger := logutil.NewMemoryLogger()

		// Create writer (to file or /dev/null).
		w := bce.writer()
		mw := &meteredWriter{w: w}

		// Create c.
		c := bce.compressor(logger, mw)
		mc := &meteredWriter{
			before: bce.b.StartTimer,
			after:  bce.b.StopTimer,
			w:      c,
		}

		r, err := bce.reader()
		require.Nil(bce.b, err, "Failed to get data reader.")
		mr := &meteredReader{r: r}

		_, err = io.Copy(mc, mr)

		// Don't defer closing things, otherwise we can exhaust open file limit.
		r.Close()
		c.Close()
		w.Close()
		require.Nil(bce.b, err, logger.Events)

		// Report how many bytes we've compressed.
		bce.b.SetBytes(mr.count)

		// Record how many bytes compressed so we can report these later.
		numCompressedBytes += mw.count
		numUncompressedBytes += mr.count
	}

	// Report the average compression ratio (= <uncompressed>:<compressed>) achieved per-operation.
	//
	// Must report this outside of b.N loop because b.ReportMetric overrides
	// any previously reported value for the same unit.
	bce.b.ReportMetric(
		float64(numUncompressedBytes)/float64(numCompressedBytes),
		// By convention, units should end in /op.
		"compression-ratio/op",
	)
}

func (bce *benchmarkCompressEnv) compressor(logger logutil.Logger, writer io.Writer) io.WriteCloser {
	var compressor io.WriteCloser
	var err error

	if bce.builtin != "" {
		compressor, err = newBuiltinCompressor(bce.builtin, writer, logger)
	} else if bce.external != "" {
		compressor, err = newExternalCompressor(context.Background(), bce.external, writer, logger)
	}

	require.Nil(bce.b, err, "Failed to create compressor.")
	return compressor
}

func (bce *benchmarkCompressEnv) writer() io.WriteCloser {
	var f io.WriteCloser
	f = discardWriteCloser()
	if bce.saveToFile {
		p := path.Join(bce.b.TempDir(), fmt.Sprintf("%x.archive", bce.b.Name()))
		var err error
		f, err = os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		require.Nil(bce.b, err)
	}
	return f
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

func (mr *meteredReader) Read(p []byte) (nbytes int, err error) {
	nbytes, err = mr.r.Read(p)
	mr.count += int64(nbytes)
	return
}

func (mw *meteredWriter) Write(p []byte) (nbytes int, err error) {
	if mw.before != nil {
		mw.before()
	}
	if mw.after != nil {
		defer mw.after()
	}
	nbytes, err = mw.w.Write(p)
	mw.count += int64(nbytes)
	return
}

func BenchmarkCompressThenDiscardLz4Builtin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: Lz4Compressor,
	})
	env.compress()
}

func BenchmarkCompressThenDiscardPargzipBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: PargzipCompressor,
	})
	env.compress()
}

func BenchmarkCompressThenDiscardDiscardPgzipBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: PgzipCompressor,
	})
	env.compress()
}

func BenchmarkCompressThenDiscardZstdBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: ZstdCompressor,
	})
	env.compress()
}

func BenchmarkCompressThenDiscardZstdExternal(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressThenDiscardZstdExternalFast4(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d --fast=4 -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressThenDiscardZstdExternalT0(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -T0 -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressThenDiscardZstdExternalT4(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -T4 -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressToFileLz4Builtin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		builtin:    Lz4Compressor,
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFilePargzipBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		builtin:    PargzipCompressor,
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFileDiscardPgzipBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		builtin:    PgzipCompressor,
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFileZstdBuiltin(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		builtin:    ZstdCompressor,
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFileZstdExternal(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		external:   fmt.Sprintf("zstd -%d -c", compressionLevel),
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFileZstdExternalFast4(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		external:   fmt.Sprintf("zstd -%d --fast=4 -c", compressionLevel),
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFileZstdExternalT0(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		external:   fmt.Sprintf("zstd -%d -T0 -c", compressionLevel),
		saveToFile: true,
	})
	env.compress()
}

func BenchmarkCompressToFileZstdExternalT4(b *testing.B) {
	env := setupBenchmarkCompressEnv(benchmarkCompressArgs{
		b:          b,
		external:   fmt.Sprintf("zstd -%d -T4 -c", compressionLevel),
		saveToFile: true,
	})
	env.compress()
}

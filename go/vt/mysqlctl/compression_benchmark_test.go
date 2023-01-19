package mysqlctl

import (
	"bufio"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
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

	fnReadCloser struct {
		io.Reader
		closer func() error
	}

	meteredReader struct {
		count int64
		r     io.Reader
	}

	meteredWriter struct {
		count int64
		w     io.Writer
	}

	timedWriter struct {
		duration time.Duration
		w        io.Writer
	}
)

const (
	// This is the default file which will be downloaded, decompressed, and
	// used by the compression benchmarks in this suite. It's a ~1.5 GiB
	// compressed tar file containing 3 InnoDB files. The InnoDB files were
	// built from this Wikipedia dataset:
	//
	//     https://dumps.wikimedia.org/archive/enwiki/20080103/enwiki-20080103-pages-articles.xml.bz2
	defaultDataURL = "https://github.com/vitessio/vitess-resources/releases/download/testdata-v1.0/enwiki-20080103-pages-articles.ibd.tar.zst"

	// By default, don't limit how many bytes we input into compression.
	defaultMaxBytes int64 = 0

	// By default the benchmarks will remove any downloaded data after all
	// benchmarks are run, unless the data URL is a local path, in which case
	// it will be left alone.
	//
	// Users may override this behavior. This option is
	// intended purely for debugging purposes.
	//
	//     export VT_MYSQLCTL_COMPRESSION_BENCHMARK_CLEANUP=false
	envVarCleanup = "VT_MYSQLCTL_COMPRESSION_BENCHMARK_CLEANUP"

	// Users may specify an alternate gzipped URL. This option is intended
	// purely for development and debugging purposes. For example:
	//
	//     export VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_URL=https://wiki.mozilla.org/images/f/ff/Example.json.gz
	//
	// A local path can also be specified:
	//
	//     export VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_URL=file:///tmp/custom.dat
	envVarDataURL = "VT_MYSQLCTL_COMPRESSION_BENCHMARK_DATA_URL"

	// Users may override how many bytes are downloaded. This option is
	// intended purely for development and debugging purposes. For example:
	//
	//     export VT_MYSQLCTL_COMPRESSION_BENCHMARK_MAX_BYTES=256
	envVarMaxBytes = "VT_MYSQLCTL_COMPRESSION_BENCHMARK_MAX_BYTES"
)

func (frc *fnReadCloser) Close() error {
	return frc.closer()
}

func dataLocalPath(u *url.URL) string {
	if isLocal(u) {
		return u.Path
	}
	// Compute a local path for a file by hashing the URL.
	return path.Join(os.TempDir(), fmt.Sprintf("%x.dat", md5.Sum([]byte(u.String()))))
}

func dataURL() (*url.URL, error) {
	u := defaultDataURL

	// Use user-defined URL, if specified.
	if udURL := os.Getenv(envVarDataURL); udURL != "" {
		u = udURL
	}

	return url.Parse(u)
}

func downloadData(url, localPath string, maxBytes int64) error {
	var err error
	var rdr io.Reader

	// If the local path does not exist, download the file from the URL.
	httpClient := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to get data at URL %q: %v", url, err)
	}
	defer resp.Body.Close()
	rdr = resp.Body

	// Assume the data we're downloading is compressed with zstd.
	zr, err := zstd.NewReader(rdr)
	if err != nil {
		return fmt.Errorf("failed to decompress data at URL %q: %v", url, err)
	}
	defer zr.Close()
	rdr = zr

	if maxBytes > 0 {
		rdr = io.LimitReader(rdr, maxBytes)
	}

	// Create a local file to write the HTTP response to.
	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the decompressed data to local path.
	if _, err := io.Copy(file, rdr); err != nil {
		return err
	}

	return nil
}

func isHTTP(u *url.URL) bool {
	return u.Scheme == "http" || u.Scheme == "https"
}

func isLocal(u *url.URL) bool {
	return u.Scheme == "file" || (u.Scheme == "" && u.Hostname() == "")
}

func maxBytes() (int64, error) {
	// Limit how many bytes we unpack from the archive.
	mb := defaultMaxBytes

	// Use user-defined max bytes, if specified and valid.
	if udMaxBytes := os.Getenv(envVarMaxBytes); udMaxBytes != "" {
		udmb, err := strconv.ParseInt(udMaxBytes, 10, 64)
		if err != nil {
			return mb, err
		}
		mb = udmb
	}

	return mb, nil
}

func newBenchmarkCompressEnv(args benchmarkCompressArgs) benchmarkCompressEnv {
	bce := benchmarkCompressEnv{
		benchmarkCompressArgs: args,
	}
	bce.validate()
	bce.prepare()
	return bce
}

func shouldCleanup(u *url.URL) (bool, error) {
	c := true

	// Don't cleanup local paths provided by the user.
	if isLocal(u) {
		c = false
	}

	// Use user-defined cleanup, if specified and valid.
	if udCleanup := os.Getenv(envVarCleanup); udCleanup != "" {
		udc, err := strconv.ParseBool(udCleanup)
		if err != nil {
			return c, err
		}
		c = udc
	}

	return c, nil
}

func (bce *benchmarkCompressEnv) compress() {
	var durCompressed time.Duration
	var numUncompressedBytes, numCompressedBytes int64

	// The Benchmark, Reader and Writer interfaces make it difficult to time
	// compression without frequent calls to {Start,Stop}Timer or including
	// disk read/write times the measurement. Instead we'll use ReportMetric
	// after all loops are completed.
	bce.b.StopTimer()
	bce.b.ResetTimer()

	for i := 0; i < bce.b.N; i++ {
		logger := logutil.NewMemoryLogger()

		// Don't write anywhere. We're just interested in compression time.
		w := io.Discard

		// Keep track of how many compressed bytes come through.
		mw := &meteredWriter{w: w}

		// Create compressor.
		c := bce.compressor(logger, mw)

		// Time how long we spend on c.Write.
		tc := &timedWriter{w: c}

		r, err := bce.reader()
		require.Nil(bce.b, err, "Failed to get data reader.")

		// Track how many bytes we read.
		mr := &meteredReader{r: r}

		// It makes sense to use {Start,Stop}Timer here, but we're not
		// interested in how long it takes to read from disk.
		_, err = io.Copy(tc, mr)

		// Don't defer closing things, otherwise we can exhaust open file limit.
		r.Close()
		c.Close()

		require.Nil(bce.b, err, logger.Events)

		// Record how many bytes compressed so we can report these later.
		durCompressed += tc.duration
		numCompressedBytes += mw.count
		numUncompressedBytes += mr.count
	}

	bce.b.ReportMetric(
		float64(durCompressed.Nanoseconds()/int64(bce.b.N)),
		"ns/op",
	)

	mbOut := numUncompressedBytes / 1024 / 1024
	bce.b.ReportMetric(
		float64(mbOut)/durCompressed.Seconds(),
		"MB/s",
	)

	bce.b.ReportMetric(
		float64(numUncompressedBytes)/float64(numCompressedBytes),
		"compression-ratio",
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

	require.Nil(bce.b, err, "failed to create compressor.")
	return compressor
}

func (bce *benchmarkCompressEnv) prepare() {
	u, err := dataURL()
	require.NoError(bce.b, err, "failed to get data url")

	localPath := dataLocalPath(u)

	if isLocal(u) {
		if _, err := os.Stat(localPath); errors.Is(err, os.ErrNotExist) {
			require.Failf(bce.b, "local path does not exist", localPath)
		}
	} else if isHTTP(u) {
		if _, err := os.Stat(localPath); errors.Is(err, os.ErrNotExist) {
			mb, _ := maxBytes()
			bce.b.Logf("downloading data from %s", u.String())
			if err := downloadData(u.String(), localPath, mb); err != nil {
				require.Failf(bce.b, "failed to download data", err.Error())
			}
		}
	} else {
		require.Failf(bce.b, "don't know how to get data from url", u.String())
	}
}

func (bce *benchmarkCompressEnv) reader() (io.ReadCloser, error) {
	var r io.Reader

	u, _ := dataURL()

	f, err := os.Open(dataLocalPath(u))
	if err != nil {
		return nil, err
	}
	r = f

	mb, _ := maxBytes()
	if mb > 0 {
		r = io.LimitReader(f, mb)
	}

	buf := bufio.NewReaderSize(r, 2*1024*1024)
	return &fnReadCloser{buf, f.Close}, nil
}

func (bce *benchmarkCompressEnv) validate() {
	if bce.external != "" {
		cmdArgs := strings.Split(bce.external, " ")

		_, err := validateExternalCmd(cmdArgs[0])
		if err != nil {
			bce.b.Skipf("command %q not available in this host: %v; skipping...", cmdArgs[0], err)
		}
	}

	if bce.builtin == "" && bce.external == "" {
		require.Fail(bce.b, "either builtin or external compressor must be specified.")
	}
}

func (mr *meteredReader) Read(p []byte) (nbytes int, err error) {
	nbytes, err = mr.r.Read(p)
	mr.count += int64(nbytes)
	return
}

func (mw *meteredWriter) Write(p []byte) (nbytes int, err error) {
	nbytes, err = mw.w.Write(p)
	mw.count += int64(nbytes)
	return
}

func (tw *timedWriter) Write(p []byte) (nbytes int, err error) {
	start := time.Now()
	nbytes, err = tw.w.Write(p)
	tw.duration += time.Since(start)
	return
}

func TestMain(m *testing.M) {
	code := m.Run()

	u, _ := dataURL()
	localPath := dataLocalPath(u)

	cleanup, err := shouldCleanup(u)
	if cleanup {
		msg := "cleaning up %q"
		args := []any{localPath}

		if err != nil {
			args = append(args, err)
			msg = msg + "; %v"
		}

		fmt.Printf(msg+"\n", args...)
		if _, err := os.Stat(localPath); !errors.Is(err, os.ErrNotExist) {
			os.Remove(localPath)
		}
	}

	os.Exit(code)
}

func BenchmarkCompressLz4Builtin(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: Lz4Compressor,
	})
	env.compress()
}

func BenchmarkCompressPargzipBuiltin(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: PargzipCompressor,
	})
	env.compress()
}

func BenchmarkCompressPgzipBuiltin(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: PgzipCompressor,
	})
	env.compress()
}

func BenchmarkCompressZstdBuiltin(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:       b,
		builtin: ZstdCompressor,
	})
	env.compress()
}

func BenchmarkCompressZstdExternal(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressZstdExternalFast4(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d --fast=4 -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressZstdExternalT0(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -T0 -c", compressionLevel),
	})
	env.compress()
}

func BenchmarkCompressZstdExternalT4(b *testing.B) {
	env := newBenchmarkCompressEnv(benchmarkCompressArgs{
		b:        b,
		external: fmt.Sprintf("zstd -%d -T4 -c", compressionLevel),
	})
	env.compress()
}

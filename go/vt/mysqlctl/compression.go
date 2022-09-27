/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/google/shlex"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"
	"github.com/planetscale/pargzip"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	PgzipCompressor    = "pgzip"
	PargzipCompressor  = "pargzip"
	ZstdCompressor     = "zstd"
	Lz4Compressor      = "lz4"
	ExternalCompressor = "external"
)

var (
	compressionLevel = 1
	// CompressionEngineName specifies which compressor/decompressor to use
	CompressionEngineName = "pargzip"
	// ExternalCompressorCmd / ExternalDecompressorCmd specify the external commands compress/decompress the backups
	ExternalCompressorCmd   string
	ExternalCompressorExt   string
	ExternalDecompressorCmd string

	errUnsupportedDeCompressionEngine = errors.New("unsupported engine in MANIFEST. You need to provide --external-decompressor if using 'external' compression engine")
	errUnsupportedCompressionEngine   = errors.New("unsupported engine value for --compression-engine-name. supported values are 'external', 'pgzip', 'pargzip', 'zstd', 'lz4'")

	// this is used by getEngineFromExtension() to figure out which engine to use in case the user didn't specify
	engineExtensions = map[string][]string{
		".gz":  {PgzipCompressor, PargzipCompressor},
		".lz4": {Lz4Compressor},
		".zst": {ZstdCompressor},
	}
)

func init() {
	for _, cmd := range []string{"mysqlctl", "mysqlctld", "vtcombo", "vttablet", "vttestserver", "vtctld", "vtctldclient", "vtexplain"} {
		servenv.OnParseFor(cmd, registerBackupCompressionFlags)
	}
}

func registerBackupCompressionFlags(fs *pflag.FlagSet) {
	fs.IntVar(&compressionLevel, "compression-level", compressionLevel, "what level to pass to the compressor.")
	fs.StringVar(&CompressionEngineName, "compression-engine-name", CompressionEngineName, "compressor engine used for compression.")
	fs.StringVar(&ExternalCompressorCmd, "external-compressor", ExternalCompressorCmd, "command with arguments to use when compressing a backup.")
	fs.StringVar(&ExternalCompressorExt, "external-compressor-extension", ExternalCompressorExt, "extension to use when using an external compressor.")
	fs.StringVar(&ExternalDecompressorCmd, "external-decompressor", ExternalDecompressorCmd, "command with arguments to use when decompressing a backup.")
}

func getExtensionFromEngine(engine string) (string, error) {
	for ext, eng := range engineExtensions {
		for _, e := range eng {
			if e == engine {
				return ext, nil
			}
		}
	}
	return "", fmt.Errorf("%w %q", errUnsupportedCompressionEngine, engine)
}

// Validates if the external decompressor exists and return its path.
func validateExternalCmd(cmd string) (string, error) {
	if cmd == "" {
		return "", errors.New("external command is empty")
	}
	return exec.LookPath(cmd)
}

// Validate compression engine is one of the supported values.
func validateExternalCompressionEngineName(engine string) error {
	switch engine {
	case PgzipCompressor:
	case PargzipCompressor:
	case Lz4Compressor:
	case ZstdCompressor:
	case ExternalCompressor:
	default:
		return fmt.Errorf("%w value: %q", errUnsupportedCompressionEngine, engine)
	}

	return nil
}

func prepareExternalCmd(ctx context.Context, cmdStr string) (*exec.Cmd, error) {
	cmdArgs, err := shlex.Split(cmdStr)
	if err != nil {
		return nil, err
	}
	if len(cmdArgs) < 1 {
		return nil, errors.New("external command is empty")
	}
	cmdPath, err := validateExternalCmd(cmdArgs[0])
	if err != nil {
		return nil, err
	}
	return exec.CommandContext(ctx, cmdPath, cmdArgs[1:]...), nil
}

// This returns a writer that writes the compressed output of the external command to the provided writer.
func newExternalCompressor(ctx context.Context, cmdStr string, writer io.Writer, logger logutil.Logger) (io.WriteCloser, error) {
	logger.Infof("Compressing using external command: %q", cmdStr)
	// validate value of compression engine name
	if err := validateExternalCompressionEngineName(CompressionEngineName); err != nil {
		return nil, err
	}

	cmd, err := prepareExternalCmd(ctx, cmdStr)
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to start external command")
	}
	compressor := &externalCompressor{cmd: cmd}
	cmd.Stdout = writer
	cmdIn, err := cmd.StdinPipe()
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create external ompressor stdin pipe")
	}
	compressor.stdin = cmdIn
	cmdErr, err := cmd.StderrPipe()
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create external ompressor stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return nil, vterrors.Wrap(err, "can't start external decompressor")
	}

	compressor.wg.Add(1) // we wait for the gorouting to finish when we call Close() on the writer
	go scanLinesToLogger("compressor stderr", cmdErr, logger, compressor.wg.Done)
	return compressor, nil
}

// This returns a reader that reads the compressed input and passes it to the external command to be decompressed. Calls to its
// Read() will return the uncompressed data until EOF.
func newExternalDecompressor(ctx context.Context, cmdStr string, reader io.Reader, logger logutil.Logger) (io.ReadCloser, error) {
	logger.Infof("Decompressing using external command: %q", cmdStr)

	cmd, err := prepareExternalCmd(ctx, cmdStr)
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to start external command")
	}
	decompressor := &externalDecompressor{cmd: cmd}
	cmd.Stdin = reader
	cmdOut, err := cmd.StdoutPipe()
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create external decompressor stdout pipe")
	}
	decompressor.stdout = cmdOut
	cmdErr, err := cmd.StderrPipe()
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create external decompressor stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return nil, vterrors.Wrap(err, "can't start external decompressor")
	}

	decompressor.wg.Add(1) // we wait for the gorouting to finish when we call Close() on the reader
	go scanLinesToLogger("decompressor stderr", cmdErr, logger, decompressor.wg.Done)
	return decompressor, nil
}

// This returns a reader that will decompress the underlying provided reader and will use the specified supported engine.
func newBuiltinDecompressor(engine string, reader io.Reader, logger logutil.Logger) (decompressor io.ReadCloser, err error) {
	if engine == PargzipCompressor {
		logger.Warningf("engine \"pargzip\" doesn't support decompression, using \"pgzip\" instead")
		engine = PgzipCompressor
	}

	switch engine {
	case PgzipCompressor:
		d, err := pgzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		decompressor = d
	case "lz4":
		decompressor = io.NopCloser(lz4.NewReader(reader))
	case "zstd":
		d, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		decompressor = d.IOReadCloser()
	default:
		err = fmt.Errorf("Unkown decompressor engine: %q", engine)
		return decompressor, err
	}

	logger.Infof("Decompressing backup using engine %q", engine)
	return decompressor, err
}

// This returns a writer that will compress the data using the specified engine before writing to the underlying writer.
func newBuiltinCompressor(engine string, writer io.Writer, logger logutil.Logger) (compressor io.WriteCloser, err error) {
	switch engine {
	case PgzipCompressor:
		gzip, err := pgzip.NewWriterLevel(writer, compressionLevel)
		if err != nil {
			return compressor, vterrors.Wrap(err, "cannot create gzip compressor")
		}
		gzip.SetConcurrency(backupCompressBlockSize, backupCompressBlocks)
		compressor = gzip
	case PargzipCompressor:
		gzip := pargzip.NewWriter(writer)
		gzip.ChunkSize = backupCompressBlockSize
		gzip.Parallel = backupCompressBlocks
		gzip.CompressionLevel = compressionLevel
		compressor = gzip
	case Lz4Compressor:
		lz4Writer := lz4.NewWriter(writer).WithConcurrency(backupCompressBlocks)
		lz4Writer.Header = lz4.Header{
			CompressionLevel: compressionLevel,
		}
		compressor = lz4Writer
	case ZstdCompressor:
		zst, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevel(compressionLevel)))
		if err != nil {
			return compressor, vterrors.Wrap(err, "cannot create zstd compressor")
		}
		compressor = zst
	default:
		err = fmt.Errorf("%w value: %q", errUnsupportedCompressionEngine, engine)
		return compressor, err
	}

	logger.Infof("Compressing backup using engine %q", engine)
	return
}

// This struct wraps the underlying exec.Cmd and implements the io.WriteCloser interface.
type externalCompressor struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	wg    sync.WaitGroup
}

func (e *externalCompressor) Write(p []byte) (n int, err error) {
	return e.stdin.Write(p)
}

func (e *externalCompressor) Close() error {
	if err := e.stdin.Close(); err != nil {
		return err
	}

	// wait for the stderr to finish reading as well
	e.wg.Wait()
	return e.cmd.Wait()
}

// This struct wraps the underlying exec.Cmd and implements the io.ReadCloser interface.
type externalDecompressor struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	wg     sync.WaitGroup
}

func (e *externalDecompressor) Read(p []byte) (n int, err error) {
	return e.stdout.Read(p)
}

func (e *externalDecompressor) Close() error {
	// wait for the stderr to finish reading as well
	e.wg.Wait()

	// exec.Cmd.Wait() will also close the stdout pipe, so we don't need to call it directly
	return e.cmd.Wait()
}

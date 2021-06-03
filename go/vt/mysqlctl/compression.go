package mysqlctl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"
	"github.com/planetscale/pargzip"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	compressionLevel = flag.Int("compression_level", 1, "What level to pass to the compressor")

	errUnsupportedCompressionEngine    = errors.New("unsupported engine")
	errUnsupportedCompressionExtension = errors.New("unsupported extension")

	// this is use by getEngineFromExtension() to figure out which engine to use in case the user didn't specify
	engineExtensions = map[string][]string{
		".gz":  {"pgzip", "pargzip"},
		".lz4": {"lz4"},
		".zst": {"zstd"},
	}
)

func getEngineFromExtension(extension string) (string, error) {
	for ext, eng := range engineExtensions {
		if ext == extension {
			return eng[0], nil // we select the first supported engine in auto mode
		}
	}

	return "", fmt.Errorf("%w \"%s\"", errUnsupportedCompressionExtension, extension)
}

func getExtensionFromEngine(engine string) (string, error) {
	for ext, eng := range engineExtensions {
		for _, e := range eng {
			if e == engine {
				return ext, nil
			}
		}
	}

	return "", fmt.Errorf("%w \"%s\"", errUnsupportedCompressionEngine, engine)
}

// Validates if the external decompressor exists and return its path.
func validateExternalCmd(cmd string) (string, error) {
	if cmd == "" {
		return "", errors.New("external command is empty")
	}

	return exec.LookPath(cmd)
}

func prepareExternalCompressionCmd(ctx context.Context, cmdStr string) (*exec.Cmd, error) {
	cmdArgs := strings.Split(cmdStr, " ")
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
// It is important to wait on the WaitGroup provided to make sure that even after closing the writer,
// the command will have processed its input buffer, otherwise not all data might have been written to the target writer.
func newExternalCompressor(ctx context.Context, cmdStr string, writer io.Writer, wg *sync.WaitGroup, logger logutil.Logger) (compressor io.WriteCloser, err error) {
	logger.Infof("Compressing using external command: \"%s\"", cmdStr)

	cmd, err := prepareExternalCompressionCmd(ctx, cmdStr)
	if err != nil {
		return compressor, vterrors.Wrap(err, "unable to start external command")
	}

	cmd.Stdout = writer

	cmdIn, err := cmd.StdinPipe()
	if err != nil {
		return compressor, vterrors.Wrap(err, "cannot create external ompressor stdin pipe")
	}

	cmdErr, err := cmd.StderrPipe()
	if err != nil {
		return compressor, vterrors.Wrap(err, "cannot create external ompressor stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return compressor, vterrors.Wrap(err, "can't start external decompressor")
	}

	compressor = cmdIn

	wg.Add(2) // one for the logger, another one for the go func below
	go scanLinesToLogger("compressor stderr", cmdErr, logger, wg.Done)

	go func() {
		// log the exit status
		if err := cmd.Wait(); err != nil {
			logger.Errorf("external compressor failed: %v", err)
		}
		wg.Done()
	}()

	return
}

// This returns a reader that reads the compressed input and passes it to the external command to be decompressed. Calls to its
// Read() will return the uncompressed data until EOF.
func newExternalDecompressor(ctx context.Context, cmdStr string, reader io.Reader, logger logutil.Logger) (decompressor io.ReadCloser, err error) {
	var decompressorWg sync.WaitGroup

	logger.Infof("Decompressing using external command: \"%s\"", cmdStr)

	cmd, err := prepareExternalCompressionCmd(ctx, cmdStr)
	if err != nil {
		return decompressor, vterrors.Wrap(err, "unable to start external command")
	}

	cmd.Stdin = reader

	cmdOut, err := cmd.StdoutPipe()
	if err != nil {
		return decompressor, vterrors.Wrap(err, "cannot create external decompressor stdout pipe")
	}

	cmdErr, err := cmd.StderrPipe()
	if err != nil {
		return decompressor, vterrors.Wrap(err, "cannot create external decompressor stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return decompressor, vterrors.Wrap(err, "can't start external decompressor")
	}

	decompressorWg.Add(1)
	go scanLinesToLogger("decompressor stderr", cmdErr, logger, decompressorWg.Done)

	decompressor = cmdOut

	go func() {
		decompressorWg.Wait()
		// log the exit status
		if err := cmd.Wait(); err != nil {
			logger.Errorf("external compressor failed: %v", err)
		}
	}()

	return
}

// This returns a reader that will decompress the underlying provided reader and will use the specified supported engine (or
// try to detect which one to use based on the extension if the default "auto" is used.
func newBuiltinDecompressor(engine, extension string, reader io.Reader, logger logutil.Logger) (decompressor io.ReadCloser, err error) {
	if engine == "auto" {
		logger.Infof("Builtin decompressor set to auto, checking which engine to decompress based on the extension")

		eng, err := getEngineFromExtension(extension)
		if err != nil {
			return decompressor, err
		}

		engine = eng
	}

	switch engine {
	case "pgzip":
		d, err := pgzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		decompressor = d
	case "pargzip":
		err = errors.New("engine pargzip does not support decompression")
		return decompressor, err
	case "lz4":
		decompressor = ioutil.NopCloser(lz4.NewReader(reader))
	case "zstd":
		d, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}

		decompressor = d.IOReadCloser()
	default:
		err = fmt.Errorf("Unkown decompressor engine: \"%s\"", engine)
		return decompressor, err
	}

	logger.Infof("Decompressing backup using built-in engine \"%s\"", engine)

	return decompressor, err
}

// This return a writer that will compress the data using the specified engine before writing to the underlying writer.
func newBuiltinCompressor(engine string, writer io.Writer, logger logutil.Logger) (compressor io.WriteCloser, err error) {
	switch engine {
	case "pgzip":
		gzip, err := pgzip.NewWriterLevel(writer, *compressionLevel)
		if err != nil {
			return compressor, vterrors.Wrap(err, "cannot create gzip compressor")
		}

		gzip.SetConcurrency(*backupCompressBlockSize, *backupCompressBlocks)

		compressor = gzip
	case "pargzip":
		gzip := pargzip.NewWriter(writer)
		gzip.ChunkSize = *backupCompressBlockSize
		gzip.Parallel = *backupCompressBlocks
		gzip.CompressionLevel = *compressionLevel

		compressor = gzip
	case "lz4":
		lz4Writer := lz4.NewWriter(writer).WithConcurrency(*backupCompressBlocks)
		lz4Writer.Header = lz4.Header{
			CompressionLevel: *compressionLevel,
		}

		compressor = lz4Writer
	case "zstd":
		// compressor = zstd.NewWriterLevel(writer, *compressionLevel)
		zst, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevel(*compressionLevel)))
		if err != nil {
			return compressor, vterrors.Wrap(err, "cannot create zstd compressor")
		}

		compressor = zst
	default:
		err = fmt.Errorf("Unkown compressor engine: \"%s\"", engine)
		return compressor, err
	}

	logger.Infof("Compressing backup using built-in engine \"%s\"", engine)

	return
}

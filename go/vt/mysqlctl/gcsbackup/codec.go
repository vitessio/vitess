package gcsbackup

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/journal"
)

const (
	// Version is the current codec version.
	//
	// It is stored in the backup file header, the encoder
	// sets it and the decoder checks to see it is the same.
	version = 1
)

const (
	chunksize = 1 << 20 // 1mb
)

// Encrypter represents an encrypter.
type encrypter interface {
	// Encrypt encrypts the given plaintext.
	//
	// The method returns ciphertext or an error.
	encrypt(ctx context.Context, plaintext []byte) ([]byte, error)
}

// Decrypter represents a decrypter.
type decrypter interface {
	// Decrypt decrypts the given ciphertext.
	//
	// The method returns plaintext or an error.
	decrypt(ctx context.Context, ciphertext []byte) ([]byte, error)
}

// Header represents the header.
type header struct {
	Version int    `json:"version"`
	DataKey []byte `json:"data_key"`
}

// Encoder implements a backup file encoder.
type encoder struct {
	jw  *journal.Writer
	dst io.WriteCloser
	buf *bytes.Buffer
	gcm cipher.AEAD
}

// NewEncoder returns a new encoder with dst.
func newEncoder(ctx context.Context, enc encrypter, dst io.WriteCloser) (*encoder, error) {
	key, err := randkey()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	jw := journal.NewWriter(dst)

	next, err := jw.Next()
	if err != nil {
		return nil, err
	}

	ct, err := enc.encrypt(ctx, key[:])
	if err != nil {
		return nil, err
	}

	buf, err := json.Marshal(header{
		Version: version,
		DataKey: ct,
	})
	if err != nil {
		return nil, err
	}

	if _, err := next.Write(buf); err != nil {
		return nil, err
	}

	return &encoder{
		jw:  jw,
		dst: dst,
		buf: bytes.NewBuffer(nil),
		gcm: gcm,
	}, nil
}

// Write implements io.Writer.
//
// The writer writes the bytes to an internal buffer, when it
// reaches the target chunksize or surpasses it the method will
// flush and reset the internal buffer onto a new leveldb journal.
func (enc *encoder) Write(p []byte) (int, error) {
	if enc.buf.Len() >= chunksize {
		if err := enc.flush(); err != nil {
			return 0, err
		}
	}

	return enc.buf.Write(p)
}

// Nonce generates a nonce.
func (enc *encoder) nonce() ([]byte, error) {
	buf := make([]byte, enc.gcm.NonceSize())
	_, err := rand.Read(buf)
	return buf, err
}

// Flush flushes the current chunk.
func (enc *encoder) flush() error {
	next, err := enc.jw.Next()
	if err != nil {
		return err
	}

	nonce, err := enc.nonce()
	if err != nil {
		return err
	}

	if _, err := next.Write(nonce); err != nil {
		return err
	}

	ct := enc.gcm.Seal(nil, nonce, enc.buf.Bytes(), nil)
	if _, err := next.Write(ct); err != nil {
		enc.buf.Reset()
		return err
	}

	enc.buf.Reset()
	return nil
}

// Close flushes and closes the encoder.
func (enc *encoder) Close() error {
	if enc.buf.Len() > 0 {
		if err := enc.flush(); err != nil {
			return err
		}
	}

	if err := enc.jw.Flush(); err != nil {
		return err
	}

	if err := enc.jw.Close(); err != nil {
		return err
	}

	if err := enc.dst.Close(); err != nil {
		return err
	}

	return nil
}

// Decoder implements a decoder.
type decoder struct {
	jr  *journal.Reader
	buf *bytes.Reader
	src io.ReadCloser
	key []byte
	gcm cipher.AEAD
}

// NewDecoder returns a new decoder from src.
func newDecoder(ctx context.Context, dec decrypter, src io.ReadCloser) (*decoder, error) {
	var hdr header

	jr := journal.NewReader(src, nil, true, true)
	r, err := jr.Next()
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r).Decode(&hdr); err != nil {
		return nil, err
	}

	if hdr.Version != version {
		return nil, errors.Errorf("expected version %d, got %d",
			version,
			hdr.Version,
		)
	}

	ct, err := dec.decrypt(ctx, hdr.DataKey)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(ct)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &decoder{
		jr:  jr,
		src: src,
		key: hdr.DataKey,
		gcm: gcm,
	}, nil
}

// Read implements io.Reader.
//
// The method transforms the underlying journal onto a sequence
// of readers. When read is called, the method will lookup the current
// journal and read from it, if it is nil, the method will read the next
// journal.
//
// When a journal returns an `EOF` error, the method will get the next
// journal for the next read call.
func (dec *decoder) Read(p []byte) (int, error) {
	if dec.buf == nil {
		if err := dec.next(); err != nil {
			return 0, err
		}
	}

	n, err := dec.buf.Read(p)

	if errors.Is(err, io.EOF) {
		// EOF, return what was read and attempt
		// to read and decrypt the next journal.
		return n, dec.next()
	}

	return n, err
}

// Next decrypts and reads the next journal.
func (dec *decoder) next() error {
	r, err := dec.jr.Next()
	if err != nil {
		return err
	}

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	size := dec.gcm.NonceSize()
	nonce, ct := buf[:size], buf[size:]

	data, err := dec.gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return err
	}

	if dec.buf == nil {
		dec.buf = bytes.NewReader(data)
	} else {
		dec.buf.Reset(data)
	}

	return nil
}

// Close implements io.Closer.
func (dec *decoder) Close() error {
	return dec.src.Close()
}

// Randkey generates a new random key.
func randkey() ([32]byte, error) {
	var buf [32]byte
	_, err := rand.Read(buf[:])
	return buf, err
}

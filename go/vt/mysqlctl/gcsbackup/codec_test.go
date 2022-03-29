package gcsbackup

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	t.Run("encode decode", func(t *testing.T) {
		var ctx = context.Background()
		var assert = require.New(t)
		var buf = bytes.NewBuffer(nil)
		var kms fakeKMS

		enc, err := newEncoder(ctx, kms, closer{buf})
		assert.NoError(err)

		n, err := enc.Write([]byte("foo"))
		assert.NoError(err)
		assert.Equal(3, n)
		assert.NoError(enc.Close())

		dec, err := newDecoder(ctx, kms, ioutil.NopCloser(buf))
		assert.NoError(err)

		data, err := ioutil.ReadAll(dec)
		assert.NoError(err)
		assert.Equal([]byte("foo"), data)
	})
}

type closer struct {
	io.Writer
}

func (closer) Close() error {
	return nil
}

type fakeKMS struct{}

func (fakeKMS) encrypt(ctx context.Context, pt []byte) ([]byte, error) {
	return pt, nil
}

func (fakeKMS) decrypt(ctx context.Context, ct []byte) ([]byte, error) {
	return ct, nil
}

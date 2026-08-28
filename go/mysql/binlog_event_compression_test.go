/*
Copyright 2024 The Vitess Authors.

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

package mysql

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestDecoderPool(t *testing.T) {
	validateDecoder := func(t *testing.T, err error, decoder *zstd.Decoder) {
		require.NoError(t, err)
		require.NotNil(t, decoder)
		require.IsType(t, &zstd.Decoder{}, decoder)
	}
	tests := []struct {
		name    string
		reader  io.Reader
		wantErr bool
	}{
		{
			name:   "happy path",
			reader: bytes.NewReader([]byte{0x68, 0x61, 0x70, 0x70, 0x79}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// It's not guaranteed that we get the same decoder back from the pool
			// that we just put in, so we use a loop and ensure that it worked at
			// least one of the times. Without doing this the test would be flaky.
			poolingUsed := false

			for range 20 {
				decoder, err := statefulDecoderPool.Get(tt.reader)
				validateDecoder(t, err, decoder)
				statefulDecoderPool.Put(decoder)

				decoder2, err := statefulDecoderPool.Get(tt.reader)
				validateDecoder(t, err, decoder2)
				if decoder2 == decoder {
					poolingUsed = true
				}
				statefulDecoderPool.Put(decoder2)

				decoder3, err := statefulDecoderPool.Get(tt.reader)
				validateDecoder(t, err, decoder3)
				if decoder3 == decoder || decoder3 == decoder2 {
					poolingUsed = true
				}
				statefulDecoderPool.Put(decoder3)
			}

			require.True(t, poolingUsed)
		})
	}
}

// compressPayloadForTest zstd-compresses in, the way a MySQL primary would when
// sending a compressed transaction payload.
func compressPayloadForTest(t testing.TB, in []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	out := enc.EncodeAll(in, nil)
	require.NoError(t, enc.Close())
	return out
}

// TestTransactionPayloadEventLenBounds checks that an event length taken from the
// payload cannot size an allocation before the bytes behind it have arrived. The
// length is a 4 byte field, so without this the payload can command a multi
// gigabyte allocation regardless of how much data it actually carries.
//
// The assertion is on allocation volume, not merely on getting an error: the
// pre-existing length-mismatch check further down already errors on this input,
// so an error alone does not distinguish the fix from its absence.
func TestTransactionPayloadEventLenBounds(t *testing.T) {
	inner := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(inner[binlogEventLenOffset:headerLen], 1<<30)
	compressed := compressPayloadForTest(t, inner)

	read := func() {
		tp := &TransactionPayload{
			uncompressedSize: uint64(len(inner)),
			compressionType:  TransactionPayloadCompressionZstd,
		}
		tp.payload = compressed
		require.NoError(t, tp.decode())
		defer tp.Close()
		_, err := tp.GetNextEvent()
		require.Error(t, err, "an event length larger than the payload must be rejected")
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	read()
	runtime.ReadMemStats(&after)

	// The claimed length is 1GiB. Allocating anywhere near it means the length was
	// trusted before the data arrived.
	const limit = 64 << 20
	allocated := after.TotalAlloc - before.TotalAlloc
	require.Less(t, allocated, uint64(limit),
		"reading a payload that claims a 1GiB event allocated %d bytes; the length was trusted before the data arrived",
		allocated)
}

// TestTransactionPayloadStreamingInconsistentMetadata covers the case where the
// event header advertises a large uncompressedSize as well as a large event
// length, which is what streaming mode does not verify: the allocation must still
// be bounded by the bytes that actually arrive.
func TestTransactionPayloadStreamingInconsistentMetadata(t *testing.T) {
	inner := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(inner[binlogEventLenOffset:headerLen], 1<<30)

	tp := &TransactionPayload{
		// Above ZstdInMemoryDecompressorMaxSize, so decompress() takes the
		// streaming path and never checks this figure against the real size.
		uncompressedSize: ZstdInMemoryDecompressorMaxSize + 1,
		compressionType:  TransactionPayloadCompressionZstd,
	}
	tp.payload = compressPayloadForTest(t, inner)
	require.NoError(t, tp.decode())
	defer tp.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := tp.GetNextEvent()
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	allocated := after.TotalAlloc - before.TotalAlloc
	require.Less(t, allocated, uint64(64<<20),
		"streaming mode allocated %d bytes for a 13 byte frame claiming a 1GiB event", allocated)
}

// TestTransactionPayloadPostThresholdAmplification covers the case an earlier
// version of this bound missed: a payload that really does deliver a large amount
// of data before claiming a much larger event. Capping only the initial
// reservation and then growing to the claimed length once the cap filled left the
// original amplification intact past that point -- measured, 138 compressed bytes
// claiming a 512MiB event still allocated 540,471,168 bytes. The remaining-bytes
// bound rejects it instead, because no event body can exceed what is left of the
// decompressed stream.
func TestTransactionPayloadPostThresholdAmplification(t *testing.T) {
	const claimed = 512 << 20
	body := make([]byte, (1<<20)+4096) // comfortably past any fixed cap
	inner := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(inner[binlogEventLenOffset:headerLen], uint32(claimed))
	inner = append(inner, body...)

	tp := &TransactionPayload{
		uncompressedSize: uint64(len(inner)),
		compressionType:  TransactionPayloadCompressionZstd,
	}
	tp.payload = compressPayloadForTest(t, inner)
	require.NoError(t, tp.decode())
	defer tp.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := tp.GetNextEvent()
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	allocated := after.TotalAlloc - before.TotalAlloc
	require.Less(t, allocated, uint64(16<<20),
		"a %d byte frame claiming a %d byte event allocated %d bytes",
		len(tp.payload), claimed, allocated)
}

// TestTransactionPayloadSelfConsistent checks that the bound above does not reject
// a payload whose event length matches its actual size.
func TestTransactionPayloadSelfConsistent(t *testing.T) {
	const eventLen = 40
	inner := make([]byte, eventLen)
	binary.LittleEndian.PutUint32(inner[binlogEventLenOffset:headerLen], eventLen)

	tp := &TransactionPayload{
		uncompressedSize: uint64(len(inner)),
		compressionType:  TransactionPayloadCompressionZstd,
	}
	tp.payload = compressPayloadForTest(t, inner)
	require.NoError(t, tp.decode())
	defer tp.Close()

	ev, err := tp.GetNextEvent()
	require.NoError(t, err)
	require.NotNil(t, ev)
}

// TestTransactionPayloadBothLengthsLie covers the hole a reviewer found in the
// previous bound: uncompressedSize is never verified against the decoded stream
// in streaming mode, so bounding eventLen by it let a frame lie in BOTH fields.
// A 26 byte frame advertising uncompressedSize 2GiB and eventLen 1GiB was
// accepted and allocated 1,073,746,640 bytes on main.
func TestTransactionPayloadBothLengthsLie(t *testing.T) {
	inner := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(inner[binlogEventLenOffset:headerLen], 1<<30)

	tp := &TransactionPayload{
		// Above ZstdInMemoryDecompressorMaxSize so decompress() streams and never
		// checks this figure, and large enough that it cannot bound eventLen.
		uncompressedSize: 2 << 30,
		compressionType:  TransactionPayloadCompressionZstd,
	}
	tp.payload = compressPayloadForTest(t, inner)
	require.NoError(t, tp.decode())
	defer tp.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := tp.GetNextEvent()
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	allocated := after.TotalAlloc - before.TotalAlloc
	require.Less(t, allocated, uint64(16<<20),
		"a %d byte frame claiming a 1GiB event inside a 2GiB payload allocated %d bytes",
		len(tp.payload), allocated)
}

// TestTransactionPayloadPostChunkClaim covers the case a reviewer raised against
// the fixed-chunk version of this bound: deliver just over the chunk, then claim
// far more. Reading one chunk and committing to the claimed length only postponed
// the allocation -- a 398 byte frame delivering 1.1MiB while claiming 4GiB
// allocated 4,304,566,256 bytes on main and 4,305,625,816 with a fixed chunk.
func TestTransactionPayloadPostChunkClaim(t *testing.T) {
	const claim = uint32(4<<30 - 1)
	body := make([]byte, (1<<20)+65536)
	inner := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(inner[binlogEventLenOffset:headerLen], claim)
	inner = append(inner, body...)

	tp := &TransactionPayload{
		uncompressedSize: uint64(claim),
		compressionType:  TransactionPayloadCompressionZstd,
	}
	tp.payload = compressPayloadForTest(t, inner)
	require.NoError(t, tp.decode())
	defer tp.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := tp.GetNextEvent()
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	allocated := after.TotalAlloc - before.TotalAlloc
	require.Less(t, allocated, uint64(64<<20),
		"a %d byte frame delivering %d bytes while claiming %d allocated %d bytes",
		len(tp.payload), len(body), claim, allocated)
}

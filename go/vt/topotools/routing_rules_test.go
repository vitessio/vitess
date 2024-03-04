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

package topotools

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestRoutingRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	rules := map[string][]string{
		"t1": {"t2", "t3"},
		"t4": {"t5"},
	}

	err := SaveRoutingRules(ctx, ts, rules)
	require.NoError(t, err, "could not save routing rules to topo %v", rules)

	roundtripRules, err := GetRoutingRules(ctx, ts)
	require.NoError(t, err, "could not fetch routing rules from topo")

	assert.Equal(t, rules, roundtripRules)
}

func TestRoutingRulesErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
	factory.SetError(errors.New("topo failure for testing"))

	t.Run("GetRoutingRules error", func(t *testing.T) {

		rules, err := GetRoutingRules(ctx, ts)
		assert.Error(t, err, "expected error from GetRoutingRules, got rules=%v", rules)
	})

	t.Run("SaveRoutingRules error", func(t *testing.T) {
		rules := map[string][]string{
			"t1": {"t2", "t3"},
			"t4": {"t5"},
		}

		err := SaveRoutingRules(ctx, ts, rules)
		assert.Error(t, err, "expected error from GetRoutingRules, got rules=%v", rules)
	})
}

func TestShardRoutingRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	srr := map[string]string{
		"ks1.shard1": "ks2",
		"ks3.shard2": "ks4",
	}

	err := SaveShardRoutingRules(ctx, ts, srr)
	require.NoError(t, err, "could not save shard routing rules to topo %v", err)

	roundtripRules, err := GetShardRoutingRules(ctx, ts)
	require.NoError(t, err, "could not fetch shard routing rules from topo: %v", err)

	assert.Equal(t, srr, roundtripRules)
}

func TestKeyspaceRoutingRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	rulesMap := map[string]string{
		"ks1": "ks2",
		"ks4": "ks5",
	}

	err := SaveKeyspaceRoutingRules(ctx, ts, rulesMap)
	require.NoError(t, err, "could not save keyspace routing rules to topo %v", rulesMap)

	roundtripRulesMap, err := GetKeyspaceRoutingRules(ctx, ts)
	require.NoError(t, err, "could not fetch keyspace routing rules from topo")
	assert.EqualValues(t, rulesMap, roundtripRulesMap)
}

// TestLotsOfKeyspaceRoutingRules checks the size of the rules map for a huge number of rules and the effectiveness
// of storing the map compressed in the topo.
func TestLotsOfKeyspaceRoutingRules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	const numRules = 10
	maxVal := int64(9999999999)
	minVal := int64(9999999999) - 1*numRules

	keyPrefix := ""   //"original_keyspace_"
	valuePrefix := "" //"target_keyspace_"
	getKey := func(i int64) string {
		return fmt.Sprintf("%s%d", keyPrefix, i)
	}
	getValue := func(i int64) string {
		return fmt.Sprintf("%s%d", valuePrefix, i)
	}
	rulesMap := make(map[string]string)
	concatted := strings.Builder{}
	var keyList []string
	for j := 0; j < numRules; j++ {
		i := int64(rand.Intn(int(maxVal-minVal))) + minVal
		rulesMap[getKey(i)] = getValue(i)
		concatted.WriteString(fmt.Sprintf("%s:%s,", getKey(i), getValue(i)))
		keyList = append(keyList, getKey(i))
	}
	log.Infof("Key list: %v:%s", keyList, compressRanges(strings.Join(keyList, ",")))
	t1 := time.Now()
	err := SaveKeyspaceRoutingRules(ctx, ts, rulesMap)
	log.Infof("SaveKeyspaceRoutingRules took %v", time.Since(t1))
	require.NoError(t, err, "could not save keyspace routing rules to topo %v", rulesMap)

	t1 = time.Now()
	roundtripRulesMap, err := GetKeyspaceRoutingRules(ctx, ts)
	log.Infof("GetKeyspaceRoutingRules took %v", time.Since(t1))
	require.NoError(t, err, "could not fetch keyspace routing rules from topo")

	assert.EqualValues(t, rulesMap, roundtripRulesMap)
	log.Infof("Number of rules: %d", len(rulesMap))

	serialized, err := serializeMap(rulesMap)
	require.NoError(t, err, "could not serialize keyspace routing rules to topo %v", rulesMap)
	log.Infof("Size of serialized rules map: %d KB", len(serialized)/1024)
	compressed, err := compressData(serialized)
	require.NoError(t, err, "could not compress keyspace routing rules to topo %v", rulesMap)
	log.Infof("Size of compressed rules map: %d KB", len(compressed)/1024)
	decompressed, err := decompressData(compressed)
	require.NoError(t, err, "could not decompress keyspace routing rules to topo %v", rulesMap)
	log.Infof("Size of decompressed rules map: %d", len(decompressed))
	deserializedMap, err := deserializeMap(decompressed)
	log.Infof("Compression ratio percentage: %f, reverse ratio %f",
		float64(len(compressed))/float64(len(serialized))*100, float64(len(serialized))/float64(len(compressed)))
	require.NoError(t, err, "could not deserialize keyspace routing rules to topo %v", rulesMap)
	log.Infof("Size of deserialized rules map: %d", len(deserializedMap))
	assert.EqualValues(t, rulesMap, deserializedMap)
	compressed2 := compressed
	compressed, err = compressData([]byte(concatted.String()))
	compressed3 := compressed
	_ = compressed3
	lst := strings.Join(keyList, ",")
	compressed, err = compressDataGzip([]byte(lst))
	require.NoError(t, err)
	log.Infof("Size of  concatted string: %d KB", len(concatted.String())/1024)
	log.Infof("Compression ratio percentage for concatted string: %f", float64(concatted.Len())/float64(len(compressed)))
	log.Infof("Sizes of compressed map: %d KB, compressed concatted : %d KB", len(compressed2)/1024, len(compressed)/1024)
	log.Infof("Sizes of compressed map: %d KB, compressed list : %d KB", len(compressed2)/1024, len(compressed)/1024)

	lst2 := compressRanges(lst)
	log.Infof("Size of 'compressed' list %d, original list %d", len(lst2), len(lst))
	compressed, err = compressDataGzip([]byte(lst))
	log.Infof("Compression ratio percentage for list: %f", float64(len(lst))/float64(len(lst2)))
}

// serializeMap serializes a map into a byte slice using gob.
func serializeMap(data map[string]string) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// compressData compresses a byte slice using gzip.
func compressDataGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompressData decompresses a gzip-compressed byte slice.
func decompressDataGzip(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	return ioutil.ReadAll(gz)
}

// deserializeMap deserializes a byte slice into a map using gob.
func deserializeMap(data []byte) (map[string]string, error) {
	var m map[string]string
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// compressData compresses data using Zstandard.
func compressDataZstd(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return encoder.EncodeAll(data, make([]byte, 0, len(data))), nil
}

// decompressData decompresses data using Zstandard.
func decompressDataZstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}

// compressData compresses data using Brotli.
func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := brotli.NewWriter(&buf)
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompressData decompresses Brotli compressed data.
func decompressData(compressedData []byte) ([]byte, error) {
	var buf bytes.Buffer
	reader := brotli.NewReader(bytes.NewReader(compressedData))
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// compressRanges takes a string of integers separated by commas,
// compresses consecutive integers into ranges, and returns a single string.
func compressRanges(input string) string {
	// Split the input string into a slice of strings.
	strNumbers := strings.Split(input, ",")
	numbers := make([]int, len(strNumbers))

	// Convert strings to integers.
	for i, str := range strNumbers {
		num, err := strconv.Atoi(str)
		if err != nil {
			fmt.Printf("Error converting string to int: %v\n", err)
			return ""
		}
		numbers[i] = num
	}

	// Sort the slice of integers.
	sort.Ints(numbers)

	// Iterate through numbers to find ranges.
	var ranges []string
	for i := 0; i < len(numbers); {
		start := numbers[i]
		end := start

		// Find the end of the current range.
		for i+1 < len(numbers) && numbers[i+1] == numbers[i]+1 {
			i++
			end = numbers[i]
		}

		// Add the range to the result slice.
		if start == end {
			ranges = append(ranges, strconv.Itoa(start))
		} else {
			ranges = append(ranges, fmt.Sprintf("%d-%d", start, end))
		}

		i++
	}
	// Join the ranges with commas and return.
	return strings.Join(ranges, ",")
}

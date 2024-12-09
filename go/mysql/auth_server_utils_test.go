// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDeserializeCachingSha2PasswordAuthString tests that MySQL-generated caching_sha2_password authentication strings
// can be correctly deserialized into their component parts. We use a hex encoded string for the authentication string,
// because it is binary data and displaying it as a string and copying/pasting it corrupts the data.
func TestDeserializeCachingSha2PasswordAuthString(t *testing.T) {
	tests := []struct {
		hexEncodedAuthStringBytes string
		expectedDigestType        string
		expectedIterations        int
		expectedSalt              []byte
		expectedDigest            []byte
		expectedErrorSubstring    string
	}{
		{
			hexEncodedAuthStringBytes: "2441243030352434341F5017121D0420134615056D3519305C4C57507A4B4E584E482E5351544E324B2E44764B586566567243336F56367739736F61386E424B695741395443",
			expectedDigestType:        "SHA256",
			expectedIterations:        5,
			expectedSalt:              []byte{52, 52, 31, 80, 23, 18, 29, 4, 32, 19, 70, 21, 5, 109, 53, 25, 48, 92, 76, 87},
			expectedDigest: []byte{0x50, 0x7a, 0x4b, 0x4e, 0x58, 0x4e, 0x48, 0x2e, 0x53, 0x51, 0x54,
				0x4e, 0x32, 0x4b, 0x2e, 0x44, 0x76, 0x4b, 0x58, 0x65, 0x66, 0x56, 0x72, 0x43, 0x33, 0x6f, 0x56,
				0x36, 0x77, 0x39, 0x73, 0x6f, 0x61, 0x38, 0x6e, 0x42, 0x4b, 0x69, 0x57, 0x41, 0x39, 0x54, 0x43},
		},
		{
			hexEncodedAuthStringBytes: "244124303035241A502F3D02576A0150494D096659325E017E08086E516B42326E5762733366615556756E6131666174354533594255684536356E79772F5971397876772F32",
			expectedDigestType:        "SHA256",
			expectedIterations:        5,
			expectedSalt:              []byte{0x1a, 0x50, 0x2f, 0x3d, 0x2, 0x57, 0x6a, 0x1, 0x50, 0x49, 0x4d, 0x9, 0x66, 0x59, 0x32, 0x5e, 0x1, 0x7e, 0x8, 0x8},
			expectedDigest: []byte{0x6e, 0x51, 0x6b, 0x42, 0x32, 0x6e, 0x57, 0x62, 0x73, 0x33, 0x66,
				0x61, 0x55, 0x56, 0x75, 0x6e, 0x61, 0x31, 0x66, 0x61, 0x74, 0x35, 0x45, 0x33, 0x59, 0x42, 0x55,
				0x68, 0x45, 0x36, 0x35, 0x6e, 0x79, 0x77, 0x2f, 0x59, 0x71, 0x39, 0x78, 0x76, 0x77, 0x2f, 0x32},
		},

		// TODO: Test malformed auth strings
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			bytes, err := hex.DecodeString(test.hexEncodedAuthStringBytes)
			require.NoError(t, err)

			digestType, iterations, salt, digest, err := DeserializeCachingSha2PasswordAuthString(bytes)
			if test.expectedErrorSubstring == "" {
				require.NoError(t, err)
				require.Equal(t, test.expectedDigestType, digestType)
				require.Equal(t, test.expectedIterations, iterations)
				require.Equal(t, test.expectedSalt, salt)
				require.Equal(t, test.expectedDigest, digest)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedErrorSubstring)
			}
		})
	}
}

// TestSerializeCachingSha2PasswordAuthString tests that we can generate a correct caching_sha2_password authentication
// string from a password, salt, and number of iterations. We use a hex encoded string for the expected authentication
// string, because it is binary data and displaying it as a string and copying/pasting it corrupts the data.
func TestSerializeCachingSha2PasswordAuthString(t *testing.T) {
	tests := []struct {
		password                     string
		salt                         []byte
		iterations                   int
		expectedHexEncodedAuthString string
		expectedErrorSubstring       string
	}{
		{
			password:                     "pass3",
			salt:                         []byte{52, 52, 31, 80, 23, 18, 29, 4, 32, 19, 70, 21, 5, 109, 53, 25, 48, 92, 76, 87},
			iterations:                   5,
			expectedHexEncodedAuthString: "2441243030352434341F5017121D0420134615056D3519305C4C57507A4B4E584E482E5351544E324B2E44764B586566567243336F56367739736F61386E424B695741395443",
		},
		{
			password:                     "pass1",
			salt:                         []byte{0x1a, 0x50, 0x2f, 0x3d, 0x2, 0x57, 0x6a, 0x1, 0x50, 0x49, 0x4d, 0x9, 0x66, 0x59, 0x32, 0x5e, 0x1, 0x7e, 0x8, 0x8},
			iterations:                   5,
			expectedHexEncodedAuthString: "244124303035241A502F3D02576A0150494D096659325E017E08086E516B42326E5762733366615556756E6131666174354533594255684536356E79772F5971397876772F32",
		},
		{
			// When an iteration count larger than 0xFFF is specified, an error should be returned
			password:               "pass1",
			salt:                   []byte{0x1a, 0x50, 0x2f, 0x3d, 0x2, 0x57, 0x6a, 0x1, 0x50, 0x49, 0x4d, 0x9, 0x66, 0x59, 0x32, 0x5e, 0x1, 0x7e, 0x8, 0x8},
			iterations:             maxIterations + 1,
			expectedErrorSubstring: "iterations value (4096) is greater than max allowed iterations (4095)",
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			authStringBytes, err := SerializeCachingSha2PasswordAuthString(test.password, test.salt, test.iterations)
			if test.expectedErrorSubstring == "" {
				expectedBytes, err := hex.DecodeString(test.expectedHexEncodedAuthString)
				require.NoError(t, err)
				require.Equal(t, expectedBytes, authStringBytes)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedErrorSubstring)
			}
		})
	}
}

/*
Copyright 2019 The Vitess Authors.
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
package grpcoptionaltls

import "io"

const TLSPeekedBytes = 6

func looksLikeTLS(bytes []byte) bool {
	if len(bytes) < TLSPeekedBytes {
		return false
	}
	// TLS starts as
	// 0: 0x16 - handshake protocol magic
	// 1: 0x03 - SSL version major
	// 2: 0x00 to 0x03 - SSL version minor (SSLv3 or TLS1.0 through TLS1.3)
	// 3-4: length (2 bytes)
	// 5: 0x01 - handshake type (ClientHello)
	// 6-8: handshake len (3 bytes), equals value from offset 3-4 minus 4
	// HTTP2 initial frame bytes
	// https://tools.ietf.org/html/rfc7540#section-3.4

	// Definitely not TLS
	if bytes[0] != 0x16 || bytes[1] != 0x03 || bytes[5] != 0x01 {
		return false
	}
	return true
}

// DetectTLS reads necessary number of bytes from io.Reader
// returns result, bytes read from Reader and error
// No matter if error happens or what flag value is
// returned bytes should be checked
func DetectTLS(r io.Reader) (bool, []byte, error) {
	var bytes = make([]byte, TLSPeekedBytes)
	if n, err := io.ReadFull(r, bytes); err != nil {
		return false, bytes[:n], err
	}
	return looksLikeTLS(bytes), bytes, nil
}

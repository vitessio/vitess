/*
Copyright 2020 The Vitess Authors.

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

package textutil

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/google/uuid"
)

const (
	randomHashSize = 64
)

// RandomHash returns a 64 hex character random string
func RandomHash() string {
	rb := make([]byte, randomHashSize)
	_, _ = rand.Read(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	return hex.EncodeToString(hasher.Sum(nil))
}

// UUIDv5 creeates a UUID v5 string based on the given inputs. We use a SHA256 algorithm.
func UUIDv5(inputs ...string) string {
	var baseUUID uuid.UUID
	input := strings.Join(inputs, "\n")
	outputUUID := uuid.NewHash(sha256.New(), baseUUID, []byte(input), 5)

	return outputUUID.String()
}

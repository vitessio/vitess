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
	"fmt"
	"math/big"
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

// uuidv5 creates a UUID v5 string based on the given inputs. We use a SHA256 algorithm.
func uuidv5(inputs ...string) uuid.UUID {
	var baseUUID uuid.UUID
	input := strings.Join(inputs, "\n")
	return uuid.NewHash(sha256.New(), baseUUID, []byte(input), 5)
}

// UUIDv5 creates a UUID v5 string based on the given inputs. We use a SHA256 algorithm. Return format is textual
func UUIDv5(inputs ...string) string {
	return uuidv5(inputs...).String()
}

// UUIDv5Var creeates a UUID v5 string based on the given inputs. Return value is a big.Int
func UUIDv5Val(inputs ...string) big.Int {
	u := uuidv5(inputs...)
	var i big.Int
	i.SetBytes(u[:])
	return i
}

// UUIDv5Base36 creeates a UUID v5 string based on the given inputs. Return value is a 25 character, base36 string
func UUIDv5Base36(inputs ...string) string {
	i := UUIDv5Val(inputs...)
	return fmt.Sprintf("%025s", i.Text(36))
}

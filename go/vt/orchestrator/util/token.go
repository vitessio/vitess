/*
   Copyright 2014 Outbrain Inc.

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

package util

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	shortTokenLength = 8
)

func toHash(input []byte) string {
	hasher := sha256.New()
	hasher.Write(input)
	return hex.EncodeToString(hasher.Sum(nil))
}

func getRandomData() []byte {
	size := 64
	rb := make([]byte, size)
	_, _ = rand.Read(rb)
	return rb
}

func RandomHash() string {
	return toHash(getRandomData())
}

// Token is used to identify and validate requests to this service
type Token struct {
	Hash string
}

func (this *Token) Short() string {
	if len(this.Hash) <= shortTokenLength {
		return this.Hash
	}
	return this.Hash[0:shortTokenLength]
}

var ProcessToken *Token = NewToken()

func NewToken() *Token {
	return &Token{
		Hash: RandomHash(),
	}
}

func PrettyUniqueToken() string {
	return fmt.Sprintf("%d:%s", time.Now().UnixNano(), NewToken().Hash)
}

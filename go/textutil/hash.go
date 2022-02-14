package textutil

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// RandomHash returns a 64 hex character random string
func RandomHash() string {
	size := 64
	rb := make([]byte, size)
	_, _ = rand.Read(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	return hex.EncodeToString(hasher.Sum(nil))
}

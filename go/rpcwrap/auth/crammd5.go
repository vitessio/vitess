package auth

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

func CRAMMD5GetChallenge() (string, error) {
	var randDigits uint32
	err := binary.Read(rand.Reader, binary.LittleEndian, &randDigits)

	if err != nil {
		return "", err
	}
	timestamp := time.Now().Unix()

	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("<%d.%d@%s>", randDigits, timestamp, hostname), nil
}

func CRAMMD5GetExpected(username, secret, challenge string) string {
	var ret []byte
	hash := hmac.New(md5.New, []byte(secret))
	hash.Write([]byte(challenge))
	ret = hash.Sum(nil)
	return username + " " + hex.EncodeToString(ret)
}

package unicode

import (
	"errors"
	"unicode/utf8"
)

const RuneError = utf8.RuneError

var errBMPRange = errors.New("input string contains characters outside of BMP range (cp > 0xFFFF)")

func ensureBMPRange(in []byte) error {
	for _, cp := range string(in) {
		if cp > 0xFFFF {
			return errBMPRange
		}
	}
	return nil
}

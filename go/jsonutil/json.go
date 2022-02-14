// Package jsonutil contains json-related utility functions
package jsonutil

import (
	"bytes"
	"encoding/json"
)

// MarshalNoEscape is the same functionality as json.Marshal but
// with HTML escaping disabled
func MarshalNoEscape(v interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalIndentNoEscape is the same functionality as json.MarshalIndent but with HTML escaping
// disabled
func MarshalIndentNoEscape(v interface{}, prefix, indent string) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent(prefix, indent)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

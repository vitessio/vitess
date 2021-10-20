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

package tablebuilder

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

type EmbeddedPageBuilder struct {
	index map[string]string
	raw   bytes.Buffer
}

func hashWeights(values []uint16) string {
	h := sha256.New()
	for _, v := range values {
		h.Write([]byte{byte(v >> 8), byte(v)})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (pb *EmbeddedPageBuilder) WritePage(w io.Writer, varname string, values []uint16) string {
	hash := hashWeights(values)
	if existing, ok := pb.index[hash]; ok {
		return "&" + existing
	}

	pb.index[hash] = varname
	fmt.Fprintf(w, "var %s = weightsUCA_embed(%d, %d)\n", varname, pb.raw.Len()/2, len(values))
	for _, v := range values {
		pb.raw.WriteByte(byte(v))
		pb.raw.WriteByte(byte(v >> 8))
	}
	return "&" + varname
}

func (pb *EmbeddedPageBuilder) WriteFastPage(w io.Writer, varname string, values []uint16) {
	if len(values) != 256 {
		panic("WriteFastPage: page does not have 256 values")
	}

	var min uint16 = 0xFFFF
	fmt.Fprintf(w, "var fast%s = [...]uint16{", varname)
	for col, val := range values {
		if col%8 == 0 {
			fmt.Fprintf(w, "\n")
		}
		fmt.Fprintf(w, "0x%04x, ", val)
		if val != 0 && val < min {
			min = val
		}
	}
	fmt.Fprintf(w, "\n}\n\n")
	fmt.Fprintf(w, "const fast%s_min = 0x%04x\n\n", varname, min)
}

func (pb *EmbeddedPageBuilder) WriteTrailer(w io.Writer, embedfile string) {
	fmt.Fprintf(w, "\n\n")
	fmt.Fprintf(w, "//go:embed %s\n", embedfile)
	fmt.Fprintf(w, "var weightsUCA_embed_data string\n\n")
	fmt.Fprintf(w, "func weightsUCA_embed(pos, length int) []uint16 {\n")
	fmt.Fprintf(w, "return (*[0x7fff0000]uint16)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&weightsUCA_embed_data)).Data))[pos:pos+length]\n")
	fmt.Fprintf(w, "}\n")
}

func (pb *EmbeddedPageBuilder) EmbedData() []byte {
	return pb.raw.Bytes()
}

func NewPageBuilder() *EmbeddedPageBuilder {
	return &EmbeddedPageBuilder{
		index: make(map[string]string),
	}
}

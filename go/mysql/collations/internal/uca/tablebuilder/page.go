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

package collations

import (
	"compress/gzip"
	"encoding/gob"
	"io"
	"os"
)

type GoldenCase struct {
	Lang    string
	Text    []byte
	Weights map[string][]byte
}

type GoldenTest struct {
	Name  string
	Cases []GoldenCase
}

func (golden *GoldenTest) Encode(w io.Writer) error {
	gw := gzip.NewWriter(w)
	defer gw.Close()

	enc := gob.NewEncoder(gw)
	return enc.Encode(golden)
}

func (golden *GoldenTest) EncodeToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return golden.Encode(f)
}

func (golden *GoldenTest) Decode(r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gr.Close()

	dec := gob.NewDecoder(gr)
	return dec.Decode(golden)
}

func (golden *GoldenTest) DecodeFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return golden.Decode(f)
}

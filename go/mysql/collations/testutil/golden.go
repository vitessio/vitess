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

package testutil

import (
	"compress/gzip"
	"encoding/gob"
	"io"
	"os"
)

type GoldenCase struct {
	Lang    Lang
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

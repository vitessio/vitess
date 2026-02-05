/*
Copyright 2023 The Vitess Authors.

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

package opentsdb

import (
	"bufio"
	"io"
)

var newLineDelimiter = byte('\n')

// DataPointReader parses bytes from io.Reader into DataPoints.
type DataPointReader struct {
	reader *bufio.Reader
}

func NewDataPointReader(r io.Reader) *DataPointReader {
	return &DataPointReader{
		reader: bufio.NewReader(r),
	}
}

// Read returns a DataPoint from the underlying io.Reader.
//
// Returns an error if no DataPoint could be parsed.
func (tr *DataPointReader) Read() (*DataPoint, error) {
	bs, err := tr.reader.ReadBytes(newLineDelimiter)
	if err != nil {
		return nil, err
	}

	dp := &DataPoint{}

	if err := unmarshalTextToData(dp, bs[:len(bs)-1]); err != nil {
		return nil, err
	}

	return dp, nil
}

/*
Copyright 2025 The Vitess Authors.

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

package replication

import (
	"fmt"
	"strconv"
	"strings"
)

type BinlogFilePos struct {
	File string
	Pos  uint64
}

// ParseBinlogFilePos parses a binlog file and position in the input
// format used by internal code that processes output from MySQL such
// as SHOW REPLICA STATUS and SHOW BINARY LOG STATUS.
func ParseBinlogFilePos(s string) (BinlogFilePos, error) {
	bfp := BinlogFilePos{}
	if s == "" {
		return bfp, nil
	}

	file, posStr, ok := strings.Cut(s, ":")
	if !ok {
		return bfp, fmt.Errorf("invalid binlog file position (%v): expecting file:pos", s)
	}

	pos, err := strconv.ParseUint(posStr, 0, 64)
	if err != nil {
		return bfp, fmt.Errorf("invalid binlog file position (%v): expecting position to be an unsigned 64 bit integer", posStr)
	}

	bfp.File = file
	bfp.Pos = pos

	return bfp, nil
}

// String returns the string representation of the BinlogFilePos
// using a colon as the seperator.
func (bfp BinlogFilePos) String() string {
	return fmt.Sprintf("%s:%d", bfp.File, bfp.Pos)
}

func (bfp BinlogFilePos) IsZero() bool {
	return bfp.File == "" && bfp.Pos == 0
}

func (bfp BinlogFilePos) ConvertToFlavorPosition() (pos Position, err error) {
	pos.GTIDSet, err = ParseFilePosGTIDSet(bfp.String())
	return pos, err
}

func (bfp BinlogFilePos) Equal(b BinlogFilePos) bool {
	return bfp.File == b.File && bfp.Pos == b.Pos
}

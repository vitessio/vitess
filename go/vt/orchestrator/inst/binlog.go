/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package inst

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var detachPattern *regexp.Regexp

func init() {
	detachPattern, _ = regexp.Compile(`//([^/:]+):([\d]+)`) // e.g. `//binlog.01234:567890`
}

type BinlogType int

const (
	BinaryLog BinlogType = iota
	RelayLog
)

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type BinlogCoordinates struct {
	LogFile string
	LogPos  int64
	Type    BinlogType
}

// rpad formats the binlog coordinates to a given size. If the size
// increases this value is modified so it can be reused later. This
// is to ensure consistent formatting in debug output.
func rpad(coordinates BinlogCoordinates, length *int) string {
	s := fmt.Sprintf("%+v", coordinates)
	if len(s) > *length {
		*length = len(s)
	}

	if len(s) >= *length {
		return s
	}
	return fmt.Sprintf("%s%s", s, strings.Repeat(" ", *length-len(s)))
}

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306
func ParseBinlogCoordinates(logFileLogPos string) (*BinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &BinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *BinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", this.LogFile, this.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (this BinlogCoordinates) String() string {
	return this.DisplayString()
}

// Equals tests equality of this corrdinate and another one.
func (this *BinlogCoordinates) Equals(other *BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos && this.Type == other.Type
}

// IsEmpty returns true if the log file is empty, unnamed
func (this *BinlogCoordinates) IsEmpty() bool {
	return this.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
	if this.LogFile < other.LogFile {
		return true
	}
	if this.LogFile == other.LogFile && this.LogPos < other.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos // No Type comparison
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (this *BinlogCoordinates) FileSmallerThan(other *BinlogCoordinates) bool {
	return this.LogFile < other.LogFile
}

// FileNumberDistance returns the numeric distance between this corrdinate's file number and the other's.
// Effectively it means "how many roatets/FLUSHes would make these coordinates's file reach the other's"
func (this *BinlogCoordinates) FileNumberDistance(other *BinlogCoordinates) int {
	thisNumber, _ := this.FileNumber()
	otherNumber, _ := other.FileNumber()
	return otherNumber - thisNumber
}

// FileNumber returns the numeric value of the file, and the length in characters representing the number in the filename.
// Example: FileNumber() of mysqld.log.000789 is (789, 6)
func (this *BinlogCoordinates) FileNumber() (int, int) {
	tokens := strings.Split(this.LogFile, ".")
	numPart := tokens[len(tokens)-1]
	numLen := len(numPart)
	fileNum, err := strconv.Atoi(numPart)
	if err != nil {
		return 0, 0
	}
	return fileNum, numLen
}

// PreviousFileCoordinatesBy guesses the filename of the previous binlog/relaylog, by given offset (number of files back)
func (this *BinlogCoordinates) PreviousFileCoordinatesBy(offset int) (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: this.Type}

	fileNum, numLen := this.FileNumber()
	if fileNum == 0 {
		return result, errors.New("Log file number is zero, cannot detect previous file")
	}
	newNumStr := fmt.Sprintf("%d", (fileNum - offset))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (this *BinlogCoordinates) PreviousFileCoordinates() (BinlogCoordinates, error) {
	return this.PreviousFileCoordinatesBy(1)
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (this *BinlogCoordinates) NextFileCoordinates() (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: this.Type}

	fileNum, numLen := this.FileNumber()
	newNumStr := fmt.Sprintf("%d", (fileNum + 1))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// Detach returns a detahced form of coordinates
func (this *BinlogCoordinates) Detach() (detachedCoordinates BinlogCoordinates) {
	detachedCoordinates = BinlogCoordinates{LogFile: fmt.Sprintf("//%s:%d", this.LogFile, this.LogPos), LogPos: this.LogPos}
	return detachedCoordinates
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (this *BinlogCoordinates) ExtractDetachedCoordinates() (isDetached bool, detachedCoordinates BinlogCoordinates) {
	detachedCoordinatesSubmatch := detachPattern.FindStringSubmatch(this.LogFile)
	if len(detachedCoordinatesSubmatch) == 0 {
		return false, *this
	}
	detachedCoordinates.LogFile = detachedCoordinatesSubmatch[1]
	detachedCoordinates.LogPos, _ = strconv.ParseInt(detachedCoordinatesSubmatch[2], 10, 0)
	return true, detachedCoordinates
}

/*
Copyright 2019 The Vitess Authors.

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

package apps

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type EntryType int

const (
	Connect EntryType = iota
	Quit
	Query
	InitDb
)

type LogEntry struct {
	ConnectionID int
	Time         time.Time
	Typ          EntryType
	Text         string
}

func ReadLogFile(filePath string) ([]*LogEntry, error) {
	fil, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	return ReadLogLines(string(fil))
}

func ReadLogLines(input string) ([]*LogEntry, error) {
	lines := strings.Split(input, "\n")
	var currentQuery *LogEntry
	var result []*LogEntry

	for _, line := range lines {
		entry, normalLogLine := readLogLine(line)
		if normalLogLine {
			currentQuery = &entry
			result = append(result, currentQuery)
		} else {
			if currentQuery == nil {
				return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "encountered a weird log line %s", line)
			}
			currentQuery.Text = currentQuery.Text + "\n" + line
		}
	}

	return result, nil
}

func readLogLine(in string) (entry LogEntry, success bool) {
	endOfDateTime := strings.Index(in, "\t")
	if endOfDateTime < 0 {
		return LogEntry{}, false
	}
	dateTimeText := in[:endOfDateTime]
	tid, err := time.Parse(time.RFC3339, dateTimeText)
	if err != nil {
		return LogEntry{}, false
	}

	endOfTypeAndID := strings.LastIndex(in, "\t")
	text := strings.TrimSpace(in[endOfTypeAndID:])

	idAndCommand := in[endOfDateTime+1 : endOfTypeAndID]
	idText, commandText := splitIDAndCommand(idAndCommand)
	id, err := strconv.Atoi(idText)
	if err != nil {
		return LogEntry{}, false
	}

	return LogEntry{
		ConnectionID: id,
		Typ:          parseCommand(commandText),
		Time:         tid,
		Text:         text,
	}, true
}

func splitIDAndCommand(in string) (string, string) {
	r := regexp.MustCompile(`\s+(\d+) (\w+)`)
	result := r.FindAllStringSubmatch(in, -1)
	id := result[0][1]
	text := result[0][2]
	return id, text
}

func parseCommand(in string) EntryType {
	switch in {
	case "Connect":
		return Connect
	case "Init":
		return InitDb
	case "Quit":
		return Quit
	case "Query":
		return Query
	}

	panic(fmt.Sprintf("unknown command type %s", in))
}

/*
Copyright 2020 The Vitess Authors.

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

package textutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSplitDelimitedList(t *testing.T) {
	defaultList := []string{"one", "two", "three"}
	tt := []struct {
		s    string
		list []string
	}{
		{s: "one,two,three"},
		{s: "one, two, three"},
		{s: "one,two; three  "},
		{s: "one two three"},
		{s: "one,,,two,three"},
		{s: " one, ,two,  three "},
	}

	for _, tc := range tt {
		if tc.list == nil {
			tc.list = defaultList
		}
		list := SplitDelimitedList(tc.s)
		assert.Equal(t, tc.list, list)
	}
}

func TestEscapeJoin(t *testing.T) {
	elems := []string{"normal", "with space", "with,comma", "with?question"}
	s := EscapeJoin(elems, ",")
	assert.Equal(t, "normal,with+space,with%2Ccomma,with%3Fquestion", s)
}

func TestSplitUnescape(t *testing.T) {
	{
		s := ""
		elems, err := SplitUnescape(s, ",")
		assert.NoError(t, err)
		assert.Nil(t, elems)
	}
	{
		s := "normal,with+space,with%2Ccomma,with%3Fquestion"
		expected := []string{"normal", "with space", "with,comma", "with?question"}
		elems, err := SplitUnescape(s, ",")
		assert.NoError(t, err)
		assert.Equal(t, expected, elems)
	}
	{
		s := "invalid%2"
		elems, err := SplitUnescape(s, ",")
		assert.Error(t, err)
		assert.Equal(t, []string{}, elems)
	}
}

func TestSingleWordCamel(t *testing.T) {
	tt := []struct {
		word   string
		expect string
	}{
		{
			word:   "",
			expect: "",
		},
		{
			word:   "_",
			expect: "_",
		},
		{
			word:   "a",
			expect: "A",
		},
		{
			word:   "A",
			expect: "A",
		},
		{
			word:   "_A",
			expect: "_a",
		},
		{
			word:   "mysql",
			expect: "Mysql",
		},
		{
			word:   "mySQL",
			expect: "Mysql",
		},
	}
	for _, tc := range tt {
		t.Run(tc.word, func(t *testing.T) {
			camel := SingleWordCamel(tc.word)
			assert.Equal(t, tc.expect, camel)
		})
	}
}

func TestValueIsSimulatedNull(t *testing.T) {
	tt := []struct {
		name   string
		val    interface{}
		isNull bool
	}{
		{
			name:   "case string false",
			val:    "test",
			isNull: false,
		},
		{
			name:   "case string true",
			val:    SimulatedNullString,
			isNull: true,
		},
		{
			name:   "case []string true",
			val:    []string{SimulatedNullString},
			isNull: true,
		},
		{
			name:   "case []string false",
			val:    []string{SimulatedNullString, SimulatedNullString},
			isNull: false,
		},
		{
			name:   "case binlogdatapb.OnDDLAction true",
			val:    binlogdatapb.OnDDLAction(SimulatedNullInt),
			isNull: true,
		},
		{
			name:   "case int true",
			val:    SimulatedNullInt,
			isNull: true,
		},
		{
			name:   "case int32 true",
			val:    int32(SimulatedNullInt),
			isNull: true,
		},
		{
			name:   "case int64 true",
			val:    int64(SimulatedNullInt),
			isNull: true,
		},
		{
			name:   "case []topodatapb.TabletType true",
			val:    []topodatapb.TabletType{topodatapb.TabletType(SimulatedNullInt)},
			isNull: true,
		},
		{
			name:   "case binlogdatapb.VReplicationWorkflowState true",
			val:    binlogdatapb.VReplicationWorkflowState(SimulatedNullInt),
			isNull: true,
		},
		{
			name:   "case default",
			val:    float64(1),
			isNull: false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			isNull := ValueIsSimulatedNull(tc.val)
			assert.Equal(t, tc.isNull, isNull)
		})
	}
}

func TestTitle(t *testing.T) {
	tt := []struct {
		s      string
		expect string
	}{
		{s: "hello world", expect: "Hello World"},
		{s: "snake_case", expect: "Snake_case"},
		{s: "TITLE CASE", expect: "TITLE CASE"},
		{s: "HelLo wOrLd", expect: "HelLo WOrLd"},
		{s: "", expect: ""},
	}

	for _, tc := range tt {
		t.Run(tc.s, func(t *testing.T) {
			title := Title(tc.s)
			assert.Equal(t, tc.expect, title)
		})
	}
}

func TestTruncateText(t *testing.T) {
	defaultLocation := TruncationLocationMiddle
	defaultMaxLen := 100
	defaultTruncationIndicator := "..."

	tests := []struct {
		name                string
		text                string
		maxLen              int
		location            TruncationLocation
		truncationIndicator string
		want                string
		wantErr             string
	}{
		{
			name:     "no truncation",
			text:     "hello world",
			maxLen:   defaultMaxLen,
			location: defaultLocation,
			want:     "hello world",
		},
		{
			name:     "no truncation - exact",
			text:     strings.Repeat("a", defaultMaxLen),
			maxLen:   defaultMaxLen,
			location: defaultLocation,
			want:     strings.Repeat("a", defaultMaxLen),
		},
		{
			name:                "barely too long - mid",
			text:                strings.Repeat("a", defaultMaxLen+1),
			truncationIndicator: defaultTruncationIndicator,
			maxLen:              defaultMaxLen,
			location:            defaultLocation,
			want:                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			name:                "barely too long - end",
			text:                strings.Repeat("a", defaultMaxLen+1),
			truncationIndicator: defaultTruncationIndicator,
			maxLen:              defaultMaxLen,
			location:            TruncationLocationEnd,
			want:                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...",
		},
		{
			name:                "too small",
			text:                strings.Repeat("a", defaultMaxLen),
			truncationIndicator: defaultTruncationIndicator,
			maxLen:              4,
			location:            defaultLocation,
			wantErr:             "the truncation indicator is too long for the provided text",
		},
		{
			name:                "bad location",
			text:                strings.Repeat("a", defaultMaxLen+1),
			truncationIndicator: defaultTruncationIndicator,
			maxLen:              defaultMaxLen,
			location:            100,
			wantErr:             "invalid truncation location: 100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := TruncateText(tt.text, tt.maxLen, tt.location, tt.truncationIndicator)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, val)
				require.LessOrEqual(t, len(val), tt.maxLen)
			}
		})
	}
}

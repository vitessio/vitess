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

package mysql

import (
	"testing"
)

func Test_filePosGTID_String(t *testing.T) {
	type fields struct {
		file string
		pos  uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"formats gtid correctly",
			fields{file: "mysql-bin.166031", pos: 192394},
			"mysql-bin.166031:192394",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gtid := filePosGTID{
				file: tt.fields.file,
				pos:  tt.fields.pos,
			}
			if got := gtid.String(); got != tt.want {
				t.Errorf("filePosGTID.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filePosGTID_ContainsGTID(t *testing.T) {
	type fields struct {
		file string
		pos  uint32
	}
	type args struct {
		other GTID
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			"returns true when the position is equal",
			fields{file: "testfile", pos: 1234},
			args{other: filePosGTID{file: "testfile", pos: 1234}},
			true,
		},
		{
			"returns true when the position is less than equal",
			fields{file: "testfile", pos: 1234},
			args{other: filePosGTID{file: "testfile", pos: 1233}},
			true,
		},
		{
			"returns false when the position is less than equal",
			fields{file: "testfile", pos: 1234},
			args{other: filePosGTID{file: "testfile", pos: 1235}},
			false,
		},
		{
			"it uses integer value for comparison (it is not lexicographical order)",
			fields{file: "testfile", pos: 99761227},
			args{other: filePosGTID{file: "testfile", pos: 103939867}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gtid := filePosGTID{
				file: tt.fields.file,
				pos:  tt.fields.pos,
			}
			if got := gtid.ContainsGTID(tt.args.other); got != tt.want {
				t.Errorf("filePosGTID.ContainsGTID() = %v, want %v", got, tt.want)
			}
		})
	}
}

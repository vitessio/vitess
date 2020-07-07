/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8stopo

import (
	"reflect"
	"testing"
)

func Test_packValue(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		want    []byte
		wantErr bool
	}{
		{
			// a gzip with an empty payload still has header bytes to identify the stream
			"empty",
			[]byte{},
			[]byte{72, 52, 115, 73, 65, 65, 65, 65, 65, 65, 65, 65, 47, 119, 69, 65, 65, 80, 47, 47, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 61},
			false,
		},
		{
			"valid payload",
			[]byte("test payload"),
			[]byte{72, 52, 115, 73, 65, 65, 65, 65, 65, 65, 65, 65, 47, 121, 112, 74, 76, 83, 53, 82, 75, 69, 105, 115, 122, 77, 108, 80, 84, 65, 69, 69, 65, 65, 68, 47, 47, 43, 69, 57, 72, 101, 115, 77, 65, 65, 65, 65},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := packValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("packValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("packValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_unpackValue(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		want    []byte
		wantErr bool
	}{
		{
			// a gzip with an empty payload still has header bytes to identify the stream
			"empty",
			[]byte{72, 52, 115, 73, 65, 65, 65, 65, 65, 65, 65, 65, 47, 119, 69, 65, 65, 80, 47, 47, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 61},
			[]byte{},
			false,
		},
		{
			"valid payload",
			[]byte{72, 52, 115, 73, 65, 65, 65, 65, 65, 65, 65, 65, 47, 121, 112, 74, 76, 83, 53, 82, 75, 69, 105, 115, 122, 77, 108, 80, 84, 65, 69, 69, 65, 65, 68, 47, 47, 43, 69, 57, 72, 101, 115, 77, 65, 65, 65, 65},
			[]byte("test payload"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unpackValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("unpackValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unpackValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_packUnpackRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		wantErr bool
	}{
		{
			"empty",
			[]byte{},
			false,
		},
		{
			"valid payload",
			[]byte("test payload"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed, err := packValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("packValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			unpacked, err := unpackValue(packed)
			if (err != nil) != tt.wantErr {
				t.Errorf("packValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(unpacked, tt.value) {
				t.Errorf("unpacked value != original value original = %v, unpacked %v", tt.value, unpacked)
				return
			}
		})
	}
}

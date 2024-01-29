/*
Copyright 2024 The Vitess Authors.

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

package grpcclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestAppendStaticAuth(t *testing.T) {
	oldCredsFile := credsFile
	opts := []grpc.DialOption{
		grpc.EmptyDialOption{},
	}

	tests := []struct {
		name        string
		cFile       string
		expectedLen int
		expectedErr string
	}{
		{
			name:        "creds file not set",
			expectedLen: 1,
		},
		{
			name:        "non-existent creds file",
			cFile:       "./testdata/unknown.json",
			expectedErr: "open ./testdata/unknown.json: no such file or directory",
		},
		{
			name:        "valid creds file",
			cFile:       "./testdata/credsFile.json",
			expectedLen: 2,
		},
		{
			name:        "invalid creds file",
			cFile:       "./testdata/invalid.json",
			expectedErr: "unexpected end of JSON input",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.cFile, func(t *testing.T) {
			defer func() {
				credsFile = oldCredsFile
			}()

			if tt.cFile != "" {
				credsFile = tt.cFile
			}
			dialOpts, err := AppendStaticAuth(opts)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expectedLen, len(dialOpts))
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

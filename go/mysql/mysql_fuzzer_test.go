//go:build gofuzz
// +build gofuzz

package mysql

import (
	"os"
	"path"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuzzHandleNextCommandFromFile(t *testing.T) {
	directoryName := "fuzzdata"
	files, err := os.ReadDir(directoryName)
	require.NoError(t, err)
	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			defer func() {
				r := recover()
				if r != nil {
					t.Error(r)
					t.Fatal(string(debug.Stack()))
				}
			}()
			testcase, err := os.ReadFile(path.Join(directoryName, file.Name()))
			require.NoError(t, err)
			FuzzHandleNextCommand(testcase)
		})
	}
}

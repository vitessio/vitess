package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFileParam(t *testing.T) {
	tests := []struct {
		name        string
		flag        string
		flagFile    string
		paramName   string
		required    bool
		want        string
		wantErr     bool
		errContains string
	}{
		{
			name:      "flag value provided",
			flag:      "test value",
			flagFile:  "",
			paramName: "param",
			required:  true,
			want:      "test value",
		},
		{
			name:        "both flag and file provided",
			flag:        "test value",
			flagFile:    "test.txt",
			paramName:   "param",
			required:    true,
			wantErr:     true,
			errContains: "action requires only one of param or param-file",
		},
		{
			name:      "neither provided but not required",
			flag:      "",
			flagFile:  "",
			paramName: "param",
			required:  false,
			want:      "",
		},
		{
			name:        "neither provided but required",
			flag:        "",
			flagFile:    "",
			paramName:   "param",
			required:    true,
			wantErr:     true,
			errContains: "action requires one of param or param-file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getFileParam(tt.flag, tt.flagFile, tt.paramName, tt.required)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetFileParamWithFile(t *testing.T) {
	content := "test content"
	tmpDir := t.TempDir()
	tmpFile := path.Join(tmpDir, "test.txt")
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	require.NoError(t, err)

	got, err := getFileParam("", tmpFile, "param", true)
	require.NoError(t, err)
	assert.Equal(t, content, got)

	_, err = getFileParam("", "nonexistent.txt", "param", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot read file")
}

func TestParseAndRunErrors(t *testing.T) {
	tests := []struct {
		name              string
		sqlFlag           string
		sqlFileFlag       string
		schemaFlag        string
		schemaFileFlag    string
		vschemaFlag       string
		vschemaFileFlag   string
		plannerVersionStr string
		wantErr           bool
		errContains       string
	}{
		{
			name:              "invalid planner version",
			plannerVersionStr: "invalid",
			sqlFlag:           "SELECT 1;",
			schemaFlag:        "CREATE TABLE t (id INT);",
			vschemaFlag:       `{"ks": {}}`,
			wantErr:           true,
			errContains:       "invalid value specified for planner-version of 'invalid' -- valid value is Gen4 or an empty value to use the default planner",
		},
		{
			name:        "sql file not found",
			sqlFileFlag: "nonexistent.sql",
			schemaFlag:  "CREATE TABLE t (id INT);",
			vschemaFlag: `{"ks": {}}`,
			wantErr:     true,
			errContains: "cannot read file nonexistent.sql: open nonexistent.sql: no such file or directory",
		},
		{
			name:        "missing schema",
			sqlFlag:     "SELECT 1;",
			vschemaFlag: `{"ks": {}}`,
			wantErr:     true,
			errContains: "action requires one of schema or schema-file",
		},
		{
			name:           "invalid schema file",
			sqlFlag:        "SELECT 1;",
			schemaFileFlag: "invalid.sql",
			vschemaFlag:    `{"ks": {}}`,
			wantErr:        true,
			errContains:    "cannot read file invalid.sql: open invalid.sql: no such file or directory",
		},
		{
			name:        "invalid vschema",
			sqlFlag:     "SELECT 1;",
			schemaFlag:  "CREATE TABLE t (id INT);",
			vschemaFlag: "invalid json",
			wantErr:     true,
			errContains: "syntax error",
		},
		{
			name:            "invalid vschema file",
			sqlFlag:         "SELECT 1;",
			schemaFlag:      "CREATE TABLE t (id INT);",
			vschemaFileFlag: "invalid.json",
			wantErr:         true,
			errContains:     "cannot read file invalid.json: open invalid.json: no such file or directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Backup and restore flags
			oldFlags := backupFlags()
			defer restoreFlags(oldFlags)

			// Set flags for this test case
			sqlFlag = tt.sqlFlag
			sqlFileFlag = tt.sqlFileFlag
			schemaFlag = tt.schemaFlag
			schemaFileFlag = tt.schemaFileFlag
			vschemaFlag = tt.vschemaFlag
			vschemaFileFlag = tt.vschemaFileFlag
			plannerVersionStr = tt.plannerVersionStr

			err := parseAndRun(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestOutputModes(t *testing.T) {
	validVschema := `{
		"ks": {
			"sharded": false,
			"tables": {
				"t": {}
			}
		}
	}`
	validSchema := "CREATE TABLE t (id INT PRIMARY KEY);"

	tests := []struct {
		name       string
		outputMode string
		check      func(string) bool
	}{
		{
			name:       "text output",
			outputMode: "text",
			check: func(s string) bool {
				return s != ""
			},
		},
		{
			name:       "json output",
			outputMode: "json",
			check: func(s string) bool {
				return json.Valid([]byte(s))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldFlags := backupFlags()
			defer restoreFlags(oldFlags)

			sqlFlag = "SELECT * FROM t;"
			schemaFlag = validSchema
			vschemaFlag = validVschema
			outputMode = tt.outputMode

			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() { os.Stdout = oldStdout }()

			err := parseAndRun(context.Background())
			require.NoError(t, err, "Failed to parse and run")

			w.Close()
			var buf bytes.Buffer
			_, err = io.Copy(&buf, r)
			require.NoError(t, err, "Failed to copy output buffer")

			output := buf.String()

			assert.True(t, tt.check(output), "Output check failed")
		})
	}
}

type flags struct {
	sql, sqlFile, schema, schemaFile, vschema, vschemaFile, plannerVersion string
}

func backupFlags() flags {
	return flags{
		sql:            sqlFlag,
		sqlFile:        sqlFileFlag,
		schema:         schemaFlag,
		schemaFile:     schemaFileFlag,
		vschema:        vschemaFlag,
		vschemaFile:    vschemaFileFlag,
		plannerVersion: plannerVersionStr,
	}
}

func restoreFlags(f flags) {
	sqlFlag = f.sql
	sqlFileFlag = f.sqlFile
	schemaFlag = f.schema
	schemaFileFlag = f.schemaFile
	vschemaFlag = f.vschema
	vschemaFileFlag = f.vschemaFile
	plannerVersionStr = f.plannerVersion
}

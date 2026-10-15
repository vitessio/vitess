package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinlogLocation(t *testing.T) {
	tests := []struct {
		name          string
		cnf           Mycnf
		wantBasename  string
		wantIndexPath string
		wantErr       bool
	}{
		{
			name: "absolute log-bin", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/bin-logs/vt-0000000101-bin"},
			wantBasename: "/vt/bin-logs/vt-0000000101-bin", wantIndexPath: "/vt/bin-logs/vt-0000000101-bin.index",
		},
		{
			name: "relative log-bin", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "mysql-bin"},
			wantBasename: "/vt/data/mysql-bin", wantIndexPath: "/vt/data/mysql-bin.index",
		},
		{
			name: "log-bin extension is dropped", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/bin-logs/src-bin.log"},
			wantBasename: "/vt/bin-logs/src-bin", wantIndexPath: "/vt/bin-logs/src-bin.index",
		},
		{
			name: "absolute index without extension", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/b/x", BinLogIndexPath: "/vt/idx/custom"},
			wantBasename: "/vt/b/x", wantIndexPath: "/vt/idx/custom.index",
		},
		{
			name: "relative index with extension", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/b/x", BinLogIndexPath: "custom.idx"},
			wantBasename: "/vt/b/x", wantIndexPath: "/vt/data/custom.idx",
		},
		{name: "empty log-bin", cnf: Mycnf{DataDir: "/vt/data"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			basename, indexPath, err := binlogLocation(&tt.cnf)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBasename, basename)
			assert.Equal(t, tt.wantIndexPath, indexPath)
		})
	}
}

package vtgate

import (
	"fmt"
	"testing"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"github.com/stretchr/testify/require"
)

func TestDDLFlags(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	defer func() {
		*enableOnlineDDL = true
		*enableDirectDDL = true
	}()
	testcases := []struct {
		enableDirectDDL bool
		enableOnlineDDL bool
		sql             string
		wantErr         bool
		err             string
	}{
		{
			enableDirectDDL: false,
			sql:             "create table t (id int)",
			wantErr:         true,
			err:             "direct DDL is disabled",
		}, {
			enableDirectDDL: true,
			sql:             "create table t (id int)",
			wantErr:         false,
		}, {
			enableOnlineDDL: false,
			sql:             "revert vitess_migration 'abc'",
			wantErr:         true,
			err:             "online DDL is disabled",
		},
	}
	for _, testcase := range testcases {
		t.Run(fmt.Sprintf("%s-%v-%v", testcase.sql, testcase.enableDirectDDL, testcase.enableOnlineDDL), func(t *testing.T) {
			*enableDirectDDL = testcase.enableDirectDDL
			*enableOnlineDDL = testcase.enableOnlineDDL
			_, err := executor.Execute(ctx, "TestDDLFlags", session, testcase.sql, nil)
			if testcase.wantErr {
				require.EqualError(t, err, testcase.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

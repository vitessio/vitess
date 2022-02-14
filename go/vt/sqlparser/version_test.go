package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertMySQLVersion(t *testing.T) {
	testcases := []struct {
		version        string
		commentVersion string
		error          string
	}{{
		version:        "5.7.9",
		commentVersion: "50709",
	}, {
		version:        "0008.08.9",
		commentVersion: "80809",
	}, {
		version:        "5.7.9, Vitess - 10.0.1",
		commentVersion: "50709",
	}, {
		version:        "8.1 Vitess - 10.0.1",
		commentVersion: "80100",
	}, {
		version: "Vitess - 10.0.1",
		error:   "MySQL version not correctly setup - Vitess - 10.0.1.",
	}, {
		version:        "5.7.9.22",
		commentVersion: "50709",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.version, func(t *testing.T) {
			output, err := convertMySQLVersionToCommentVersion(tcase.version)
			if tcase.error != "" {
				require.EqualError(t, err, tcase.error)
			} else {
				require.NoError(t, err)
				require.Equal(t, tcase.commentVersion, output)
			}
		})
	}
}

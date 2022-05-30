package vtexplain

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test10020(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	require.NoError(t, err)

	vSchema, err := os.ReadFile("vschema.json")
	require.NoError(t, err)
	opts := defaultTestOpts()
	opts.ExecutionMode = ModeMulti
	opts.NumShards = 2
	opts.Target = "ks1"
	for i := 0; i < 1000; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			vte, err := Init(string(vSchema), string(schema), "", opts)
			require.NoError(t, err, "vtexplain Init error\n%s", string(schema))
			_, err = vte.Run("SELECT AVG(IdY) FROM table_1")
			vte.Stop()
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported: in scatter query: aggregation function 'avg'")
		})
	}
}

func TestCreateFiles(t *testing.T) {
	schema := ""
	vschema := `{
  "ks1": {
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
`

	v := `      "table_%d": {
        "column_vindexes": [
          {
            "column": "idy",
            "name": "hash"
          }
        ]
      }`
	q := `CREATE TABLE table_%d
(
    id     binary(16)                   NOT NULL DEFAULT '0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    stats  tinyint(3) unsigned          NOT NULL DEFAULT '0',
    idy    int(10) unsigned             NOT NULL AUTO_INCREMENT,
    idz    int(10) unsigned             NOT NULL,
    keycol varchar(40) COLLATE utf8_bin NOT NULL,
    PRIMARY KEY (idy, idz),
    UNIQUE KEY idy_something (idy)
) ENGINE = InnoDB
  AUTO_INCREMENT = 139234562
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;

`
	for i := 0; i < 500; i++ {
		schema += fmt.Sprintf(q, i)
		if i != 0 {
			vschema += `,
`
		}
		vschema += fmt.Sprintf(v, i)
	}

	vschema += `
    }
  }
}
`

	require.NoError(t, os.WriteFile("schema.sql", []byte(schema), os.FileMode(0644)))
	require.NoError(t, os.WriteFile("vschema.json", []byte(vschema), os.FileMode(0644)))
}

/*
Copyright 2022 The Vitess Authors.

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

package semantics

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestGenerateInfoSchemaMap(t *testing.T) {
	t.Skip("run manually to re-create the content of the getInfoSchema functions")
	b := new(strings.Builder)

	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:3306)/test")
	require.NoError(t, err)
	defer db.Close()

	collationName := collations.MySQL8().LookupName(collations.SystemCollation.Collation)

	for _, tbl := range informationSchemaTables80 {
		result, err := db.Query(fmt.Sprintf("show columns from information_schema.`%s`", tbl))
		if err != nil {
			t.Logf("error querying table %s: %v", tbl, err)
			continue
		}
		defer result.Close()
		b.WriteString("cols = []vindexes.Column{}\n")
		for result.Next() {
			var r row
			result.Scan(&r.Field, &r.Type, &r.Null, &r.Key, &r.Default, &r.Extra)
			allString := re.FindStringSubmatch(r.Type)
			var typ string
			if allString == nil {
				typ = r.Type
			} else {
				typ = allString[1]
			}
			unsigned := false
			if idx := strings.Index(typ, "unsigned"); idx > 0 {
				typ = typ[:idx-1]
				unsigned = true
			}
			i2 := sqlparser.SQLTypeToQueryType(typ, unsigned)
			if int(i2) == 0 {
				t.Fatalf("%s %s", tbl, r.Field)
			}
			var size, scale int64
			var values string
			switch i2 {
			case sqltypes.Enum, sqltypes.Set:
				values = allString[2]
			default:
				if len(allString) > 1 && allString[2] != "" {
					parts := strings.Split(allString[2], ",")
					size, err = strconv.ParseInt(parts[0], 10, 32)
					require.NoError(t, err)
					if len(parts) > 1 {
						scale, err = strconv.ParseInt(parts[1], 10, 32)
						require.NoError(t, err)
					}
				}
			}
			//  createCol(name string, typ int, collation string, def string, invisible bool, size, scale int32, notNullable bool)
			b.WriteString(fmt.Sprintf("cols = append(cols, createCol(\"%s\", %d, \"%s\", \"%s\", %d, %d, %t, \"%s\"))\n", r.Field, int(i2), collationName, r.Default, size, scale, r.Null == "NO", values))
		}
		b.WriteString(fmt.Sprintf("infSchema[\"%s\"] = cols\n", tbl))
	}

	fmt.Println(b.String())
}

var (
	informationSchemaTables80 = []string{
		"ADMINISTRABLE_ROLE_AUTHORIZATIONS",
		"APPLICABLE_ROLES",
		"CHARACTER_SETS",
		"CHECK_CONSTRAINTS",
		"COLLATION_CHARACTER_SET_APPLICABILITY",
		"COLLATIONS",
		"COLUMN_PRIVILEGES",
		"COLUMN_STATISTICS",
		"COLUMNS",
		"COLUMNS_EXTENSIONS",
		"ENABLED_ROLES",
		"ENGINES",
		"EVENTS",
		"FILES",
		"GLOBAL_STATUS",
		"GLOBAL_VARIABLES",
		"INNODB_BUFFER_PAGE",
		"INNODB_BUFFER_PAGE_LRU",
		"INNODB_BUFFER_POOL_STATS",
		"INNODB_CACHED_INDEXES",
		"INNODB_CMP",
		"INNODB_CMP_PER_INDEX",
		"INNODB_CMP_PER_INDEX_RESET",
		"INNODB_CMP_RESET",
		"INNODB_CMPMEM",
		"INNODB_CMPMEM_RESET",
		"INNODB_COLUMNS",
		"INNODB_DATAFILES",
		"INNODB_FIELDS",
		"INNODB_FOREIGN",
		"INNODB_FOREIGN_COLS",
		"INNODB_FT_BEING_DELETED",
		"INNODB_FT_CONFIG",
		"INNODB_FT_DEFAULT_STOPWORD",
		"INNODB_FT_DELETED",
		"INNODB_FT_INDEX_CACHE",
		"INNODB_FT_INDEX_TABLE",
		"INNODB_INDEXES",
		"INNODB_METRICS",
		"INNODB_SESSION_TEMP_TABLESPACES",
		"INNODB_TABLES",
		"INNODB_TABLESPACES",
		"INNODB_TABLESPACES_BRIEF",
		"INNODB_TABLESTATS",
		"INNODB_TEMP_TABLE_INFO",
		"INNODB_TRX",
		"INNODB_VIRTUAL",
		"KEY_COLUMN_USAGE",
		"KEYWORDS",
		"OPTIMIZER_TRACE",
		"PARAMETERS",
		"PARTITIONS",
		"PLUGINS",
		"PROCESSLIST",
		"PROFILING",
		"REFERENTIAL_CONSTRAINTS",
		"RESOURCE_GROUPS",
		"ROLE_COLUMN_GRANTS",
		"ROLE_ROUTINE_GRANTS",
		"ROLE_TABLE_GRANTS",
		"ROUTINES",
		"SCHEMA_PRIVILEGES",
		"SCHEMATA",
		"SCHEMATA_EXTENSIONS",
		"ST_GEOMETRY_COLUMNS",
		"ST_SPATIAL_REFERENCE_SYSTEMS",
		"ST_UNITS_OF_MEASURE",
		"STATISTICS",
		"TABLE_CONSTRAINTS",
		"TABLE_CONSTRAINTS_EXTENSIONS",
		"TABLE_PRIVILEGES",
		"TABLES",
		"TABLES_EXTENSIONS",
		"TABLESPACES",
		"TABLESPACES_EXTENSIONS",
		"TRIGGERS",
		"USER_ATTRIBUTES",
		"USER_PRIVILEGES",
		"VIEW_ROUTINE_USAGE",
		"VIEW_TABLE_USAGE",
		"VIEWS",
	}
)

type row struct {
	Field   string
	Type    string
	Null    string
	Key     any
	Default string
	Extra   any
}

var re = regexp.MustCompile(`(.*)\((.*)\)`)

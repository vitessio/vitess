/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestNormalize(t *testing.T) {
	prefix := "bv"
	testcases := []struct {
		in      string
		outstmt string
		outbv   map[string]*querypb.BindVariable
	}{{
		// str val
		in:      "select * from t where v1 = 'aa'",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte("aa")),
		},
	}, {
		// placeholder
		in:      "select * from t where col=?",
		outstmt: "select * from t where col = :v1",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// qualified table name
		in:      "select * from `t` where col=?",
		outstmt: "select * from t where col = :v1",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// str val in select
		in:      "select 'aa' from t",
		outstmt: "select :bv1 from t",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte("aa")),
		},
	}, {
		// int val
		in:      "select * from t where v1 = 1",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// float val
		in:      "select * from t where v1 = 1.2",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Float64BindVariable(1.2),
		},
	}, {
		// multiple vals
		in:      "select * from t where v1 = 1.2 and v2 = 2",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Float64BindVariable(1.2),
			"bv2": sqltypes.Int64BindVariable(2),
		},
	}, {
		// bv collision
		in:      "select * from t where v1 = :bv1 and v2 = 1",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv2": sqltypes.Int64BindVariable(1),
		},
	}, {
		// val reuse
		in:      "select * from t where v1 = 1 and v2 = 1",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// ints and strings are different
		in:      "select * from t where v1 = 1 and v2 = '1'",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.BytesBindVariable([]byte("1")),
		},
	}, {
		// val should not be reused for non-select statements
		in:      "insert into a values(1, 1)",
		outstmt: "insert into a values (:bv1, :bv2)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(1),
		},
	}, {
		// val should be reused only in subqueries of DMLs
		in:      "update a set v1=(select 5 from t), v2=5, v3=(select 5 from t), v4=5",
		outstmt: "update a set v1 = (select :bv1 from t), v2 = :bv2, v3 = (select :bv1 from t), v4 = :bv3",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(5),
			"bv2": sqltypes.Int64BindVariable(5),
			"bv3": sqltypes.Int64BindVariable(5),
		},
	}, {
		// list vars should work for DMLs also
		in:      "update a set v1=5 where v2 in (1, 4, 5)",
		outstmt: "update a set v1 = :bv1 where v2 in ::bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(5),
			"bv2": sqltypes.TestBindVariable([]interface{}{1, 4, 5}),
		},
	}, {
		// Hex value does not convert
		in:      "select * from t where v1 = 0x1234",
		outstmt: "select * from t where v1 = 0x1234",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Hex value does not convert for DMLs
		in:      "update a set v1 = 0x1234",
		outstmt: "update a set v1 = 0x1234",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Bin value does not convert
		in:      "select * from t where v1 = b'11'",
		outstmt: "select * from t where v1 = B'11'",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Bin value does not convert for DMLs
		in:      "update a set v1 = b'11'",
		outstmt: "update a set v1 = B'11'",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// ORDER BY column_position
		in:      "select a, b from t order by 1 asc",
		outstmt: "select a, b from t order by 1 asc",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// ORDER BY variable
		in:      "select a, b from t order by c asc",
		outstmt: "select a, b from t order by c asc",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Values up to len 256 will reuse.
		in:      fmt.Sprintf("select * from t where v1 = '%256s' and v2 = '%256s'", "a", "a"),
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte(fmt.Sprintf("%256s", "a"))),
		},
	}, {
		// Values greater than len 256 will not reuse.
		in:      fmt.Sprintf("select * from t where v1 = '%257s' and v2 = '%257s'", "b", "b"),
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte(fmt.Sprintf("%257s", "b"))),
			"bv2": sqltypes.BytesBindVariable([]byte(fmt.Sprintf("%257s", "b"))),
		},
	}, {
		// bad int
		in:      "select * from t where v1 = 12345678901234567890",
		outstmt: "select * from t where v1 = 12345678901234567890",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// comparison with no vals
		in:      "select * from t where v1 = v2",
		outstmt: "select * from t where v1 = v2",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// IN clause with existing bv
		in:      "select * from t where v1 in ::list",
		outstmt: "select * from t where v1 in ::list",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// IN clause with non-val values
		in:      "select * from t where v1 in (1, a)",
		outstmt: "select * from t where v1 in (:bv1, a)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// IN clause with vals
		in:      "select * from t where v1 in (1, '2')",
		outstmt: "select * from t where v1 in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]interface{}{1, []byte("2")}),
		},
	}, {
		// NOT IN clause
		in:      "select * from t where v1 not in (1, '2')",
		outstmt: "select * from t where v1 not in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]interface{}{1, []byte("2")}),
		},
	}, {
		// Do not normalize cast/convert types
		in:      `select CAST("test" AS CHAR(60))`,
		outstmt: `select convert(:bv1, CHAR(60)) from dual`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.StringBindVariable("test"),
		},
	}, {
		// insert syntax
		in:      "insert into a (v1, v2, v3) values (1, '2', 3)",
		outstmt: "insert into a(v1, v2, v3) values (:bv1, :bv2, :bv3)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.StringBindVariable("2"),
			"bv3": sqltypes.Int64BindVariable(3),
		},
	}}
	for _, tc := range testcases {
		stmt, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		known := GetBindvars(stmt)
		bv := make(map[string]*querypb.BindVariable)
		require.NoError(t, Normalize(stmt, NewReservedVars(prefix, known), bv))
		outstmt := String(stmt)
		if outstmt != tc.outstmt {
			t.Errorf("Query:\n%s:\n%s, want\n%s", tc.in, outstmt, tc.outstmt)
		}
		if !reflect.DeepEqual(tc.outbv, bv) {
			t.Errorf("Query:\n%s:\n%v, want\n%v", tc.in, bv, tc.outbv)
		}
	}
}

func TestGetBindVars(t *testing.T) {
	stmt, err := Parse("select * from t where :v1 = :v2 and :v2 = :v3 and :v4 in ::v5")
	if err != nil {
		t.Fatal(err)
	}
	got := GetBindvars(stmt)
	want := map[string]struct{}{
		"v1": {},
		"v2": {},
		"v3": {},
		"v4": {},
		"v5": {},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetBindVars: %v, want: %v", got, want)
	}
}

/*
Skipping ColName, TableName:
BenchmarkNormalize-8     1000000              2205 ns/op             821 B/op         27 allocs/op
Prior to skip:
BenchmarkNormalize-8      500000              3620 ns/op            1461 B/op         55 allocs/op
*/
func BenchmarkNormalize(b *testing.B) {
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	ast, reservedVars, err := Parse2(sql)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		require.NoError(b, Normalize(ast, NewReservedVars("", reservedVars), map[string]*querypb.BindVariable{}))
	}
}

func BenchmarkNormalizeTraces(b *testing.B) {
	for _, trace := range []string{"django_queries.txt", "lobsters.sql.gz"} {
		b.Run(trace, func(b *testing.B) {
			queries := loadQueries(b, trace)
			if len(queries) > 10000 {
				queries = queries[:10000]
			}

			parsed := make([]Statement, 0, len(queries))
			reservedVars := make([]BindVars, 0, len(queries))
			for _, q := range queries {
				pp, kb, err := Parse2(q)
				if err != nil {
					b.Fatal(err)
				}
				parsed = append(parsed, pp)
				reservedVars = append(reservedVars, kb)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for i, query := range parsed {
					_ = Normalize(query, NewReservedVars("", reservedVars[i]), map[string]*querypb.BindVariable{})
				}
			}
		})
	}
}

func BenchmarkNormalizeVTGate(b *testing.B) {
	const keyspace = "main_keyspace"

	queries := loadQueries(b, "lobsters.sql.gz")
	if len(queries) > 10000 {
		queries = queries[:10000]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			stmt, reservedVars, err := Parse2(sql)
			if err != nil {
				b.Fatal(err)
			}

			query := sql
			statement := stmt
			bindVarNeeds := &BindVarNeeds{}
			bindVars := make(map[string]*querypb.BindVariable)
			_ = IgnoreMaxMaxMemoryRowsDirective(stmt)

			// Normalize if possible and retry.
			if CanNormalize(stmt) || MustRewriteAST(stmt) {
				result, err := PrepareAST(stmt, NewReservedVars("vtg", reservedVars), bindVars, true, keyspace)
				if err != nil {
					b.Fatal(err)
				}
				statement = result.AST
				bindVarNeeds = result.BindVarNeeds
				query = String(statement)
			}

			_ = query
			_ = statement
			_ = bindVarNeeds
		}
	}
}

func randtmpl(template string) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const numberBytes = "0123456789"

	result := []byte(template)
	for i, c := range result {
		switch c {
		case '#':
			result[i] = numberBytes[rand.Intn(len(numberBytes))]
		case '@':
			result[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
	}
	return string(result)
}

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func BenchmarkNormalizeTPCCBinds(b *testing.B) {
	query := `INSERT IGNORE INTO customer0
(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
values
(:c_id, :c_d_id, :c_w_id, :c_first, :c_middle, :c_last, :c_street_1, :c_street_2, :c_city, :c_state, :c_zip, :c_phone, :c_since, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_ytd_payment, :c_payment_cnt, :c_delivery_cnt, :c_data)`
	benchmarkNormalization(b, []string{query})
}

func BenchmarkNormalizeTPCCInsert(b *testing.B) {
	generateInsert := func(rows int) string {
		var query bytes.Buffer
		query.WriteString("INSERT IGNORE INTO customer0 (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) values ")
		for i := 0; i < rows; i++ {
			fmt.Fprintf(&query, "(%d, %d, %d, '%s','OE','%s','%s', '%s', '%s', '%s', '%s','%s',NOW(),'%s',50000,%f,-10,10,1,0,'%s' )",
				rand.Int(), rand.Int(), rand.Int(),
				"first-"+randString(rand.Intn(10)),
				randtmpl("last-@@@@"),
				randtmpl("street1-@@@@@@@@@@@@"),
				randtmpl("street2-@@@@@@@@@@@@"),
				randtmpl("city-@@@@@@@@@@@@"),
				randtmpl("@@"), randtmpl("zip-#####"),
				randtmpl("################"),
				"GC", rand.Float64(), randString(300+rand.Intn(200)),
			)
			if i < rows-1 {
				query.WriteString(", ")
			}
		}
		return query.String()
	}

	var queries []string

	for i := 0; i < 1024; i++ {
		queries = append(queries, generateInsert(4))
	}

	benchmarkNormalization(b, queries)
}

func BenchmarkNormalizeTPCC(b *testing.B) {
	templates := []string{
		`SELECT c_discount, c_last, c_credit, w_tax
FROM customer%d AS c
JOIN warehouse%d AS w ON c_w_id=w_id
WHERE w_id = %d
AND c_d_id = %d
AND c_id = %d`,
		`SELECT d_next_o_id, d_tax 
FROM district%d 
WHERE d_w_id = %d 
AND d_id = %d FOR UPDATE`,
		`UPDATE district%d
SET d_next_o_id = %d
WHERE d_id = %d AND d_w_id= %d`,
		`INSERT INTO orders%d
(o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)
VALUES (%d,%d,%d,%d,NOW(),%d,%d)`,
		`INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id)
VALUES (%d,%d,%d)`,
		`SELECT i_price, i_name, i_data 
FROM item%d
WHERE i_id = %d`,
		`SELECT s_quantity, s_data, s_dist_%s s_dist 
FROM stock%d  
WHERE s_i_id = %d AND s_w_id= %d FOR UPDATE`,
		`UPDATE stock%d
SET s_quantity = %d
WHERE s_i_id = %d 
AND s_w_id= %d`,
		`INSERT INTO order_line%d
(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
VALUES (%d,%d,%d,%d,%d,%d,%d,%d,'%s')`,
		`UPDATE warehouse%d
SET w_ytd = w_ytd + %d 
WHERE w_id = %d`,
		`SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
FROM warehouse%d
WHERE w_id = %d`,
		`UPDATE district%d 
SET d_ytd = d_ytd + %d 
WHERE d_w_id = %d 
AND d_id= %d`,
		`SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name 
FROM district%d
WHERE d_w_id = %d 
AND d_id = %d`,
		`SELECT count(c_id) namecnt
FROM customer%d
WHERE c_w_id = %d 
AND c_d_id= %d
AND c_last='%s'`,
		`SELECT c_first, c_middle, c_last, c_street_1,
c_street_2, c_city, c_state, c_zip, c_phone,
c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since
FROM customer%d
WHERE c_w_id = %d 
AND c_d_id= %d
AND c_id=%d FOR UPDATE`,
		`SELECT c_data
FROM customer%d
WHERE c_w_id = %d 
AND c_d_id=%d
AND c_id= %d`,
		`UPDATE customer%d
SET c_balance=%f, c_ytd_payment=%f, c_data='%s'
WHERE c_w_id = %d 
AND c_d_id=%d
AND c_id=%d`,
		`UPDATE customer%d
SET c_balance=%f, c_ytd_payment=%f
WHERE c_w_id = %d 
AND c_d_id=%d
AND c_id=%d`,
		`INSERT INTO history%d
(h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data)
VALUES (%d,%d,%d,%d,%d,NOW(),%d,'%s')`,
		`SELECT count(c_id) namecnt
FROM customer%d
WHERE c_w_id = %d 
AND c_d_id= %d
AND c_last='%s'`,
		`SELECT c_balance, c_first, c_middle, c_id
FROM customer%d
WHERE c_w_id = %d 
AND c_d_id= %d
AND c_last='%s' ORDER BY c_first`,
		`SELECT c_balance, c_first, c_middle, c_last
FROM customer%d
WHERE c_w_id = %d 
AND c_d_id=%d
AND c_id=%d`,
		`SELECT o_id, o_carrier_id, o_entry_d
FROM orders%d 
WHERE o_w_id = %d 
AND o_d_id = %d 
AND o_c_id = %d 
ORDER BY o_id DESC`,
		`SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
FROM order_line%d WHERE ol_w_id = %d AND ol_d_id = %d  AND ol_o_id = %d`,
		`SELECT no_o_id
FROM new_orders%d 
WHERE no_d_id = %d 
AND no_w_id = %d 
ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE`,
		`DELETE FROM new_orders%d
WHERE no_o_id = %d 
AND no_d_id = %d  
AND no_w_id = %d`,
		`SELECT o_c_id
FROM orders%d 
WHERE o_id = %d 
AND o_d_id = %d 
AND o_w_id = %d`,
		`UPDATE orders%d 
SET o_carrier_id = %d
WHERE o_id = %d 
AND o_d_id = %d 
AND o_w_id = %d`,
		`UPDATE order_line%d 
SET ol_delivery_d = NOW()
WHERE ol_o_id = %d 
AND ol_d_id = %d 
AND ol_w_id = %d`,
		`SELECT SUM(ol_amount) sm
FROM order_line%d 
WHERE ol_o_id = %d 
AND ol_d_id = %d 
AND ol_w_id = %d`,
		`UPDATE customer%d 
SET c_balance = c_balance + %f,
c_delivery_cnt = c_delivery_cnt + 1
WHERE c_id = %d 
AND c_d_id = %d 
AND c_w_id = %d`,
		`SELECT d_next_o_id 
FROM district%d
WHERE d_id = %d AND d_w_id= %d`,
		`SELECT COUNT(DISTINCT(s.s_i_id))
FROM stock%d AS s
JOIN order_line%d AS ol ON ol.ol_w_id=s.s_w_id AND ol.ol_i_id=s.s_i_id			
WHERE ol.ol_w_id = %d 
AND ol.ol_d_id = %d
AND ol.ol_o_id < %d 
AND ol.ol_o_id >= %d
AND s.s_w_id= %d
AND s.s_quantity < %d `,
		`SELECT DISTINCT ol_i_id FROM order_line%d
WHERE ol_w_id = %d AND ol_d_id = %d
AND ol_o_id < %d AND ol_o_id >= %d`,
		`SELECT count(*) FROM stock%d
WHERE s_w_id = %d AND s_i_id = %d
AND s_quantity < %d`,
		`SELECT min(no_o_id) mo
FROM new_orders%d 
WHERE no_w_id = %d AND no_d_id = %d`,
		`SELECT o_id FROM orders%d o, (SELECT o_c_id,o_w_id,o_d_id,count(distinct o_id) FROM orders%d WHERE o_w_id=%d AND o_d_id=%d AND o_id > 2100 AND o_id < %d GROUP BY o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t WHERE t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1 `,
		`DELETE FROM order_line%d where ol_w_id=%d AND ol_d_id=%d AND ol_o_id=%d`,
		`DELETE FROM orders%d where o_w_id=%d AND o_d_id=%d and o_id=%d`,
		`DELETE FROM history%d where h_w_id=%d AND h_d_id=%d LIMIT 10`,
	}

	re := regexp.MustCompile(`%\w`)
	repl := func(m string) string {
		switch m {
		case "%s":
			return "RANDOM_STRING"
		case "%d":
			return strconv.Itoa(rand.Int())
		case "%f":
			return strconv.FormatFloat(rand.Float64(), 'f', 8, 64)
		default:
			panic(m)
		}
	}

	var queries []string

	for _, tmpl := range templates {
		for i := 0; i < 128; i++ {
			queries = append(queries, re.ReplaceAllStringFunc(tmpl, repl))
		}
	}

	benchmarkNormalization(b, queries)
}

func benchmarkNormalization(b *testing.B, sqls []string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			stmt, reserved, err := Parse2(sql)
			if err != nil {
				b.Fatalf("%v: %q", err, sql)
			}

			reservedVars := NewReservedVars("vtg", reserved)
			_, err = PrepareAST(stmt, reservedVars, make(map[string]*querypb.BindVariable), true, "keyspace0")
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

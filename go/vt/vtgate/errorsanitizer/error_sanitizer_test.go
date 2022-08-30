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

package errorsanitizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorNormalization(t *testing.T) {
	testCases := []struct {
		name, input, output string
	}{
		{
			"no change",
			"nothing to normalize here",
			"nothing to normalize here",
		},
		{
			"no truncation after 'code ='",
			`target: mars.-.primary: vttablet: rpc error: code = InvalidArgument desc = Unknown system variable 'query_response_time_stats' (errno 1193) (sqlstate HY000) (CallerID: admin): Sql: \"select @@query_response_time_stats from dual\", BindVars: {}: 2760`,
			`target: mars.-.primary: vttablet: rpc error: code = InvalidArgument desc = Unknown system variable 'query_response_time_stats' (errno 1193) (sqlstate HY000) (CallerID: admin)`,
		},
		{
			"replace after 'Duplicate entry'",
			`target: keep3rv1.-.primary: vttablet: Duplicate entry '0' for key 'stats.id' (errno 1062) (sqlstate 23000) (CallerID: admin): Sql: \"insert into stats(jobs, work_done, keepers, rewarded_kp3r, bonded_kp3r) values (:v1, :v2, :v3, :v4, :v5)\", BindVars: {v1: \"type:VARBINARY value:\\\"0\\\"\"v2: \"type:VARBINARY value:\\\"1\\\"\"v3: \"type:VARBINARY value:\\\"0\\\"\"v4: \"type:VARBINARY value:\\\"0\\\"\"v5: \"type:VARBINARY value:\\\"0\\\"\"}`,
			`target: keep3rv1.-.primary: vttablet: Duplicate entry '<val>' for key 'stats.id' (errno 1062) (sqlstate 23000) (CallerID: admin)`,
		},
		{
			"malicious 'Duplicate entry'", // the entry value `this ' for key` tries to trick the regexp
			`target: keep3rv1.-.primary: vttablet: Duplicate entry 'this ' for key' for key 'stats.id' (errno 1062) (sqlstate 23000) (CallerID: admin): Sql: \"insert into stats(jobs, work_done, keepers, rewarded_kp3r, bonded_kp3r) values (:v1, :v2, :v3, :v4, :v5)\", BindVars: {v1: \"type:VARBINARY value:\\\"0\\\"\"v2: \"type:VARBINARY value:\\\"1\\\"\"v3: \"type:VARBINARY value:\\\"0\\\"\"v4: \"type:VARBINARY value:\\\"0\\\"\"v5: \"type:VARBINARY value:\\\"0\\\"\"}`,
			`target: keep3rv1.-.primary: vttablet: Duplicate entry '<val>' for key 'stats.id' (errno 1062) (sqlstate 23000) (CallerID: admin)`,
		},
		{
			"malicious key name", // the key name `column ' for key` actually does trick the regexp
			`target: keep3rv1.-.primary: vttablet: Duplicate entry 'this' for key 'column ' for key' (errno 1062) (sqlstate 23000) (CallerID: admin): Sql: \"insert into stats(jobs, work_done, keepers, rewarded_kp3r, bonded_kp3r) values (:v1, :v2, :v3, :v4, :v5)\", BindVars: {v1: \"type:VARBINARY value:\\\"0\\\"\"v2: \"type:VARBINARY value:\\\"1\\\"\"v3: \"type:VARBINARY value:\\\"0\\\"\"v4: \"type:VARBINARY value:\\\"0\\\"\"v5: \"type:VARBINARY value:\\\"0\\\"\"}`,
			`target: keep3rv1.-.primary: vttablet: Duplicate entry '<val>' for key' (errno 1062) (sqlstate 23000) (CallerID: admin)`,
			// should be:
			// `target: keep3rv1.-.primary: vttablet: Duplicate entry '<val>' for key 'column ' for key' (errno 1062) (sqlstate 23000) (CallerID: admin)`,
		},
		{
			"truncates after 'syntax error'",
			`syntax error at position 42 near 'WHERE'`,
			`syntax error at position 42`,
		},
		{
			"removes Sql after colon",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin): Sql: \"insert into User(id, firstName, lastName, password, phone, email, citizenship, dob, address_street1, address_city, address_state, address_zip, address_country, piiSsn, signupIpAddress, createdAt, updatedAt) values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12, :v13, :v14, :v15, :v16, :v17)\", BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin)`,
		},
		{
			"removes Sql after comma",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin): Junk: "whatever", Sql: \"insert into User(id, firstName, lastName, password, phone, email, citizenship, dob, address_street1, address_city, address_state, address_zip, address_country, piiSsn, signupIpAddress, createdAt, updatedAt) values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12, :v13, :v14, :v15, :v16, :v17)\", BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin): Junk: "whatever"`,
		},
		{
			"removes BindVars after colon",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin): BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin)`,
		},
		{
			"removes BindVars after comma",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin): Junk: "whatever", BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: admin): Junk: "whatever"`,
		},
		{
			"truncates very long strings",
			"Doloribus quo ullam labore nostrum nihil dolore nemo. Ad molestiae ab at dolores et. Iusto adipisci tempora et quia blanditiis et.  Velit alias eos quia et velit. Impedit ipsa itaque facilis repellendus. Quidem fuga sit voluptas minus. Neque amet et necessitatibus voluptatum. Voluptatem eum consequatur et dolor. Nulla deserunt quia cum ea hic architecto.  Eum et sed quo et officia nostrum eos. Nam quisquam et dolor repellat. Ea aperiam iste et.  Sint commodi non ut non occaecati velit. Architecto et fuga alias blanditiis consequatur qui ipsa magnam. Ea velit mollitia sed eligendi dolor et. Commodi et non sint optio asperiores.  Et dolores id corrupti voluptatum quasi voluptatem ipsam voluptatem. Dolorum et natus fugit. Ad id ea laudantium adipisci molestiae ratione eum quisquam.",
			"Doloribus quo ullam labore nostrum nihil dolore nemo. Ad molestiae ab at dolores et. Iusto adipisci tempora et quia blanditiis et.  Velit alias eos quia et velit. Impedit ipsa itaque facilis repellendus. Quidem fuga sit voluptas minus. Neque amet et necessitatibus voluptatum. Voluptatem eum consequatur et dolor. Nulla deserunt quia cum ea hic architecto.  Eum et sed quo et officia ",
		},
		{
			"removes 'incorrect' string value literals",
			"target: targ.-.primary: vttablet: rpc error: code = InvalidArgument desc = Incorrect string value: '\\xC3?A' for column 'name' at row 253 (errno 1366) (sqlstate HY000) (CallerID: admin)",
			"target: targ.-.primary: vttablet: rpc error: code = InvalidArgument desc = Incorrect string value: '<val>' for column 'name' at row 253 (errno 1366) (sqlstate HY000) (CallerID: admin)",
		},
		{
			"removes 'incorrect' datetime  value literals",
			"target: targ.-.primary: vttablet: rpc error: code = InvalidArgument desc = Incorrect datetime value: '1991-09-29 NULL' for column 'startdate' at row 1 (errno 1292) (sqlstate 22007) (CallerID: admin)",
			"target: targ.-.primary: vttablet: rpc error: code = InvalidArgument desc = Incorrect datetime value: '<val>' for column 'startdate' at row 1 (errno 1292) (sqlstate 22007) (CallerID: admin)",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := NormalizeError(tc.input)
			assert.Equal(t, tc.output, op)
		})
	}
}

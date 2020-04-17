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

package apps

import (
	"testing"
	"time"

	"vitess.io/vitess/go/test/utils"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestReadLogLine(t *testing.T) {
	type test struct {
		logLine  string
		expected LogEntry
	}

	tests := []test{
		{
			logLine: "2020-03-02T15:06:48.894157Z	    2 Connect	root@localhost on  using Socket",
			expected: LogEntry{
				ConnectionID: 2,
				Time:         time.Date(2020, 3, 2, 15, 6, 48, 894157*1000, time.UTC),
				Typ:          Connect,
				Text:         "root@localhost on  using Socket",
			},
		},
		{
			logLine: "2020-03-02T15:08:22.551372Z	    5 Quit	",
			expected: LogEntry{
				ConnectionID: 5,
				Time:         time.Date(2020, 3, 2, 15, 8, 22, 551372*1000, time.UTC),
				Typ:          Quit,
				Text:         "",
			},
		},
		{
			logLine: "2020-03-02T15:15:24.533709Z	    6 Init DB	wordpressdb",
			expected: LogEntry{
				ConnectionID: 6,
				Time:         time.Date(2020, 3, 2, 15, 15, 24, 533709*1000, time.UTC),
				Typ:          InitDb,
				Text:         "wordpressdb",
			},
		},
		{
			logLine: "2020-03-02T15:07:32.400439Z	    5 Query	select @@version_comment limit 1",
			expected: LogEntry{
				ConnectionID: 5,
				Time:         time.Date(2020, 3, 2, 15, 7, 32, 400439*1000, time.UTC),
				Typ:          Query,
				Text:         "select @@version_comment limit 1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.logLine, func(t *testing.T) {
			actual, success := readLogLine(test.logLine)
			require.True(t, success, "should be successful")
			if diff := cmp.Diff(test.expected, actual); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestReadFullLog(t *testing.T) {
	input := `2020-03-02T15:07:32.400210Z	    5 Connect	root@localhost on  using Socket
2020-03-02T15:07:32.400439Z	    5 Query	select @@version_comment limit 1
2020-03-02T15:08:04.272295Z	    5 Query	select 42
2020-03-02T15:08:22.551372Z	    5 Quit	`
	result, err := ReadLogLines(input)
	require.NoError(t, err)

	expected := []*LogEntry{{
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 7, 32, 400210*1000, time.UTC),
		Typ:          Connect,
		Text:         "root@localhost on  using Socket",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 7, 32, 400439*1000, time.UTC),
		Typ:          Query,
		Text:         "select @@version_comment limit 1",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 8, 4, 272295*1000, time.UTC),
		Typ:          Query,
		Text:         "select 42",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 8, 22, 551372*1000, time.UTC),
		Typ:          Quit,
		Text:         "",
	}}

	utils.MustMatch(t, expected, result, "reading logs")
}

func TestReadFullLogWithInterleavedChunks(t *testing.T) {
	input := `2020-03-02T15:07:32.400210Z	    5 Connect	root@localhost on  using Socket
2020-03-02T15:07:32.400439Z	    5 Query	select @@version_comment limit 1
2020-03-02T15:15:24.532950Z	    6 Connect	wp_user@localhost on  using TCP/IP
2020-03-02T15:08:04.272295Z	    5 Query	select 42
2020-03-02T15:15:24.533709Z	    6 Init DB	wordpressdb
2020-03-02T15:15:24.533921Z	    6 Query	SELECT wp_
2020-03-02T15:08:22.551372Z	    5 Quit	
2020-03-02T15:15:24.536723Z	    6 Quit	`
	result, err := ReadLogLines(input)
	require.NoError(t, err)

	expected := []*LogEntry{{
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 7, 32, 400210*1000, time.UTC),
		Typ:          Connect,
		Text:         "root@localhost on  using Socket",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 7, 32, 400439*1000, time.UTC),
		Typ:          Query,
		Text:         "select @@version_comment limit 1",
	}, {
		ConnectionID: 6,
		Time:         time.Date(2020, 3, 2, 15, 15, 24, 532950*1000, time.UTC),
		Typ:          Connect,
		Text:         "wp_user@localhost on  using TCP/IP",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 8, 4, 272295*1000, time.UTC),
		Typ:          Query,
		Text:         "select 42",
	}, {
		ConnectionID: 6,
		Time:         time.Date(2020, 3, 2, 15, 15, 24, 533709*1000, time.UTC),
		Typ:          InitDb,
		Text:         "wordpressdb",
	}, {
		ConnectionID: 6,
		Time:         time.Date(2020, 3, 2, 15, 15, 24, 533921*1000, time.UTC),
		Typ:          Query,
		Text:         "SELECT wp_",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 8, 22, 551372*1000, time.UTC),
		Typ:          Quit,
		Text:         "",
	}, {
		ConnectionID: 6,
		Time:         time.Date(2020, 3, 2, 15, 15, 24, 536723*1000, time.UTC),
		Typ:          Quit,
		Text:         "",
	}}

	if diff := cmp.Diff(expected, result); diff != "" {
		t.Error(diff)
	}
}

func TestReadFullLogWithMultiLineQueries(t *testing.T) {
	input := `2020-03-02T15:07:32.400210Z	    5 Connect	root@localhost on  using Socket
2020-03-02T15:16:50.431748Z	    5 Query	CREATE TABLE wp_users (
	ID bigint(20) unsigned NOT NULL auto_increment,
	user_login varchar(60) NOT NULL default '',
	user_pass varchar(255) NOT NULL default '',
	user_nicename varchar(50) NOT NULL default '',
	user_email varchar(100) NOT NULL default '',
	user_url varchar(100) NOT NULL default '',
	user_registered datetime NOT NULL default '0000-00-00 00:00:00',
	user_activation_key varchar(255) NOT NULL default '',
	user_status int(11) NOT NULL default '0',
	display_name varchar(250) NOT NULL default '',
	PRIMARY KEY  (ID),
	KEY user_login_key (user_login),
	KEY user_nicename (user_nicename),
	KEY user_email (user_email)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci
2020-03-02T15:08:22.551372Z	    5 Quit	`
	result, err := ReadLogLines(input)
	require.NoError(t, err)

	expected := []*LogEntry{{
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 7, 32, 400210*1000, time.UTC),
		Typ:          Connect,
		Text:         "root@localhost on  using Socket",
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 16, 50, 431748*1000, time.UTC),
		Typ:          Query,
		Text: `CREATE TABLE wp_users (
	ID bigint(20) unsigned NOT NULL auto_increment,
	user_login varchar(60) NOT NULL default '',
	user_pass varchar(255) NOT NULL default '',
	user_nicename varchar(50) NOT NULL default '',
	user_email varchar(100) NOT NULL default '',
	user_url varchar(100) NOT NULL default '',
	user_registered datetime NOT NULL default '0000-00-00 00:00:00',
	user_activation_key varchar(255) NOT NULL default '',
	user_status int(11) NOT NULL default '0',
	display_name varchar(250) NOT NULL default '',
	PRIMARY KEY  (ID),
	KEY user_login_key (user_login),
	KEY user_nicename (user_nicename),
	KEY user_email (user_email)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci`,
	}, {
		ConnectionID: 5,
		Time:         time.Date(2020, 3, 2, 15, 8, 22, 551372*1000, time.UTC),
		Typ:          Quit,
		Text:         "",
	}}

	utils.MustMatch(t, expected, result, "")
}

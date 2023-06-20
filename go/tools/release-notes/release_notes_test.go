/*
Copyright 2021 The Vitess Authors.

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

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
)

func Test_groupPRs(t *testing.T) {
	tests := []struct {
		name    string
		prInfos []prInfo
		want    map[string]map[string][]prInfo
	}{
		{
			name:    "Single PR info with no labels",
			prInfos: []prInfo{{Title: "pr 1", Number: 1}},
			want:    map[string]map[string][]prInfo{"Other": {"Other": []prInfo{{Title: "pr 1", Number: 1}}}},
		}, {
			name:    "Single PR info with type label",
			prInfos: []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}}}},
			want:    map[string]map[string][]prInfo{"Bug fixes": {"Other": []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}}}}}}},
		{
			name:    "Single PR info with type and component labels",
			prInfos: []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}},
			want:    map[string]map[string][]prInfo{"Bug fixes": {"VTGate": []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}}}},
		{
			name: "Multiple PR infos with type and component labels", prInfos: []prInfo{
				{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}},
				{Title: "pr 2", Number: 2, Labels: []label{{Name: prefixType + "Feature"}, {Name: prefixComponent + "VTTablet"}}}},
			want: map[string]map[string][]prInfo{"Bug fixes": {"VTGate": []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}}, "Feature": {"VTTablet": []prInfo{{Title: "pr 2", Number: 2, Labels: []label{{Name: prefixType + "Feature"}, {Name: prefixComponent + "VTTablet"}}}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := groupPRs(tt.prInfos)
			utils.MustMatch(t, tt.want, got)
		})
	}
}

func TestParseGitLogOutput(t *testing.T) {
	in := `harshTEST@planetscale.com	Merge pull request #7968 from planetscale/bump_java_snapshot_v11	7e8ebbb5b79b65d2d45fd6c838efb51bdafc7c0b 195a09df191d3e86a32ebcc7a1f1dde168fe819e	168fe819e
deeptTEST@planetscale.com	Merge pull request #7970 from planetscale/vttestserver-default-charset	887be6914690b6d106aba001c72deea80a4d8dab ff8c750eda4b30787e772547a451ed1f50931150	f50931150
deeptTEST@planetscale.com	Merge pull request #7943 from planetscale/fix-mysql80-container-image	01fb7e55ab92df7c3f300b85976fdf3fd5bd35b3 3cc94a10752014c9ce311d88af9e1aa18e7fa2d8	18e7fa2d8
57520317+rohit-nayak-TEST@users.noreply.github.com	Merge pull request #7831 from planetscale/rn-vr-log2	37c09d3be83922a8ef936fbc028a5031f96b7dbf f57350c3ea1720496e5f1cec35d58f069e4df515	69e4df515
TEST@planetscale.com	docker/vttestserver/run.sh:  Add $CHARSET environment variable	482a7008117ee3215663aeb33cad981e5242a88a	e5242a88a
rohTEST@planetscale.com	Add ability to select from vreplication_log in VReplicationExec	427cac89cd6b143d3a1928ee682b3a9538709da5	538709da5
rohTEST@planetscale.com	Use withDDL for vreplication log queries	4a1ab946e3628ba8ef610ea4a158186a5fdd17ba	a5fdd17ba
rohTEST@planetscale.com	Add license file. Minor refactor	fa9de690ce0d27a781befbc1866aca5cd447798f	cd447798f
rohTEST@planetscale.com	Added comments and refactored tests	b6d39acb08939ba56e9e9587f34f3b8bcdcdc504	bcdcdc504
rohTEST@planetscale.com	Add logs for start and end of the copy phase	1cf72866ddfbd554700d6c9e32b9835ebb3b444c	ebb3b444c
rohTEST@planetscale.com	Fix test	0992d39c6d473b548679d012cfa5a889ffa448ef	9ffa448ef
rohTEST@planetscale.com	Add test for vreplication log and fix string conversion bug	b616143b14b75e7c23042c2eef4f6b27a275b0f7	7a275b0f7
rohTEST@planetscale.com	Ignore queries related to _vt.vreplication_log in tests	e6926932c14da9a2213be246bc2de5f011668551	011668551
rohTEST@planetscale.com	Create log table. Util functions to insert logs. Insert logs in VReplicationExec and setMessage/State	37c09d3be83922a8ef936fbc028a5031f96b7dbf	1f96b7dbf
harshTEST@planetscale.com	Merge pull request #7951 from vmg/vmg/vr-client-perf    7794c62651066970e1176181cb7000d385d0b327	172fac7dec8b11937a4efb26ebf4bedf1771f189	f1771f189
alkin.tezuysTEST@gmail.com	java: Bump SNAPSHOT version to 11.0.0-SNAPSHOT after Vitess release v10	7794c62651066970e1176181cb7000d385d0b327	385d0b327
alkin.tezuysTEST@gmail.com	Merge pull request #7964 from planetscale/10_0_RC1_release_notes	31d84d6ce8e233a053794ad0ffe5168d34d04450 b020dc71f5c7dc663d814563f1b6c97340f4411f	340f4411f
vTEST@strn.cat	vstreamer: fix docs	e7bf329da0029414c3b18e18e5cb2226b9a731a2	6b9a731a2
amasTEST@slack-corp.com 	[workflow] extract migration targets from wrangler (#7934)	8bd5a7cb093369b50a0926bfa3a112b3b744e782	3b744e782
alkin.tezuysTEST@gmail.com	More spacing issues fixed	7509d47ba785e7a39b8726dc80f93955953ab98d	5953ab98d
alkin.tezuysTEST@gmail.com	Minor spacing fixes	d31362e76ac69fb2bc4083e22e7c87683099fecd	83099fecd
alkin.tezuysTEST@gmail.com	Update 10_0_0_release_notes.md	a7034bdf5d454a47738335ed2afc75f72bdbcf37	72bdbcf37
alkin.tezuysTEST@gmail.com	v10 GA Release Notes	ad37320b2637620ee36d44d163399ecc2c1eea6c	c2c1eea6c
andrTEST@planetscale.com	Merge pull request #7912 from planetscale/show-databases-like	7e13d4bccca0325ca07a488334e77c4f2f964f6b 95eceb17d10c62d56f2e94e5478afb5a1b63e1c2	a1b63e1c2
andrTEST@planetscale.com	Merge pull request #7629 from planetscale/gen4-table-aliases	2e1b1e9322a6bfcfe792cca341b0d52860d3c66e 7ad14e3f3d26cb1780cdbf9c22029740e5aebde4	0e5aebde4
andrTEST@planetscale.com	Merge remote-tracking branch 'upstream/master' into show-databases-like	6b3ee1c31a939fc6628515f00087baa3e1e8acf7 2e1b1e9322a6bfcfe792cca341b0d52860d3c66e	860d3c66e
2607934+shlomi-noaTEST@users.noreply.github.com	Merge pull request #7959 from Hellcatlk/master	6c826115937d28ef83f05a1f0d54db0fcb814db4 cdab3040aaaa11c51e291d6b1a7af6fadd83dedf	add83dedf
zouy.fnTEST@cn.fujitsu.com	Fix a gofmt warning	08038850a258d6de250cf9d864d6118616f5562c	616f5562c
vTEST@strn.cat	mysql: allow reusing row storage when reading from a stream	a2850bbf41100618cb1192067b16585ba7c6b0c7	ba7c6b0c7
vTEST@strn.cat	throttle: do not check for time constantly	e0b90daebe9e6b98d969934a24899b41d25e3a68	1d25e3a68
andrTEST@planetscale.com	fix compilation error	18036f5fb5f58523dbf50726beb741cedac2baf8	edac2baf8
andrTEST@planetscale.com	better code comment	c173c945cf0e75e8649e6fa621509b5fb4ebd6c9	fb4ebd6c9
vTEST@strn.cat	conn: do not let header escape to the heap	d31fb23d8cb9463810ed9fc132df4060a6812f6e	0a6812f6e
vTEST@strn.cat	vstreamer: do not allocate when filtering rows	dafc1cb729d7be7dff2c05bd05a926005eb9a044	05eb9a044
vTEST@strn.cat	vstreamer: do not allocate when converting rows	c5cd3067aeb9d952a2f45084c37634267e4f9062	67e4f9062
andrTEST@planetscale.com	Merge remote-tracking branch 'upstream/master' into gen4-table-aliases	8c01827ed8b748240f213d9476ee162306ab01eb b1f9000ddd166d49adda6581e7ca9e0aca10c252	aca10c252
aquarapTEST@gmail.com	Fix mysql80 docker build with dep.	a28591577b8d432b9c5d78abf59ad494a0a943b0	4a0a943b0
TEST@planetscale.com	Revert "docker/lite/install_dependencies.sh:  Upgrade MySQL 8 to 8.0.24"	7858ff46545cff749b3663c92ae90ef27a5dfbc2	27a5dfbc2
TEST@planetscale.com	docker/lite/install_dependencies.sh:  Upgrade MySQL 8 to 8.0.24	c91d46782933292941a846fef2590ff1a6fa193f	a6fa193f`

	prs, authorCommits, nonMergeCommits, err := parseGitLog(in)
	require.NoError(t, err)
	assert.Equal(t, prs, []string{"7629", "7831", "7912", "7934", "7943", "7951", "7959", "7964", "7968", "7970"})
	assert.Equal(t, authorCommits, []string{"385d0b327", "3b744e782", "4a0a943b0", "538709da5", "616f5562c", "6b9a731a2", "e5242a88a", "edac2baf8"})
	assert.Equal(t, 28, nonMergeCommits)
}

func TestLoadSummaryReadme(t *testing.T) {
	readmeFile, err := os.CreateTemp("", "*.md")
	require.NoError(t, err)

	readmeContent := `- New Gen4 feature
- Self hosted runners
- Bunch of features
`

	err = os.WriteFile(readmeFile.Name(), []byte(readmeContent), 0644)
	require.NoError(t, err)

	str, err := releaseSummary(readmeFile.Name())
	require.NoError(t, err)
	require.Equal(t, str, readmeContent)
}

func TestGenerateReleaseNotes(t *testing.T) {
	tcs := []struct {
		name                 string
		releaseNote          releaseNote
		expectedOut          string
		expectedOutChangeLog string
	}{
		{
			name:        "empty",
			releaseNote: releaseNote{},
			expectedOut: "# Release of Vitess \n",
		}, {
			name:        "with version number",
			releaseNote: releaseNote{Version: "v12.0.0"},
			expectedOut: "# Release of Vitess v12.0.0\n",
		}, {
			name:        "with announcement",
			releaseNote: releaseNote{Announcement: "This is the new release.\n\nNew features got added.", Version: "v12.0.0"},
			expectedOut: "# Release of Vitess v12.0.0\n" +
				"This is the new release.\n\nNew features got added.\n",
		}, {
			name:        "with announcement and known issues",
			releaseNote: releaseNote{Announcement: "This is the new release.\n\nNew features got added.", Version: "v12.0.0", KnownIssues: "* bug 1\n* bug 2\n"},
			expectedOut: "# Release of Vitess v12.0.0\n" +
				"This is the new release.\n\nNew features got added.\n" +
				"------------\n" +
				"## Known Issues\n" +
				"* bug 1\n" +
				"* bug 2\n\n",
		}, {
			name: "with announcement and change log",
			releaseNote: releaseNote{
				Announcement:      "This is the new release.\n\nNew features got added.",
				Version:           "v12.0.0",
				VersionUnderscore: "12_0_0",
				ChangeLog:         "* PR 1\n* PR 2\n",
				ChangeMetrics:     "optimization is the root of all evil",
				SubDirPath:        "changelog/12.0/12.0.0",
			},
			expectedOut: "# Release of Vitess v12.0.0\n" +
				"This is the new release.\n\nNew features got added.\n" +
				"------------\n" +
				"The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/12.0/12.0.0/changelog.md).\n" +
				"optimization is the root of all evil\n",
			expectedOutChangeLog: "# Changelog of Vitess v12.0.0\n" +
				"* PR 1\n" +
				"* PR 2\n\n",
		}, {
			name: "with only change log",
			releaseNote: releaseNote{
				Version:           "v12.0.0",
				VersionUnderscore: "12_0_0",
				ChangeLog:         "* PR 1\n* PR 2\n",
				ChangeMetrics:     "optimization is the root of all evil",
				SubDirPath:        "changelog/12.0/12.0.0",
			},
			expectedOut: "# Release of Vitess v12.0.0\n" +
				"The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/12.0/12.0.0/changelog.md).\n" +
				"optimization is the root of all evil\n",
			expectedOutChangeLog: "# Changelog of Vitess v12.0.0\n" +
				"* PR 1\n" +
				"* PR 2\n\n",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			outFileRn, err := os.CreateTemp("", "*.md")
			require.NoError(t, err)
			outFileChangelog, err := os.CreateTemp("", "*.md")
			require.NoError(t, err)
			err = tc.releaseNote.generate(outFileRn, outFileChangelog)
			require.NoError(t, err)
			all, err := os.ReadFile(outFileRn.Name())
			require.NoError(t, err)
			require.Equal(t, tc.expectedOut, string(all))
		})
	}
}

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
)

func Test_groupPRs(t *testing.T) {
	tests := []struct {
		name    string
		prInfos []pullRequestInformation
		want    map[string]map[string][]pullRequestInformation
	}{
		{
			name:    "Single PR info with no labels",
			prInfos: []pullRequestInformation{{Title: "pr 1", Number: 1}},
			want:    map[string]map[string][]pullRequestInformation{"Other": {"Other": []pullRequestInformation{{Title: "pr 1", Number: 1}}}},
		}, {
			name:    "Single PR info with type label",
			prInfos: []pullRequestInformation{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}}}},
			want:    map[string]map[string][]pullRequestInformation{"Bug fixes": {"Other": []pullRequestInformation{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}}}}}}},
		{
			name:    "Single PR info with type and component labels",
			prInfos: []pullRequestInformation{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}},
			want:    map[string]map[string][]pullRequestInformation{"Bug fixes": {"VTGate": []pullRequestInformation{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}}}},
		{
			name: "Multiple PR infos with type and component labels", prInfos: []pullRequestInformation{
				{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}},
				{Title: "pr 2", Number: 2, Labels: []label{{Name: prefixType + "Feature"}, {Name: prefixComponent + "VTTablet"}}}},
			want: map[string]map[string][]pullRequestInformation{"Bug fixes": {"VTGate": []pullRequestInformation{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}}, "Feature": {"VTTablet": []pullRequestInformation{{Title: "pr 2", Number: 2, Labels: []label{{Name: prefixType + "Feature"}, {Name: prefixComponent + "VTTablet"}}}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := groupPRs(tt.prInfos)
			utils.MustMatch(t, tt.want, got)
		})
	}
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

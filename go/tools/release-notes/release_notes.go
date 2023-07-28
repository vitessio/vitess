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
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/spf13/pflag"
)

type (
	label struct {
		Name string `json:"name"`
	}

	labels []label

	pullRequestAuthor struct {
		Login string
	}

	pullRequestInformation struct {
		Number int
		Title  string
		Labels labels
		Author pullRequestAuthor
	}

	prsByComponent = map[string][]pullRequestInformation

	prsByType = map[string]prsByComponent

	sortedPRComponent struct {
		Name    string
		PrInfos []pullRequestInformation
	}

	sortedPRType struct {
		Name       string
		Components []sortedPRComponent
	}

	knownIssue struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
	}

	releaseNote struct {
		Version, VersionUnderscore                        string
		Announcement                                      string
		KnownIssues                                       string
		AddDetails                                        string
		PathToChangeLogFileOnGH, ChangeLog, ChangeMetrics string
		SubDirPath                                        string
	}
)

var (
	releaseNotesPath = `changelog/`
)

const (
	releaseNotesPathGitHub = `https://github.com/vitessio/vitess/blob/main/`
	markdownTemplate       = `# Release of Vitess {{.Version}}

{{- if or .Announcement .AddDetails }}
{{ .Announcement }}
{{- end }}

{{- if and (or .Announcement .AddDetails) (or .KnownIssues .ChangeLog) }}
------------
{{- end }}

{{- if .KnownIssues }}
## Known Issues
{{ .KnownIssues }}
{{- end }}

{{- if .ChangeLog }}
The entire changelog for this release can be found [here]({{ .PathToChangeLogFileOnGH }}).
{{- end }}

{{- if .ChangeLog }}
{{ .ChangeMetrics }}
{{- end }}
`

	markdownTemplateChangelog = `# Changelog of Vitess {{.Version}}
{{ .ChangeLog }}
`

	markdownTemplatePR = `
{{- range $type := . }}
### {{ $type.Name }}
{{- range $component := $type.Components }} 
#### {{ $component.Name }}
{{- range $prInfo := $component.PrInfos }}
 * {{ $prInfo.Title }} [#{{ $prInfo.Number }}](https://github.com/vitessio/vitess/pull/{{ $prInfo.Number }})
{{- end }}
{{- end }}
{{- end }}
`

	markdownTemplateKnownIssues = `
{{- range $issue := . }}
 * {{ $issue.Title }} #{{ $issue.Number }} 
{{- end }}
`

	prefixType        = "Type: "
	prefixComponent   = "Component: "
	lengthOfSingleSHA = 40
)

func (rn *releaseNote) generate(rnFile, changelogFile *os.File) error {
	var err error
	// Generate the release notes
	rn.PathToChangeLogFileOnGH = releaseNotesPathGitHub + path.Join(rn.SubDirPath, "changelog.md")
	if rnFile == nil {
		rnFile, err = os.OpenFile(path.Join(rn.SubDirPath, "release_notes.md"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
	}

	t := template.Must(template.New("release_notes").Parse(markdownTemplate))
	err = t.ExecuteTemplate(rnFile, "release_notes", rn)
	if err != nil {
		return err
	}

	// Generate the changelog
	if changelogFile == nil {
		changelogFile, err = os.OpenFile(path.Join(rn.SubDirPath, "changelog.md"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
	}
	t = template.Must(template.New("release_notes_changelog").Parse(markdownTemplateChangelog))
	err = t.ExecuteTemplate(changelogFile, "release_notes_changelog", rn)
	if err != nil {
		return err
	}
	return nil
}

func loadKnownIssues(release string) ([]knownIssue, error) {
	idx := strings.Index(release, ".")
	if idx > -1 {
		release = release[:idx]
	}
	label := fmt.Sprintf("Known issue: %s", release)
	out, err := execCmd("gh", "issue", "list", "--repo", "vitessio/vitess", "--label", label, "--json", "title,number")
	if err != nil {
		return nil, err
	}
	var knownIssues []knownIssue
	err = json.Unmarshal(out, &knownIssues)
	if err != nil {
		return nil, err
	}
	return knownIssues, nil
}

func loadMergedPRsAndAuthors(name string) (pris []pullRequestInformation, authors []string, err error) {
	out, err := execCmd("gh", "pr", "list", "-s", "merged", "-S", fmt.Sprintf("milestone:%s", name), "--json", "number,title,labels,author", "--limit", "5000")
	if err != nil {
		return
	}

	err = json.Unmarshal(out, &pris)
	if err != nil {
		return nil, nil, err
	}

	// Get the full list of distinct PRs authors and sort them
	authorMap := map[string]bool{}
	for _, pri := range pris {
		login := pri.Author.Login
		if ok := authorMap[login]; !ok {
			authors = append(authors, login)
			authorMap[login] = true
		}
	}
	sort.Strings(authors)

	return
}

func execCmd(name string, arg ...string) ([]byte, error) {
	out, err := exec.Command(name, arg...).Output()
	if err != nil {
		execErr, ok := err.(*exec.ExitError)
		if ok {
			return nil, fmt.Errorf("%s:\nstderr: %s\nstdout: %s", err.Error(), execErr.Stderr, out)
		}
		if strings.Contains(err.Error(), " executable file not found in") {
			return nil, fmt.Errorf("the command `gh` seems to be missing. Please install it from https://github.com/cli/cli")
		}
		return nil, err
	}
	return out, nil
}

func groupPRs(pris []pullRequestInformation) prsByType {
	prPerType := prsByType{}

	for _, info := range pris {
		var typ, component string
		for _, lbl := range info.Labels {
			switch {
			case strings.HasPrefix(lbl.Name, prefixType):
				typ = strings.TrimPrefix(lbl.Name, prefixType)
			case strings.HasPrefix(lbl.Name, prefixComponent):
				component = strings.TrimPrefix(lbl.Name, prefixComponent)
			}
		}
		switch typ {
		case "":
			typ = "Other"
		case "Bug":
			typ = "Bug fixes"
		}

		if component == "" {
			component = "Other"
		}
		components, exists := prPerType[typ]
		if !exists {
			components = prsByComponent{}
			prPerType[typ] = components
		}

		prsPerComponentAndType := components[component]
		components[component] = append(prsPerComponentAndType, info)
	}
	return prPerType
}

func createSortedPrTypeSlice(prPerType prsByType) []sortedPRType {
	var data []sortedPRType
	for typeKey, typeElem := range prPerType {
		newPrType := sortedPRType{
			Name: typeKey,
		}
		for componentKey, prInfos := range typeElem {
			newComponent := sortedPRComponent{
				Name:    componentKey,
				PrInfos: prInfos,
			}
			sort.Slice(newComponent.PrInfos, func(i, j int) bool {
				return newComponent.PrInfos[i].Number < newComponent.PrInfos[j].Number
			})
			newPrType.Components = append(newPrType.Components, newComponent)
		}
		sort.Slice(newPrType.Components, func(i, j int) bool {
			return newPrType.Components[i].Name < newPrType.Components[j].Name
		})
		data = append(data, newPrType)
	}
	sort.Slice(data, func(i, j int) bool {
		return data[i].Name < data[j].Name
	})
	return data
}

func releaseSummary(summaryFile string) (string, error) {
	contentSummary, err := os.ReadFile(summaryFile)
	if err != nil {
		return "", err
	}
	return string(contentSummary), nil
}

func getStringForPullRequestInfos(prPerType prsByType) (string, error) {
	data := createSortedPrTypeSlice(prPerType)

	t := template.Must(template.New("markdownTemplatePR").Parse(markdownTemplatePR))
	buff := bytes.Buffer{}
	if err := t.ExecuteTemplate(&buff, "markdownTemplatePR", data); err != nil {
		return "", err
	}
	return buff.String(), nil
}

func getStringForKnownIssues(issues []knownIssue) (string, error) {
	if len(issues) == 0 {
		return "", nil
	}
	t := template.Must(template.New("markdownTemplateKnownIssues").Parse(markdownTemplateKnownIssues))
	buff := bytes.Buffer{}
	if err := t.ExecuteTemplate(&buff, "markdownTemplateKnownIssues", issues); err != nil {
		return "", err
	}
	return buff.String(), nil
}

func groupAndStringifyPullRequest(pris []pullRequestInformation) (string, error) {
	if len(pris) == 0 {
		return "", nil
	}
	prPerType := groupPRs(pris)
	prStr, err := getStringForPullRequestInfos(prPerType)
	if err != nil {
		return "", err
	}
	return prStr, nil
}

func main() {
	var (
		versionName, summaryFile string
	)
	pflag.StringVarP(&versionName, "version", "v", "", "name of the version (has to be the following format: v11.0.0)")
	pflag.StringVarP(&summaryFile, "summary", "s", "", "readme file on which there is a summary of the release")
	pflag.Parse()

	// The -version flag must be of a valid format.
	rx := regexp.MustCompile(`v([0-9]+)\.([0-9]+)\.([0-9]+)`)
	// There should be 4 sub-matches, input: "v14.0.0", output: ["v14.0.0", "14", "0", "0"].
	versionMatch := rx.FindStringSubmatch(versionName)
	if len(versionMatch) != 4 {
		log.Fatal("The --version flag must be set using a valid format. Format: 'vX.X.X'.")
	}

	// Define the path to the release notes folder
	majorVersion := versionMatch[1] + "." + versionMatch[2]
	patchVersion := versionMatch[1] + "." + versionMatch[2] + "." + versionMatch[3]
	releaseNotesPath = path.Join(releaseNotesPath, majorVersion, patchVersion)

	err := os.MkdirAll(releaseNotesPath, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	releaseNotes := releaseNote{
		Version:           versionName,
		VersionUnderscore: fmt.Sprintf("%s_%s_%s", versionMatch[1], versionMatch[2], versionMatch[3]), // v14.0.0 -> 14_0_0, this is used to format filenames.
		SubDirPath:        releaseNotesPath,
	}

	// summary of the release
	if summaryFile != "" {
		summary, err := releaseSummary(summaryFile)
		if err != nil {
			log.Fatal(err)
		}
		releaseNotes.Announcement = summary
	}

	// known issues
	knownIssues, err := loadKnownIssues(versionName)
	if err != nil {
		log.Fatal(err)
	}
	knownIssuesStr, err := getStringForKnownIssues(knownIssues)
	if err != nil {
		log.Fatal(err)
	}
	releaseNotes.KnownIssues = knownIssuesStr

	// changelog with pull requests
	prs, authors, err := loadMergedPRsAndAuthors(versionName)
	if err != nil {
		log.Fatal(err)
	}

	releaseNotes.ChangeLog, err = groupAndStringifyPullRequest(prs)
	if err != nil {
		log.Fatal(err)
	}

	// changelog metrics
	if len(prs) > 0 && len(authors) > 0 {
		releaseNotes.ChangeMetrics = fmt.Sprintf(`
The release includes %d merged Pull Requests.

Thanks to all our contributors: @%s
`, len(prs), strings.Join(authors, ", @"))
	}

	if err := releaseNotes.generate(nil, nil); err != nil {
		log.Fatal(err)
	}
}

/*
Copyright 2023 The Vitess Authors.

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
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"text/template"
)

const (
	rootDir = "./changelog/"

	rootFileTmpl = `## Releases

{{- range $r := .SubDirs }}
* [{{ $r.Name }}]({{ $r.Name }})
{{- end -}}
`

	majorVersionTmpl = `## v{{ .Name }}

{{- if .Team }}
The dedicated team for this release can be found [here](.Team).{{ end }}

{{- range $r := .SubDirs }}
* **[{{ $r.Name }}]({{ $r.Name }})**
{{ if $r.Changelog }}	* [Changelog]({{ $r.Name }}/{{ $r.Changelog }})
{{ end -}}
{{ if $r.ReleaseNotes }}	* [Release Notes]({{ $r.Name }}/{{ $r.ReleaseNotes }})
{{ end -}}
{{- end -}}
`
)

type dir struct {
	Name         string
	Path         string
	Changelog    string
	ReleaseNotes string
	Team         string
	SubDirs      []dir
}

func main() {
	rootDir, err := getDirs(dir{Path: rootDir})
	if err != nil {
		log.Fatal(err)
	}

	err = createRootReadMe(rootDir)
	if err != nil {
		log.Fatal(err)
	}

	err = createMajorReleasesReadMe(rootDir.SubDirs)
	if err != nil {
		log.Fatal(err)
	}
}

func createRootReadMe(rootDirs dir) error {
	// Create the rootDir README
	rootRM, err := os.OpenFile(path.Join(rootDir, "README.md"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	// Render all the rootDir directories to the rootDir README
	t := template.Must(template.New("root_readme").Parse(rootFileTmpl))
	err = t.ExecuteTemplate(rootRM, "root_readme", rootDirs)
	if err != nil {
		return err
	}
	return nil
}

func createMajorReleasesReadMe(subDirs []dir) error {
	for _, subDir := range subDirs {
		majorVersionReadMeFile, err := os.OpenFile(path.Join(subDir.Path, "README.md"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}

		t := template.Must(template.New("major_version_readme").Parse(majorVersionTmpl))
		err = t.ExecuteTemplate(majorVersionReadMeFile, "major_version_readme", subDir)
		if err != nil {
			return err
		}
	}
	return nil
}

func getDirs(curDir dir) (dir, error) {
	entries, err := os.ReadDir(curDir.Path)
	if err != nil {
		return dir{}, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subDir, err := getDirs(dir{
				Name: entry.Name(),
				Path: path.Join(curDir.Path, entry.Name()),
			})
			if err != nil {
				return dir{}, err
			}
			curDir.SubDirs = append(curDir.SubDirs, subDir)
			continue
		}

		switch {
		case strings.Contains(entry.Name(), "changelog.md"):
			curDir.Changelog = entry.Name()
		case strings.Contains(entry.Name(), "release_notes.md"):
			curDir.ReleaseNotes = entry.Name()
		case strings.Contains(entry.Name(), "TEAM.md"):
			curDir.Team = entry.Name()
		}
	}
	sort.Slice(curDir.SubDirs, func(i, j int) bool {
		if len(curDir.SubDirs[i].Name) < len(curDir.SubDirs[j].Name) {
			return false
		}
		return curDir.SubDirs[i].Name > curDir.SubDirs[j].Name
	})
	return curDir, nil
}

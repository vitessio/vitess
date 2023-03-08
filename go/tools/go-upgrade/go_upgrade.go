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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
)

const (
	goDevAPI = "https://go.dev/dl/?mode=json"
)

type latestGolangRelease struct {
	Version string `json:"version"`
	Stable  bool   `json:"stable"`
}

func main() {
	failIfNoUpdate := false
	allowMajorUpgrade := false
	isMainBranch := true

	currentVersion, err := currentGolangVersion()
	if err != nil {
		log.Fatal(err)
	}

	availableVersions, err := getLatestStableGolangReleases()
	if err != nil {
		log.Fatal(err)
	}

	upgradeTo := chooseNewVersion(currentVersion, availableVersions, allowMajorUpgrade)
	if upgradeTo == nil {
		if failIfNoUpdate {
			os.Exit(1)
		}
		return
	}

	// err = replaceGoVersionInCodebase(currentVersion, upgradeTo)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	currentBootstrapVersionF, err := currentBootstrapVersion()
	if err != nil {
		log.Fatal(err)
	}
	nextBootstrapVersionF := currentBootstrapVersionF
	if isMainBranch {
		nextBootstrapVersionF += 1
	} else {
		nextBootstrapVersionF += 0.1
	}
	err = updateBootstrapVersionInCodebase(currentBootstrapVersionF, nextBootstrapVersionF, upgradeTo)
	if err != nil {
		log.Fatal(err)
	}

}

// currentGolangVersion gets the running version of Golang in Vitess
// and returns it as a *version.Version.
//
// The file `./build.env` describes which version of Golang is expected by Vitess.
// We use this file to detect the current Golang version of our codebase.
// The file contains `goversion_min 1.20.1`, we will grep `goversion_min` to finally find
// the precise golang version we're using.
func currentGolangVersion() (*version.Version, error) {
	contentRaw, err := os.ReadFile("build.env")
	if err != nil {
		return nil, err
	}
	content := string(contentRaw)
	idxBegin := strings.Index(content, "goversion_min") + len("goversion_min") + 1
	idxFinish := strings.Index(content[idxBegin:], "||") + idxBegin
	versionStr := strings.TrimSpace(content[idxBegin:idxFinish])
	return version.NewVersion(versionStr)
}

func currentBootstrapVersion() (float64, error) {
	contentRaw, err := os.ReadFile("Makefile")
	if err != nil {
		return 0, err
	}
	content := string(contentRaw)
	idxBegin := strings.Index(content, "BOOTSTRAP_VERSION=") + len("BOOTSTRAP_VERSION=")
	idxFinish := strings.IndexByte(content[idxBegin:], '\n') + idxBegin
	versionStr := strings.TrimSpace(content[idxBegin:idxFinish])
	f, err := strconv.ParseFloat(versionStr, 64)
	if err != nil {
		return 0, err
	}
	return f, nil
}

// getLatestStableGolangReleases fetches the latest stable releases of Golang from
// the official website using the goDevAPI URL.
// Once fetched, the releases are returned as version.Collection.
func getLatestStableGolangReleases() (version.Collection, error) {
	resp, err := http.Get(goDevAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var latestGoReleases []latestGolangRelease
	err = json.Unmarshal(body, &latestGoReleases)
	if err != nil {
		return nil, err
	}

	var versions version.Collection
	for _, release := range latestGoReleases {
		if !release.Stable {
			continue
		}
		if !strings.HasPrefix(release.Version, "go") {
			return nil, fmt.Errorf("golang version malformatted: %s", release.Version)
		}
		newVersion, err := version.NewVersion(release.Version[2:])
		if err != nil {
			return nil, err
		}
		versions = append(versions, newVersion)
	}
	return versions, nil
}

// chooseNewVersion decides what will be the next version we're going to use in our codebase.
// Given the current Golang version, the available latest versions and whether we allow major upgrade or not,
// chooseNewVersion will return either the new version or nil if we cannot/don't need to upgrade.
func chooseNewVersion(curVersion *version.Version, latestVersions version.Collection, allowMajorUpgrade bool) *version.Version {
	selectedVersion := curVersion
	for _, latestVersion := range latestVersions {
		if !allowMajorUpgrade && !isSameMajorVersion(latestVersion, selectedVersion) {
			continue
		}
		if latestVersion.GreaterThan(selectedVersion) {
			selectedVersion = latestVersion
		}
	}
	// No change detected, return nil meaning that we do not want to have a new Golang version.
	if selectedVersion.Equal(curVersion) {
		return nil
	}
	return selectedVersion
}

// replaceGoVersionInCodebase goes through all the files in the codebase where the
// Golang version must be updated
func replaceGoVersionInCodebase(old, new *version.Version) error {
	filesToChange, err := getListOfFilesInPaths([]string{
		"./.github/workflows",
		"./test/templates",
		"./build.env",
		"./docker/bootstrap/Dockerfile.common",
	})
	if err != nil {
		return err
	}

	for _, fileToChange := range filesToChange {
		err = replaceInFile(
			[]string{old.String()},
			[]string{new.String()},
			fileToChange,
		)
		if err != nil {
			return err
		}
	}

	if !isSameMajorVersion(old, new) {
		err = replaceInFile(
			[]string{fmt.Sprintf("%d.%d", old.Segments()[0], old.Segments()[1])},
			[]string{fmt.Sprintf("%d.%d", new.Segments()[0], new.Segments()[1])},
			"./go.mod",
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateBootstrapVersionInCodebase(old, new float64, newGoVersion *version.Version) error {
	files, err := getListOfFilesInPaths([]string{
		"./docker/base",
		"./docker/lite",
		"./docker/local",
		"./docker/vttestserver",
		"./Makefile",
		"./test/templates",
	})
	if err != nil {
		return err
	}

	const btv = "bootstrap_version="
	oldReplace := fmt.Sprintf("%s%-1g", btv, old)
	newReplace := fmt.Sprintf("%s%-1g", btv, new)
	for _, file := range files {
		err = replaceInFile(
			[]string{oldReplace, strings.ToUpper(oldReplace)},
			[]string{newReplace, strings.ToUpper(newReplace)},
			file,
		)
		if err != nil {
			return err
		}
	}

	oldReplace = fmt.Sprintf("\"bootstrap-version\", \"%-1g\"", old)
	newReplace = fmt.Sprintf("\"bootstrap-version\", \"%-1g\"", new)
	err = replaceInFile(
		[]string{oldReplace},
		[]string{newReplace},
		"./test.go",
	)
	if err != nil {
		return err
	}

	err = updateBootstrapChangelog(new, newGoVersion)
	if err != nil {
		return err
	}

	return nil
}

func updateBootstrapChangelog(new float64, goVersion *version.Version) error {
	file, err := os.OpenFile("./docker/bootstrap/CHANGELOG.md", os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	s, err := file.Stat()
	if err != nil {
		return err
	}
	newContent := fmt.Sprintf(`

## [%-1g] - %s
### Changes
- Update build to golang %s`, new, time.Now().Format(time.DateOnly), goVersion.String())

	_, err = file.WriteAt([]byte(newContent), s.Size())
	if err != nil {
		return err
	}
	return nil
}

func isSameMajorVersion(a, b *version.Version) bool {
	return a.Segments()[1] == b.Segments()[1]
}

func getListOfFilesInPaths(pathsToExplore []string) ([]string, error) {
	var filesToChange []string
	for _, pathToExplore := range pathsToExplore {
		stat, err := os.Stat(pathToExplore)
		if err != nil {
			return nil, err
		}
		if stat.IsDir() {
			dirEntries, err := os.ReadDir(pathToExplore)
			if err != nil {
				return nil, err
			}
			for _, entry := range dirEntries {
				if entry.IsDir() {
					continue
				}
				filesToChange = append(filesToChange, path.Join(pathToExplore, entry.Name()))
			}
		} else {
			filesToChange = append(filesToChange, pathToExplore)
		}
	}
	return filesToChange, nil
}

// replaceInFile replaces old with new in the given file.
func replaceInFile(old, new []string, fileToChange string) error {
	if len(old) != len(new) {
		panic("old and new should be of the same length")
	}

	f, err := os.OpenFile(fileToChange, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	contentStr := string(content)

	for i := range old {
		contentStr = strings.ReplaceAll(contentStr, old[i], new[i])
	}

	_, err = f.WriteAt([]byte(contentStr), 0)
	if err != nil {
		return err
	}
	return nil
}

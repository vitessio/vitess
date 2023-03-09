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
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"encoding/json"
	"flag"
	"log"

	"github.com/hashicorp/go-version"
	"golang.org/x/exp/slices"
)

// The tool go_upgrade allows us to automate some tasks required
// to bump the version of Golang used throughout our codebase.
//
// It comes with four commands:
// 		- Default: `./go/tools/go-upgrade/go_upgrade.go`
//
// 		  This will simply bump the version across the codebase.
// 		  The latest available version of Golang will be fetched
// 		  and used instead of the old version.
//
// 		  By default, we do allow major Golang version upgrade such
// 		  as 1.20 to 1.21 but this can be overridden using the
// 		  allow-major-upgrade CLI flag. Usually, we only allow such
// 		  upgrade on the main branch of the repository.
//
// 		  In CI, particularly, we do not want to modify the workflow
// 		  files before automatically creating a Pull Request to
// 		  avoid permission issues. The rewrite of workflow files can
// 		  be disabled using the no-workflow-update CLI flag.
//
// 		  Moreover, this command automatically bumps the bootstrap
// 		  version of our codebase. If we are on the main branch, we
// 		  want to use the CLI flag `main` to remember to increment
// 		  the bootstrap version by 1 instead of 0.1.
//
//
// 		- Get Current Go Version: `./go/tools/go-upgrade/go_upgrade.go get_go_version`
//
// 		  This command outputs the currently used Golang version of
// 		  our codebase.
//
//
// 		- Get Current bootstrap Version: `./go/tools/go-upgrade/go_upgrade.go get_bootstrap_version`
//
// 		  This command outputs the currently used bootstrap version
// 		  of our codebase.
//
//
// 		- Update Workflows: `./go/tools/go-upgrade/go_upgrade.go update_workflows`
//
// 		  This command is used after the auto Pull Request has been
// 		  created. One of the reviewer must go and use this command
// 		  to update the workflows files with the newer Golang version.
// 		  It takes two arguments `go-from` and `go-to` to identify the
// 		  old Golang version (go-from) and the new one (go-to).

const (
	goDevAPI = "https://go.dev/dl/?mode=json"
)

type latestGolangRelease struct {
	Version string `json:"version"`
	Stable  bool   `json:"stable"`
}

func main() {
	noWorkflowUpdate := flag.Bool("no-workflow-update", false, "Whether or not the workflow files should be updated. Useful when using this script to auto-create PRs.")
	allowMajorUpgrade := flag.Bool("allow-major-upgrade", false, "Defines if Golang major version upgrade are allowed.")
	isMainBranch := flag.Bool("main", false, "Defines if the current branch is the main branch.")
	goFrom := flag.String("go-from", "", "The original Golang version we start with.")
	goTo := flag.String("go-to", "", "The Golang version we want to upgrade to.")
	flag.Parse()

	switch {
	case slices.Contains(os.Args, "get_go_version"):
		currentVersion, err := currentGolangVersion()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(currentVersion.String())
	case slices.Contains(os.Args, "get_bootstrap_version"):
		currentBootstrapVersionF, err := currentBootstrapVersion()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(currentBootstrapVersionF)
	case slices.Contains(os.Args, "update_workflows"):
		if *noWorkflowUpdate {
			break
		}
		err := updateWorkflowFilesOnly(*goFrom, *goTo)
		if err != nil {
			log.Fatal(err)
		}
	default:
		err := upgradePath(*allowMajorUpgrade, *noWorkflowUpdate, *isMainBranch)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func updateWorkflowFilesOnly(goFrom, goTo string) error {
	oldV, err := version.NewVersion(goFrom)
	if err != nil {
		return err
	}
	newV, err := version.NewVersion(goTo)
	if err != nil {
		return err
	}
	filesToChange, err := getListOfFilesInPaths([]string{"./.github/workflows"})
	if err != nil {
		return err
	}

	for _, fileToChange := range filesToChange {
		err = replaceInFile(
			[]string{"go-version: " + oldV.String()},
			[]string{"go-version: " + newV.String()},
			fileToChange,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func upgradePath(allowMajorUpgrade, noWorkflowUpdate, isMainBranch bool) error {
	currentVersion, err := currentGolangVersion()
	if err != nil {
		return err
	}

	availableVersions, err := getLatestStableGolangReleases()
	if err != nil {
		return err
	}

	upgradeTo := chooseNewVersion(currentVersion, availableVersions, allowMajorUpgrade)
	if upgradeTo == nil {
		return nil
	}

	err = replaceGoVersionInCodebase(currentVersion, upgradeTo, noWorkflowUpdate)
	if err != nil {
		return err
	}

	currentBootstrapVersionF, err := currentBootstrapVersion()
	if err != nil {
		return err
	}
	nextBootstrapVersionF := currentBootstrapVersionF
	if isMainBranch {
		nextBootstrapVersionF += 1
	} else {
		nextBootstrapVersionF += 0.1
	}
	err = updateBootstrapVersionInCodebase(currentBootstrapVersionF, nextBootstrapVersionF, upgradeTo)
	if err != nil {
		return err
	}
	return nil
}

// currentGolangVersion gets the running version of Golang in Vitess
// and returns it as a *version.Version.
//
// The file `./build.env` describes which version of Golang is expected by Vitess.
// We use this file to detect the current Golang version of our codebase.
// The file contains `goversion_min x.xx.xx`, we will grep `goversion_min` to finally find
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
func replaceGoVersionInCodebase(old, new *version.Version, noWorkflowUpdate bool) error {
	explore := []string{
		"./test/templates",
		"./build.env",
		"./docker/bootstrap/Dockerfile.common",
	}
	if !noWorkflowUpdate {
		explore = append(explore, "./.github/workflows")
	}
	filesToChange, err := getListOfFilesInPaths(explore)
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

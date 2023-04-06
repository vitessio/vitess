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
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	"github.com/hashicorp/go-version"
	"github.com/spf13/cobra"
)

const (
	goDevAPI = "https://go.dev/dl/?mode=json"
)

type (
	latestGolangRelease struct {
		Version string `json:"version"`
		Stable  bool   `json:"stable"`
	}

	bootstrapVersion struct {
		major, minor int // when minor == -1, it means there are no minor version
	}
)

var (
	workflowUpdate    = true
	allowMajorUpgrade = false
	isMainBranch      = false
	goTo              = ""

	rootCmd = &cobra.Command{
		Use:   "go-upgrade",
		Short: "Automates the Golang upgrade.",
		Long: `go-upgrade allows us to automate some tasks required to bump the version of Golang used throughout our codebase.

It mostly used by the update_golang_version.yml CI workflow that runs on a CRON.

This tool is meant to be run at the root of the repository.
`,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		Args: cobra.NoArgs,
	}

	getCmd = &cobra.Command{
		Use:   "get",
		Short: "Command to get useful information about the codebase.",
		Long:  "Command to get useful information about the codebase.",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		Args: cobra.NoArgs,
	}

	getGoCmd = &cobra.Command{
		Use:   "go-version",
		Short: "go-version prints the Golang version used by the current codebase.",
		Long:  "go-version prints the Golang version used by the current codebase.",
		Run:   runGetGoCmd,
		Args:  cobra.NoArgs,
	}

	getBootstrapCmd = &cobra.Command{
		Use:   "bootstrap-version",
		Short: "bootstrap-version prints the Docker Bootstrap version used by the current codebase.",
		Long:  "bootstrap-version prints the Docker Bootstrap version used by the current codebase.",
		Run:   runGetBootstrapCmd,
		Args:  cobra.NoArgs,
	}

	upgradeCmd = &cobra.Command{
		Use:   "upgrade",
		Short: "upgrade will upgrade the Golang and Bootstrap versions of the codebase to the latest available version.",
		Long: `This command bumps the Golang and Bootstrap versions of the codebase.

The latest available version of Golang will be fetched and used instead of the old version.

By default, we do not allow major Golang version upgrade such as 1.20 to 1.21 but this can be overridden using the
--allow-major-upgrade CLI flag. Usually, we only allow such upgrade on the main branch of the repository.

In CI, particularly, we do not want to modify the workflow files before automatically creating a Pull Request to
avoid permission issues. The rewrite of workflow files can be disabled using the --workflow-update=false CLI flag.

Moreover, this command automatically bumps the bootstrap version of our codebase. If we are on the main branch, we
want to use the CLI flag --main to remember to increment the bootstrap version by 1 instead of 0.1.`,
		Run:  runUpgradeCmd,
		Args: cobra.NoArgs,
	}

	upgradeWorkflowsCmd = &cobra.Command{
		Use:   "workflows",
		Short: "workflows will upgrade the Golang version used in our CI workflows files.",
		Long:  "This step is omitted by the bot since. We let the maintainers of Vitess manually upgrade the version used by the workflows using this command.",
		Run:   runUpgradeWorkflowsCmd,
		Args:  cobra.NoArgs,
	}
)

func init() {
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(upgradeCmd)

	getCmd.AddCommand(getGoCmd)
	getCmd.AddCommand(getBootstrapCmd)

	upgradeCmd.AddCommand(upgradeWorkflowsCmd)

	upgradeCmd.Flags().BoolVar(&workflowUpdate, "workflow-update", workflowUpdate, "Whether or not the workflow files should be updated. Useful when using this script to auto-create PRs.")
	upgradeCmd.Flags().BoolVar(&allowMajorUpgrade, "allow-major-upgrade", allowMajorUpgrade, "Defines if Golang major version upgrade are allowed.")
	upgradeCmd.Flags().BoolVar(&isMainBranch, "main", isMainBranch, "Defines if the current branch is the main branch.")

	upgradeWorkflowsCmd.Flags().StringVar(&goTo, "go-to", goTo, "The Golang version we want to upgrade to.")
}

func main() {
	cobra.CheckErr(rootCmd.Execute())
}

func runGetGoCmd(_ *cobra.Command, _ []string) {
	currentVersion, err := currentGolangVersion()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(currentVersion.String())
}

func runGetBootstrapCmd(_ *cobra.Command, _ []string) {
	currentVersion, err := currentBootstrapVersion()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(currentVersion.toString())
}

func runUpgradeWorkflowsCmd(_ *cobra.Command, _ []string) {
	err := updateWorkflowFilesOnly(goTo)
	if err != nil {
		log.Fatal(err)
	}
}

func runUpgradeCmd(_ *cobra.Command, _ []string) {
	err := upgradePath(allowMajorUpgrade, workflowUpdate, isMainBranch)
	if err != nil {
		log.Fatal(err)
	}
}

func updateWorkflowFilesOnly(goTo string) error {
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
			[]*regexp.Regexp{regexp.MustCompile(`go-version:[[:space:]]*([0-9.]+).*`)},
			[]string{"go-version: " + newV.String()},
			fileToChange,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func upgradePath(allowMajorUpgrade, workflowUpdate, isMainBranch bool) error {
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

	err = replaceGoVersionInCodebase(currentVersion, upgradeTo, workflowUpdate)
	if err != nil {
		return err
	}

	currentBootstrapVersionF, err := currentBootstrapVersion()
	if err != nil {
		return err
	}
	nextBootstrapVersionF := currentBootstrapVersionF
	if isMainBranch {
		nextBootstrapVersionF.major += 1
	} else {
		nextBootstrapVersionF.minor += 1
	}
	err = updateBootstrapVersionInCodebase(currentBootstrapVersionF.toString(), nextBootstrapVersionF.toString(), upgradeTo)
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

	versre := regexp.MustCompile("(?i).*goversion_min[[:space:]]*([0-9.]+).*")
	versionStr := versre.FindStringSubmatch(content)
	if len(versionStr) != 2 {
		return nil, fmt.Errorf("malformatted error, got: %v", versionStr)
	}
	return version.NewVersion(versionStr[1])
}

func currentBootstrapVersion() (bootstrapVersion, error) {
	contentRaw, err := os.ReadFile("Makefile")
	if err != nil {
		return bootstrapVersion{}, err
	}
	content := string(contentRaw)

	versre := regexp.MustCompile("(?i).*BOOTSTRAP_VERSION[[:space:]]*=[[:space:]]*([0-9.]+).*")
	versionStr := versre.FindStringSubmatch(content)
	if len(versionStr) != 2 {
		return bootstrapVersion{}, fmt.Errorf("malformatted error, got: %v", versionStr)
	}

	vs := strings.Split(versionStr[1], ".")
	major, err := strconv.Atoi(vs[0])
	if err != nil {
		return bootstrapVersion{}, err
	}

	minor := -1
	if len(vs) > 1 {
		minor, err = strconv.Atoi(vs[1])
		if err != nil {
			return bootstrapVersion{}, err
		}
	}

	return bootstrapVersion{
		major: major,
		minor: minor,
	}, nil
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
		if !allowMajorUpgrade && !isSameMajorMinorVersion(latestVersion, selectedVersion) {
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
func replaceGoVersionInCodebase(old, new *version.Version, workflowUpdate bool) error {
	if old.Equal(new) {
		return nil
	}
	explore := []string{
		"./test/templates",
		"./build.env",
		"./docker/bootstrap/Dockerfile.common",
	}
	if workflowUpdate {
		explore = append(explore, "./.github/workflows")
	}
	filesToChange, err := getListOfFilesInPaths(explore)
	if err != nil {
		return err
	}

	for _, fileToChange := range filesToChange {
		err = replaceInFile(
			[]*regexp.Regexp{regexp.MustCompile(fmt.Sprintf(`(%s)`, old.String()))},
			[]string{new.String()},
			fileToChange,
		)
		if err != nil {
			return err
		}
	}

	if !isSameMajorMinorVersion(old, new) {
		err = replaceInFile(
			[]*regexp.Regexp{regexp.MustCompile(`go[[:space:]]*([0-9.]+)`)},
			[]string{fmt.Sprintf("go %d.%d", new.Segments()[0], new.Segments()[1])},
			"./go.mod",
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateBootstrapVersionInCodebase(old, new string, newGoVersion *version.Version) error {
	if old == new {
		return nil
	}
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

	for _, file := range files {
		err = replaceInFile(
			[]*regexp.Regexp{
				regexp.MustCompile(`ARG[[:space:]]*bootstrap_version[[:space:]]*=[[:space:]]*[0-9.]+`), // Dockerfile
				regexp.MustCompile(`BOOTSTRAP_VERSION[[:space:]]*=[[:space:]]*[0-9.]+`),                // Makefile
			},
			[]string{
				fmt.Sprintf("ARG bootstrap_version=%s", new), // Dockerfile
				fmt.Sprintf("BOOTSTRAP_VERSION=%s", new),     // Makefile
			},
			file,
		)
		if err != nil {
			return err
		}
	}

	err = replaceInFile(
		[]*regexp.Regexp{regexp.MustCompile(`\"bootstrap-version\",[[:space:]]*\"([0-9.]+)\"`)},
		[]string{fmt.Sprintf("\"bootstrap-version\", \"%s\"", new)},
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

func updateBootstrapChangelog(new string, goVersion *version.Version) error {
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

## [%s] - %s
### Changes
- Update build to golang %s`, new, time.Now().Format("2006-01-02"), goVersion.String())

	_, err = file.WriteAt([]byte(newContent), s.Size())
	if err != nil {
		return err
	}
	return nil
}

func isSameMajorMinorVersion(a, b *version.Version) bool {
	return a.Segments()[0] == b.Segments()[0] && a.Segments()[1] == b.Segments()[1]
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
func replaceInFile(oldexps []*regexp.Regexp, new []string, fileToChange string) error {
	if len(oldexps) != len(new) {
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

	for i, oldex := range oldexps {
		contentStr = oldex.ReplaceAllString(contentStr, new[i])
	}

	_, err = f.WriteAt([]byte(contentStr), 0)
	if err != nil {
		return err
	}
	return nil
}

func (b bootstrapVersion) toString() string {
	if b.minor == -1 {
		return fmt.Sprintf("%d", b.major)
	}
	return fmt.Sprintf("%d.%d", b.major, b.minor)
}

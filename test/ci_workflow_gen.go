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
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"golang.org/x/sync/errgroup"
)

type mysqlVersion string

const (
	mysql57 mysqlVersion = "mysql57"
	mysql80 mysqlVersion = "mysql80"
	mysql84 mysqlVersion = "mysql84"
)

var unitTestDatabases = []mysqlVersion{mysql57, mysql80, mysql84}

var gitTimeout = time.Second * 10

const (
	oracleCloudRunner = "oracle-vm-16cpu-64gb-x86-64"
	cores16RunnerName = oracleCloudRunner
	defaultRunnerName = "ubuntu-24.04"
	goimportsTag      = "v0.39.0"
)

// To support a private git repository, set goPrivate to a repo in
// github.com/org/repo format. This assumes a GitHub PAT token is
// set as a repo secret named GH_ACCESS_TOKEN. The GitHub PAT must
// have read access to your vitess fork/repo.
const goPrivate = ""

const (
	workflowConfigDir = "../.github/workflows"

	unitTestTemplate = "templates/unit_test.tpl"

	clusterVitessTesterTemplate = "templates/cluster_vitess_tester.tpl"
)

var vitessTesterMap = map[string]string{
	"vtgate": "./go/test/endtoend/vtgate/vitess_tester",
}

type GitMeta struct {
	SHA     string
	Comment string
}

type GitMetas struct {
	Goimports     *GitMeta
	GoJunitReport *GitMeta
}

type unitTest struct {
	*GitMetas
	Name, RunsOn, Platform, FileName, GoPrivate, Evalengine string
	Race                                                    bool
}

type vitessTesterTest struct {
	*GitMetas
	FileName  string
	Name      string
	RunsOn    string
	GoPrivate string
	Path      string
}

// getGitRefSHA fetches the HEAD SHA of a git repo + branch using the "git" command.
func getGitRefSHA(ctx context.Context, url, branchOrTag string) (string, error) {
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", "ls-remote", url, branchOrTag)
	cmd.Stderr = os.Stderr
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	// 'git ls-remote <url> <branchOrTag>' returns two text columns: a commit
	// SHA and a reference. We expect the stdout to be a single line, which
	// should always be true but we still validate.
	// Example:
	//    $ git ls-remote https://github.com/vitessio/go-junit-report HEAD
	//    99fa7f0daf16db969f54a49139a14471e633e6e8	HEAD
	for line := range strings.Lines(stdout.String()) {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if fields[1] != branchOrTag && !strings.HasSuffix(fields[1], "/"+branchOrTag) {
			continue
		}

		// git SHA1 hashes are 40 hex characters.
		sha := fields[0]
		match, err := regexp.MatchString(`^[a-zA-Z0-9]{40}$`, sha)
		if !match || err != nil {
			continue
		}
		return sha, nil
	}
	return "", fmt.Errorf("cannot parse output of 'git ls-remote' for %q", url)
}

// getGitMetas concurrently fetches Git metadata for workflow dependencies.
func getGitMetas(ctx context.Context) (*GitMetas, error) {
	var metasMu sync.Mutex
	var metas GitMetas

	eg, egCtx := errgroup.WithContext(ctx)

	// vitessio/go-junit-report
	eg.Go(func() error {
		sha, err := getGitRefSHA(egCtx, "https://github.com/vitessio/go-junit-report", "HEAD")
		if err != nil {
			return err
		}
		metasMu.Lock()
		defer metasMu.Unlock()
		metas.GoJunitReport = &GitMeta{SHA: sha, Comment: "HEAD"}
		return nil
	})

	// goimports tool
	eg.Go(func() error {
		sha, err := getGitRefSHA(egCtx, "https://go.googlesource.com/tools", goimportsTag)
		if err != nil {
			return err
		}
		metasMu.Lock()
		defer metasMu.Unlock()
		metas.Goimports = &GitMeta{SHA: sha, Comment: goimportsTag}
		return nil
	})

	return &metas, eg.Wait()
}

func mergeBlankLines(buf *bytes.Buffer) string {
	var out []string
	in := strings.Split(buf.String(), "\n")
	lastWasBlank := false
	for _, line := range in {
		if strings.TrimSpace(line) == "" {
			if lastWasBlank {
				continue
			}
			lastWasBlank = true
		} else {
			lastWasBlank = false
		}

		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()

	gitMetas, err := getGitMetas(ctx)
	if err != nil {
		log.Fatalf("failed to get all Git metadata: %v", err)
	}

	generateUnitTestWorkflows(gitMetas)
	generateVitessTesterWorkflows(vitessTesterMap, clusterVitessTesterTemplate, gitMetas)
}

func generateVitessTesterWorkflows(mp map[string]string, tpl string, gitMetas *GitMetas) {
	for test, testPath := range mp {
		tt := &vitessTesterTest{
			Name:      fmt.Sprintf("Vitess Tester (%v)", test),
			RunsOn:    defaultRunnerName,
			GoPrivate: goPrivate,
			Path:      testPath,
			GitMetas:  gitMetas,
		}

		templateFileName := tpl
		tt.FileName = fmt.Sprintf("vitess_tester_%s.yml", test)
		workflowPath := fmt.Sprintf("%s/%s", workflowConfigDir, tt.FileName)
		err := writeFileFromTemplate(templateFileName, workflowPath, tt)
		if err != nil {
			log.Print(err)
		}
	}
}

func generateUnitTestWorkflows(gitMetas *GitMetas) {
	for _, platform := range unitTestDatabases {
		for _, evalengine := range []string{"1", "0"} {
			test := &unitTest{
				Name:       fmt.Sprintf("Unit Test (%s%s)", evalengineToString(evalengine), platform),
				RunsOn:     defaultRunnerName,
				Platform:   string(platform),
				GoPrivate:  goPrivate,
				Evalengine: evalengine,
				GitMetas:   gitMetas,
			}
			test.FileName = fmt.Sprintf("unit_test_%s%s.yml", evalengineToString(evalengine), platform)
			path := fmt.Sprintf("%s/%s", workflowConfigDir, test.FileName)
			err := writeFileFromTemplate(unitTestTemplate, path, test)
			if err != nil {
				log.Print(err)
			}
		}
	}

	// Generate unit tests with race detection
	for _, evalengine := range []string{"1", "0"} {
		raceTest := &unitTest{
			Name:       fmt.Sprintf("Unit Test (%sRace)", evalengineToRaceNamePrefix(evalengine)),
			RunsOn:     cores16RunnerName,
			Platform:   string(mysql80),
			GoPrivate:  goPrivate,
			Evalengine: evalengine,
			Race:       true,
			GitMetas:   gitMetas,
		}
		raceTest.FileName = fmt.Sprintf("unit_race%s.yml", evalengineToFileSuffix(evalengine))
		path := fmt.Sprintf("%s/%s", workflowConfigDir, raceTest.FileName)
		err := writeFileFromTemplate(unitTestTemplate, path, raceTest)
		if err != nil {
			log.Print(err)
		}
	}
}

func evalengineToString(evalengine string) string {
	if evalengine == "1" {
		return "evalengine_"
	}
	return ""
}

func evalengineToRaceNamePrefix(evalengine string) string {
	if evalengine == "1" {
		return "Evalengine_"
	}
	return ""
}

func evalengineToFileSuffix(evalengine string) string {
	if evalengine == "1" {
		return "_evalengine"
	}
	return ""
}

func writeFileFromTemplate(templateFile, filePath string, test any) error {
	tpl := template.New(path.Base(templateFile))
	tpl.Funcs(template.FuncMap{
		"contains": strings.Contains,
	})
	tpl, err := tpl.ParseFiles(templateFile)
	if err != nil {
		return fmt.Errorf("Error: %s\n", err)
	}

	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, test)
	if err != nil {
		return fmt.Errorf("Error: %s\n", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Error creating file: %s\n", err)
	}
	if _, err := f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n"); err != nil {
		return err
	}
	if _, err := f.WriteString(mergeBlankLines(buf)); err != nil {
		return err
	}
	fmt.Printf("Generated %s\n", filePath)
	return nil
}

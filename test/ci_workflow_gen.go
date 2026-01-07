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

	defaultMySQLVersion = mysql84
)

type mysqlVersions []mysqlVersion

var (
	defaultMySQLVersions = []mysqlVersion{defaultMySQLVersion}
)

var (
	unitTestDatabases = []mysqlVersion{mysql57, mysql80, mysql84}
)

var gitTimeout = time.Second * 10

const (
	oracleCloudRunner = "oracle-vm-16cpu-64gb-x86-64"
	githubRunner      = "gh-hosted-runners-16cores-1-24.04"
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

	// An empty string will cause the default non platform specific template
	// to be used.
	clusterTestTemplate = "templates/cluster_endtoend_test%s.tpl"

	clusterVitessTesterTemplate = "templates/cluster_vitess_tester.tpl"

	clusterTestDockerTemplate = "templates/cluster_endtoend_test_docker.tpl"
)

var (
	// Clusters 10, 25 are executed on docker, using the docker_test_cluster 10, 25 workflows.
	// Hence, they are not listed in the list below.
	clusterList = []string{
		"vtctlbackup_sharded_clustertest_heavy",
		"12",
		"13",
		"ers_prs_newfeatures_heavy",
		"15",
		"vtgate_general_heavy",
		"vtbackup",
		"18",
		"xb_backup",
		"backup_pitr",
		"backup_pitr_xtrabackup",
		"backup_pitr_mysqlshell",
		"21",
		"mysql_server_vault",
		"vstream",
		"onlineddl_vrepl",
		"onlineddl_vrepl_stress",
		"onlineddl_vrepl_stress_suite",
		"onlineddl_vrepl_suite",
		"onlineddl_revert",
		"onlineddl_scheduler",
		"tabletmanager_throttler_topo",
		"tabletmanager_tablegc",
		"tabletmanager_consul",
		"vtgate_concurrentdml",
		"vtgate_godriver",
		"vtgate_gen4",
		"vtgate_readafterwrite",
		"vtgate_reservedconn",
		"vtgate_schema",
		"vtgate_tablet_healthcheck_cache",
		"vtgate_topo",
		"vtgate_topo_consul",
		"vtgate_topo_etcd",
		"vtgate_transaction",
		"vtgate_unsharded",
		"vtgate_vindex_heavy",
		"vtgate_vschema",
		"vtgate_queries",
		"vtgate_plantests",
		"vtgate_schema_tracker",
		"vtgate_foreignkey_stress",
		"vtorc",
		"xb_recovery",
		"mysql84",
		"vreplication_across_db_versions",
		"vreplication_mariadb_to_mysql",
		"vreplication_basic",
		"vreplication_cellalias",
		"vreplication_copy_parallel",
		"vreplication_v2",
		"vreplication_partial_movetables_and_materialize",
		"vreplication_foreign_key_stress",
		"vreplication_migrate",
		"vreplication_vtctldclient_movetables_tz",
		"vreplication_vdiff2",
		"vreplication_multi_tenant",
		"schemadiff_vrepl",
		"topo_connection_cache",
		"vtgate_partial_keyspace",
		"vttablet_prscomplex",
	}

	buildTag = map[string]string{
		"vtgate_transaction": "debug2PC",
	}

	vitessTesterMap = map[string]string{
		"vtgate": "./go/test/endtoend/vtgate/vitess_tester",
	}

	clusterDockerList           = []string{}
	clustersRequiringXtraBackup = []string{
		"xb_backup",
		"xb_recovery",
		"backup_pitr_xtrabackup",
	}
	clustersRequiringMakeTools = []string{
		"18",
		"mysql_server_vault",
		"vtgate_topo_consul",
		"tabletmanager_consul",
	}
	clustersRequiringMemoryCheck = []string{
		"vtorc",
	}
	clusterRequiring16CoresMachines = []string{
		"onlineddl_vrepl",
		"onlineddl_vrepl_stress",
		"onlineddl_vrepl_stress_suite",
		"onlineddl_vrepl_suite",
		"vreplication_basic",
		"vreplication_migrate",
		"vreplication_vtctldclient_vdiff2_movetables_tz",
	}
	clusterRequiringMinio = []string{
		"21",
	}
)

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

type clusterTest struct {
	*GitMetas
	Name, Shard, Platform              string
	FileName                           string
	BuildTag                           string
	RunsOn                             string
	GoPrivate                          string
	MemoryCheck                        bool
	MakeTools, InstallXtraBackup       bool
	Docker                             bool
	LimitResourceUsage                 bool
	EnableBinlogTransactionCompression bool
	EnablePartialJSON                  bool
	PartialKeyspace                    bool
	NeedsMinio                         bool
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

// clusterMySQLVersions return list of mysql versions (one or more) that this cluster needs to test against
func clusterMySQLVersions() mysqlVersions {
	switch {
	// Add any specific clusters, or groups of clusters, here,
	// that require allMySQLVersions to be tested against.
	// At this time this list is clean because Vitess stopped
	// supporting MySQL 5.7. At some point, we will need to
	// support post 8.0 versions of MySQL, and this list will
	// inevitably grow.
	default:
		return defaultMySQLVersions
	}
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
	generateClusterWorkflows(clusterList, clusterTestTemplate, gitMetas)
	generateClusterWorkflows(clusterDockerList, clusterTestDockerTemplate, gitMetas)
}

func canonnizeList(list []string) []string {
	var output []string
	for _, item := range list {
		if item := strings.TrimSpace(item); item != "" {
			output = append(output, item)
		}
	}
	return output
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

func generateClusterWorkflows(list []string, tpl string, gitMetas *GitMetas) {
	clusters := canonnizeList(list)
	for _, cluster := range clusters {
		for _, mysqlVersion := range clusterMySQLVersions() {
			test := &clusterTest{
				Name:      fmt.Sprintf("Cluster (%s)", cluster),
				Shard:     cluster,
				BuildTag:  buildTag[cluster],
				RunsOn:    defaultRunnerName,
				GoPrivate: goPrivate,
				GitMetas:  gitMetas,
			}
			cores16Clusters := canonnizeList(clusterRequiring16CoresMachines)
			for _, cores16Cluster := range cores16Clusters {
				if cores16Cluster == cluster {
					test.RunsOn = cores16RunnerName
					break
				}
			}
			makeToolClusters := canonnizeList(clustersRequiringMakeTools)
			for _, makeToolCluster := range makeToolClusters {
				if makeToolCluster == cluster {
					test.MakeTools = true
					break
				}
			}
			memoryCheckClusters := canonnizeList(clustersRequiringMemoryCheck)
			for _, memCheckCluster := range memoryCheckClusters {
				if memCheckCluster == cluster {
					test.MemoryCheck = true
					break
				}
			}
			xtraBackupClusters := canonnizeList(clustersRequiringXtraBackup)
			for _, xtraBackupCluster := range xtraBackupClusters {
				if xtraBackupCluster == cluster {
					test.InstallXtraBackup = true
					break
				}
			}
			minioClusters := canonnizeList(clusterRequiringMinio)
			for _, minioCluster := range minioClusters {
				if minioCluster == cluster {
					test.NeedsMinio = true
					break
				}
			}
			if mysqlVersion == mysql57 {
				test.Platform = string(mysql57)
			}
			if strings.HasPrefix(cluster, "vreplication") || strings.HasSuffix(cluster, "heavy") {
				test.LimitResourceUsage = true
			}
			if strings.Contains(cluster, "vrepl") {
				test.EnableBinlogTransactionCompression = true
				test.EnablePartialJSON = true
			}
			mysqlVersionIndicator := ""
			if mysqlVersion != defaultMySQLVersion && len(clusterMySQLVersions()) > 1 {
				mysqlVersionIndicator = "_" + string(mysqlVersion)
				test.Name = test.Name + " " + string(mysqlVersion)
			}
			if strings.Contains(test.Shard, "partial_keyspace") {
				test.PartialKeyspace = true
			}

			workflowPath := fmt.Sprintf("%s/cluster_endtoend_%s%s.yml", workflowConfigDir, cluster, mysqlVersionIndicator)
			templateFileName := tpl
			if test.Platform != "" {
				templateFileName = fmt.Sprintf(tpl, "_"+test.Platform)
			} else if strings.Contains(templateFileName, "%s") {
				templateFileName = fmt.Sprintf(tpl, "")
			}
			test.FileName = fmt.Sprintf("cluster_endtoend_%s%s.yml", cluster, mysqlVersionIndicator)
			err := writeFileFromTemplate(templateFileName, workflowPath, test)
			if err != nil {
				log.Print(err)
			}
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

func evalengineToNameSuffix(evalengine string) string {
	if evalengine == "1" {
		return " evalengine"
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

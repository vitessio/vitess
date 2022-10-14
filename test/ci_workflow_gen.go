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
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"text/template"
)

type mysqlVersion string

const (
	mysql57    mysqlVersion = "mysql57"
	mysql80    mysqlVersion = "mysql80"
	mariadb103 mysqlVersion = "mariadb103"

	defaultMySQLVersion = mysql80
)

type mysqlVersions []mysqlVersion

var (
	defaultMySQLVersions = []mysqlVersion{defaultMySQLVersion}
	allMySQLVersions     = []mysqlVersion{mysql57, mysql80}
)

var (
	unitTestDatabases = []mysqlVersion{mysql57, mysql80, mariadb103}
)

const (
	workflowConfigDir = "../.github/workflows"

	unitTestTemplate = "templates/unit_test.tpl"

	// An empty string will cause the default non platform specific template
	// to be used.
	clusterTestTemplate = "templates/cluster_endtoend_test%s.tpl"

	unitTestSelfHostedTemplate    = "templates/unit_test_self_hosted.tpl"
	unitTestSelfHostedDatabases   = ""
	dockerFileTemplate            = "templates/dockerfile.tpl"
	clusterTestSelfHostedTemplate = "templates/cluster_endtoend_test_self_hosted.tpl"
	clusterTestDockerTemplate     = "templates/cluster_endtoend_test_docker.tpl"
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
		"vtbackup_transform",
		"18",
		"xb_backup",
		"21",
		"22",
		"mysql_server_vault",
		"26",
		"vstream_failover",
		"vstream_stoponreshard_true",
		"vstream_stoponreshard_false",
		"vstream_with_keyspaces_to_watch",
		"onlineddl_ghost",
		"onlineddl_vrepl",
		"onlineddl_vrepl_stress",
		"onlineddl_vrepl_stress_suite",
		"onlineddl_vrepl_suite",
		"vreplication_migrate_vdiff2_convert_tz",
		"onlineddl_revert",
		"onlineddl_declarative",
		"onlineddl_singleton",
		"onlineddl_scheduler",
		"onlineddl_revertible",
		"tabletmanager_throttler",
		"tabletmanager_throttler_custom_config",
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
		"vtgate_schema_tracker",
		"vtorc",
		"xb_recovery",
		"mysql80",
		"vreplication_across_db_versions",
		"vreplication_multicell",
		"vreplication_cellalias",
		"vreplication_basic",
		"vreplication_v2",
		"schemadiff_vrepl",
		"topo_connection_cache",
		"vtgate_partial_keyspace",
	}

	clusterSelfHostedList       = []string{}
	clusterDockerList           = []string{}
	clustersRequiringXtraBackup = []string{
		"xb_backup",
		"xb_recovery",
	}
	clustersRequiringMakeTools = []string{
		"18",
		"mysql_server_vault",
		"vtgate_topo_consul",
		"tabletmanager_consul",
	}
)

type unitTest struct {
	Name, Platform, FileName string
}

type clusterTest struct {
	Name, Shard, Platform        string
	FileName                     string
	MakeTools, InstallXtraBackup bool
	Docker                       bool
	LimitResourceUsage           bool
	PartialKeyspace              bool
}

type selfHostedTest struct {
	Name, Platform, Dockerfile, Shard, ImageName, directoryName string
	FileName                                                    string
	MakeTools, InstallXtraBackup, Docker                        bool
}

// clusterMySQLVersions return list of mysql versions (one or more) that this cluster needs to test against
func clusterMySQLVersions(clusterName string) mysqlVersions {
	switch {
	case strings.HasPrefix(clusterName, "onlineddl_"):
		return allMySQLVersions
	case clusterName == "schemadiff_vrepl":
		return allMySQLVersions
	case clusterName == "tabletmanager_tablegc":
		return allMySQLVersions
	case clusterName == "vtorc":
		return allMySQLVersions
	case clusterName == "xb_backup":
		return allMySQLVersions
	case clusterName == "xb_recovery":
		return allMySQLVersions
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
	generateUnitTestWorkflows()
	generateClusterWorkflows(clusterList, clusterTestTemplate)
	generateClusterWorkflows(clusterDockerList, clusterTestDockerTemplate)

	// tests that will use self-hosted runners
	err := generateSelfHostedUnitTestWorkflows()
	if err != nil {
		log.Fatal(err)
	}
	err = generateSelfHostedClusterWorkflows()
	if err != nil {
		log.Fatal(err)
	}
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

func parseList(csvList string) []string {
	var list []string
	for _, item := range strings.Split(csvList, ",") {
		if item != "" {
			list = append(list, strings.TrimSpace(item))
		}
	}
	return list
}

func generateSelfHostedUnitTestWorkflows() error {
	platforms := parseList(unitTestSelfHostedDatabases)
	for _, platform := range platforms {
		directoryName := fmt.Sprintf("unit_test_%s", platform)
		test := &selfHostedTest{
			Name:              fmt.Sprintf("Unit Test (%s)", platform),
			ImageName:         fmt.Sprintf("unit_test_%s", platform),
			Platform:          platform,
			directoryName:     directoryName,
			Dockerfile:        fmt.Sprintf("./.github/docker/%s/Dockerfile", directoryName),
			MakeTools:         true,
			InstallXtraBackup: false,
		}
		err := setupTestDockerFile(test)
		if err != nil {
			return err
		}
		test.FileName = fmt.Sprintf("unit_test_%s.yml", platform)
		filePath := fmt.Sprintf("%s/%s", workflowConfigDir, test.FileName)
		err = writeFileFromTemplate(unitTestSelfHostedTemplate, filePath, test)
		if err != nil {
			log.Print(err)
		}
	}
	return nil
}

func generateSelfHostedClusterWorkflows() error {
	clusters := canonnizeList(clusterSelfHostedList)
	for _, cluster := range clusters {
		for _, mysqlVersion := range clusterMySQLVersions(cluster) {
			// check mysqlversion
			mysqlVersionIndicator := ""
			if mysqlVersion != defaultMySQLVersion && len(clusterMySQLVersions(cluster)) > 1 {
				mysqlVersionIndicator = "_" + string(mysqlVersion)
			}

			directoryName := fmt.Sprintf("cluster_test_%s%s", cluster, mysqlVersionIndicator)
			test := &selfHostedTest{
				Name:              fmt.Sprintf("Cluster (%s)(%s)", cluster, mysqlVersion),
				ImageName:         fmt.Sprintf("cluster_test_%s%s", cluster, mysqlVersionIndicator),
				Platform:          "mysql80",
				directoryName:     directoryName,
				Dockerfile:        fmt.Sprintf("./.github/docker/%s/Dockerfile", directoryName),
				Shard:             cluster,
				MakeTools:         false,
				InstallXtraBackup: false,
			}
			makeToolClusters := canonnizeList(clustersRequiringMakeTools)
			for _, makeToolCluster := range makeToolClusters {
				if makeToolCluster == cluster {
					test.MakeTools = true
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
			if mysqlVersion == mysql57 {
				test.Platform = string(mysql57)
			}

			err := setupTestDockerFile(test)
			if err != nil {
				return err
			}

			test.FileName = fmt.Sprintf("cluster_endtoend_%s%s.yml", cluster, mysqlVersionIndicator)
			filePath := fmt.Sprintf("%s/%s", workflowConfigDir, test.FileName)
			err = writeFileFromTemplate(clusterTestSelfHostedTemplate, filePath, test)
			if err != nil {
				log.Print(err)
			}
		}
	}
	return nil
}

func generateClusterWorkflows(list []string, tpl string) {
	clusters := canonnizeList(list)
	for _, cluster := range clusters {
		for _, mysqlVersion := range clusterMySQLVersions(cluster) {
			test := &clusterTest{
				Name:  fmt.Sprintf("Cluster (%s)", cluster),
				Shard: cluster,
			}
			makeToolClusters := canonnizeList(clustersRequiringMakeTools)
			for _, makeToolCluster := range makeToolClusters {
				if makeToolCluster == cluster {
					test.MakeTools = true
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
			if mysqlVersion == mysql57 {
				test.Platform = string(mysql57)
			}
			if strings.HasPrefix(cluster, "vreplication") || strings.HasSuffix(cluster, "heavy") {
				test.LimitResourceUsage = true
			}
			mysqlVersionIndicator := ""
			if mysqlVersion != defaultMySQLVersion && len(clusterMySQLVersions(cluster)) > 1 {
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

func generateUnitTestWorkflows() {
	for _, platform := range unitTestDatabases {
		test := &unitTest{
			Name:     fmt.Sprintf("Unit Test (%s)", platform),
			Platform: string(platform),
		}
		test.FileName = fmt.Sprintf("unit_test_%s.yml", platform)
		path := fmt.Sprintf("%s/%s", workflowConfigDir, test.FileName)
		err := writeFileFromTemplate(unitTestTemplate, path, test)
		if err != nil {
			log.Print(err)
		}
	}
}

func setupTestDockerFile(test *selfHostedTest) error {
	// remove the directory
	relDirectoryName := fmt.Sprintf("../.github/docker/%s", test.directoryName)
	err := os.RemoveAll(relDirectoryName)
	if err != nil {
		return err
	}
	// create the directory
	err = os.MkdirAll(relDirectoryName, 0755)
	if err != nil {
		return err
	}

	// generate the docker file
	dockerFilePath := path.Join(relDirectoryName, "Dockerfile")
	err = writeFileFromTemplate(dockerFileTemplate, dockerFilePath, test)
	if err != nil {
		return err
	}

	return nil
}

func writeFileFromTemplate(templateFile, path string, test any) error {
	tpl, err := template.ParseFiles(templateFile)
	if err != nil {
		return fmt.Errorf("Error: %s\n", err)
	}

	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, test)
	if err != nil {
		return fmt.Errorf("Error: %s\n", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("Error creating file: %s\n", err)
	}
	if _, err := f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n"); err != nil {
		return err
	}
	if _, err := f.WriteString(mergeBlankLines(buf)); err != nil {
		return err
	}
	fmt.Printf("Generated %s\n", path)
	return nil
}

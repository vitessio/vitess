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

const (
	workflowConfigDir = "../.github/workflows"

	unitTestTemplate  = "templates/unit_test.tpl"
	unitTestDatabases = "percona56, mysql80, mariadb102"

	clusterTestTemplate = "templates/cluster_endtoend_test.tpl"

	unitTestSelfHostedTemplate    = "templates/unit_test_self_hosted.tpl"
	unitTestSelfHostedDatabases   = "mysql57, mariadb103"
	dockerFileTemplate            = "templates/dockerfile.tpl"
	clusterTestSelfHostedTemplate = "templates/cluster_endtoend_test_self_hosted.tpl"
)

var (
	clusterList = []string{
		"11",
		"12",
		"13",
		"14",
		"15",
		"16",
		"17",
		"18",
		"19",
		"20",
		"21",
		"22",
		"23",
		"24",
		"25",
		"26",
		"27",
		"vstream_failover",
		"vstream_stoponreshard_true",
		"vstream_stoponreshard_false",
		"onlineddl_ghost",
		"onlineddl_vrepl",
		"onlineddl_vrepl_stress",
		"onlineddl_vrepl_stress_suite",
		"onlineddl_vrepl_suite",
		"vreplication_migrate",
		"onlineddl_revert",
		"onlineddl_declarative",
		"onlineddl_singleton",
		"tabletmanager_throttler",
		"tabletmanager_throttler_custom_config",
		"tabletmanager_tablegc",
		"vtorc",
		"vtgate_buffer",
		"vtgate_concurrentdml",
		"vtgate_gen4",
		"vtgate_readafterwrite",
		"vtgate_reservedconn",
		"vtgate_schema",
		"vtgate_topo",
		"vtgate_transaction",
		"vtgate_unsharded",
		"vtgate_vindex",
		"vtgate_vschema",
		"xb_recovery",
		"resharding",
		"resharding_bytes",
		"mysql80",
	}

	clusterSelfHostedList = []string{
		"vreplication_basic",
		"vreplication_multicell",
		"vreplication_cellalias",
		"vreplication_v2",
	}
	// TODO: currently some percona tools including xtrabackup are installed on all clusters, we can possibly optimize
	// this by only installing them in the required clusters
	clustersRequiringXtraBackup = append(clusterList, clusterSelfHostedList...)
	clustersRequiringMakeTools  = []string{
		"18",
		"24",
	}
	clustersRequiringMySQL80 = []string{
		"mysql80",
	}
)

type unitTest struct {
	Name, Platform string
}

type clusterTest struct {
	Name, Shard                  string
	MakeTools, InstallXtraBackup bool
	Ubuntu20                     bool
}

type selfHostedTest struct {
	Name, Platform, Dockerfile, Shard, ImageName, directoryName string
	MakeTools, InstallXtraBackup                                bool
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
	generateClusterWorkflows()

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
		item = strings.TrimSpace(item)
		if item != "" {
			output = append(output, item)
		}
	}
	return output
}
func parseList(csvList string) []string {
	var list []string
	for _, item := range strings.Split(csvList, ",") {
		list = append(list, strings.TrimSpace(item))
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
		filePath := fmt.Sprintf("%s/unit_test_%s.yml", workflowConfigDir, platform)
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
		directoryName := fmt.Sprintf("cluster_test_%s", cluster)
		test := &selfHostedTest{
			Name:              fmt.Sprintf("Cluster (%s)", cluster),
			ImageName:         fmt.Sprintf("cluster_test_%s", cluster),
			Platform:          "mysql57",
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
		mysql80Clusters := canonnizeList(clustersRequiringMySQL80)
		for _, mysql80Cluster := range mysql80Clusters {
			if mysql80Cluster == cluster {
				test.Platform = "mysql80"
				break
			}
		}

		err := setupTestDockerFile(test)
		if err != nil {
			return err
		}
		filePath := fmt.Sprintf("%s/cluster_endtoend_%s.yml", workflowConfigDir, cluster)
		err = writeFileFromTemplate(clusterTestSelfHostedTemplate, filePath, test)
		if err != nil {
			log.Print(err)
		}
	}
	return nil
}

func generateClusterWorkflows() {
	clusters := canonnizeList(clusterList)
	for _, cluster := range clusters {
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
		ubuntu20Clusters := canonnizeList(clustersRequiringMySQL80)
		for _, ubuntu20Cluster := range ubuntu20Clusters {
			if ubuntu20Cluster == cluster {
				test.Ubuntu20 = true
				break
			}
		}

		path := fmt.Sprintf("%s/cluster_endtoend_%s.yml", workflowConfigDir, cluster)
		err := writeFileFromTemplate(clusterTestTemplate, path, test)
		if err != nil {
			log.Print(err)
		}
	}
}

func generateUnitTestWorkflows() {
	platforms := parseList(unitTestDatabases)
	for _, platform := range platforms {
		test := &unitTest{
			Name:     fmt.Sprintf("Unit Test (%s)", platform),
			Platform: platform,
		}
		path := fmt.Sprintf("%s/unit_test_%s.yml", workflowConfigDir, platform)
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

func writeFileFromTemplate(templateFile, path string, test interface{}) error {
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
	f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n")
	f.WriteString(mergeBlankLines(buf))
	fmt.Printf("Generated %s\n", path)
	return nil
}

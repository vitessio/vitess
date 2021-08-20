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
	unitTestDatabases = "percona56, mysql57, mysql80, mariadb102, mariadb103"

	clusterTestTemplate = "templates/cluster_endtoend_test.tpl"

	workflowBuildkitePipeline          = "../.buildkite/pipeline.yml"
	unitTestBuildkiteTemplate          = "templates/buildkite/unit_test.tpl"
	unitTestBuildkiteDatabases         = "percona, mysql57, mysql80, mariadb, mariadb103"
	dockerFileBuildkiteTemplate        = "templates/buildkite/dockerfile.tpl"
	dockerComposeFileBuildkiteTemplate = "templates/buildkite/docker-compose.tpl"
	clusterTestBuildkiteTemplate       = "templates/buildkite/cluster_endtoend_test.tpl"

	initialPipeline = "steps:\n"
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
		"26",
		"vreplication_basic",
		"vreplication_multicell",
		"vreplication_cellalias",
		"vstream_failover",
		"vreplication_v2",
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
	// TODO: currently some percona tools including xtrabackup are installed on all clusters, we can possibly optimize
	// this by only installing them in the required clusters
	clustersRequiringXtraBackup = clusterList
	clustersRequiringMakeTools  = []string{
		"18",
		"24",
	}
	clustersRequiringUbuntu20 = []string{
		"mysql80",
	}
)

type unitTest struct {
	Name, Platform string
}

type testBuildkite struct {
	Name, Platform, DockerCompose, Dockerfile, Shard, directoryName string
	MakeTools, InstallXtraBackup                                    bool
}

type clusterTest struct {
	Name, Shard                  string
	MakeTools, InstallXtraBackup bool
	Ubuntu20                     bool
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
	err := setupBuildkitePipelineFile()
	if err != nil {
		log.Fatal(err)
	}
	err = generateBuildkiteUnitTestWorkflows()
	if err != nil {
		log.Fatal(err)
	}
	err = generateBuildkiteClusterWorkflows()
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
		ubuntu20Clusters := canonnizeList(clustersRequiringUbuntu20)
		for _, ubuntu20Cluster := range ubuntu20Clusters {
			if ubuntu20Cluster == cluster {
				test.Ubuntu20 = true
				break
			}
		}

		path := fmt.Sprintf("%s/cluster_endtoend_%s.yml", workflowConfigDir, cluster)
		generateWorkflowFile(clusterTestTemplate, path, test)
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
		generateWorkflowFile(unitTestTemplate, path, test)
	}
}

func generateWorkflowFile(templateFile, path string, test interface{}) {
	tpl, err := template.ParseFiles(templateFile)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, test)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	f, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file: ", err)
		return
	}
	f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n")
	f.WriteString(mergeBlankLines(buf))
	fmt.Printf("Generated %s\n", path)

}

func generateBuildkiteUnitTestWorkflows() error {
	platforms := parseList(unitTestBuildkiteDatabases)
	for _, platform := range platforms {
		directoryName := fmt.Sprintf("unit_test_%s", platform)
		test := &testBuildkite{
			Name:              fmt.Sprintf("Unit Test (%s)", platform),
			Platform:          platform,
			directoryName:     directoryName,
			DockerCompose:     fmt.Sprintf("./.buildkite/%s/docker-compose.yml", directoryName),
			Dockerfile:        fmt.Sprintf("./.buildkite/%s/Dockerfile", directoryName),
			MakeTools:         true,
			InstallXtraBackup: false,
		}
		err := setupBuildkiteTestDockerFiles(test)
		if err != nil {
			return err
		}
		err = addToPipeline(unitTestBuildkiteTemplate, test)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateBuildkiteClusterWorkflows() error {
	clusters := canonnizeList(clusterList)
	for _, cluster := range clusters {
		if cluster != "11" && cluster != "18" && cluster != "mysql80" && cluster != "vreplication_v2" {
			continue
		}
		directoryName := fmt.Sprintf("cluster_test_%s", cluster)
		test := &testBuildkite{
			Name:              fmt.Sprintf("Cluster (%s)", cluster),
			Platform:          "mysql57",
			directoryName:     directoryName,
			DockerCompose:     fmt.Sprintf("./.buildkite/%s/docker-compose.yml", directoryName),
			Dockerfile:        fmt.Sprintf("./.buildkite/%s/Dockerfile", directoryName),
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
		ubuntu20Clusters := canonnizeList(clustersRequiringUbuntu20)
		for _, ubuntu20Cluster := range ubuntu20Clusters {
			if ubuntu20Cluster == cluster {
				test.Platform = "mysql80"
				break
			}
		}

		err := setupBuildkiteTestDockerFiles(test)
		if err != nil {
			return err
		}
		err = addToPipeline(clusterTestBuildkiteTemplate, test)
		if err != nil {
			return err
		}
	}
	return nil
}

func setupBuildkitePipelineFile() error {
	_ = os.Remove(workflowBuildkitePipeline)
	f, err := os.Create(workflowBuildkitePipeline)
	if err != nil {
		return err
	}
	f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n")
	f.WriteString(initialPipeline)
	return nil
}

func setupBuildkiteTestDockerFiles(test *testBuildkite) error {
	// remove the directory
	relDirectoryName := fmt.Sprintf("../.buildkite/%s", test.directoryName)
	err := os.RemoveAll(relDirectoryName)
	if err != nil {
		return err
	}
	// create the directory
	err = os.Mkdir(relDirectoryName, 0755)
	if err != nil {
		return err
	}

	// generate the docker file
	dockerFilePath := path.Join(relDirectoryName, "Dockerfile")
	err = writeFileFromTemplate(dockerFilePath, dockerFileBuildkiteTemplate, test)
	if err != nil {
		return err
	}

	// generate the docker compose file
	dockerComposeFilePath := path.Join(relDirectoryName, "docker-compose.yml")
	err = writeFileFromTemplate(dockerComposeFilePath, dockerComposeFileBuildkiteTemplate, test)
	if err != nil {
		return err
	}

	return nil
}

func writeFileFromTemplate(filePath string, templateFile string, test interface{}) error {
	tpl, err := template.ParseFiles(templateFile)
	if err != nil {
		return err
	}
	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, test)
	if err != nil {
		return err
	}
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n")
	f.WriteString(mergeBlankLines(buf))
	fmt.Printf("Generated %s\n", filePath)
	return nil
}

func addToPipeline(templateFile string, test interface{}) error {
	tpl, err := template.ParseFiles(templateFile)
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, test)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(workflowBuildkitePipeline, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	f.WriteString(mergeBlankLines(buf))
	f.Close()
	return nil
}

/*
Copyright 2020 The Vitess Authors.

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

package k8stopo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

func TestKubernetesTopo(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("k3s not supported on non-linux platforms. Skipping k8stopo integration tests")
	}

	// Create a data dir for test data
	testDataDir := t.TempDir()

	// Gen a temp file name for the config
	testConfig, err := os.CreateTemp("", "vt-test-k3s-config")
	if err != nil {
		t.Fatal(err)
	}
	testConfigPath := testConfig.Name()
	defer os.Remove(testConfigPath) // clean up

	k3sArgs := []string{
		"server", "start",
		"--write-kubeconfig=" + testConfigPath,
		"--data-dir=" + testDataDir,
		"--https-listen-port=6663",
		"--disable-agent", "--flannel-backend=none",
		"--disable-network-policy",
		"--disable-cloud-controller",
		"--disable-scheduler",
		"--no-deploy=coredns,servicelb,traefik,local-storage,metrics-server",
		"--kube-controller-manager-arg=port=10253",

		"--log=/tmp/k3svtlog",
	}

	// Start a minimal k3s daemon, and close it after all tests are done.
	ctx, killK3s := context.WithCancel(context.Background())
	c := exec.CommandContext(ctx, "k3s", k3sArgs...)

	// Start in the background and kill when tests end
	t.Log("Starting k3s")
	err = c.Start()
	if err != nil {
		t.Fatal("Unable to start k3s", err)
	}
	defer killK3s()

	// Wait for server to be ready
	for {
		t.Log("Waiting for server to be ready")
		time.Sleep(time.Second)
		config, err := clientcmd.BuildConfigFromFlags("", testConfigPath)
		if err != nil {
			continue
		}

		// Create the vitesstoponode crd
		apiextensionsClientSet, err := apiextensionsclient.NewForConfig(config)
		if err != nil {
			t.Fatal(err)
		}

		crdFile, err := os.Open("./VitessTopoNodes-crd.yaml")
		require.NoError(t, err)
		defer crdFile.Close()

		crd := &extensionsv1.CustomResourceDefinition{}

		kubeyaml.NewYAMLOrJSONDecoder(crdFile, 2048).Decode(crd)

		_, err = apiextensionsClientSet.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		break
	}

	serverAddr := "default"

	oldKubeConfigPath := kubeconfigPath
	kubeconfigPath = testConfigPath
	defer func() {
		kubeconfigPath = oldKubeConfigPath
	}()

	// Run the test suite.
	testIndex := 0
	test.TopoServerTestSuite(t, func() *topo.Server {
		// Each test will use its own sub-directories.
		// The directories will be created when used the first time.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		globalRoot := path.Join(testRoot, topo.GlobalCell)
		cellRoot := path.Join(testRoot, test.LocalCellName)

		ts, err := topo.OpenServer("k8s", serverAddr, globalRoot)
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          cellRoot,
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	})
}

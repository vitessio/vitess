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

/*
Package k8stopo implements topo.Server with the Kubernetes API as the backend.

We expect the following behavior from the kubernetes client library:

  - TODO

We follow these conventions within this package:

  - TODO
*/
package k8stopo

import (
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
	vtkube "vitess.io/vitess/go/vt/topo/k8stopo/client/clientset/versioned"
	vttyped "vitess.io/vitess/go/vt/topo/k8stopo/client/clientset/versioned/typed/topo/v1beta1"
)

var (
	// kubeconfigPath is a string that gives the location of a valid kubeconfig file
	kubeconfigPath = flag.String("topo_k8s_kubeconfig", "", "Path to a valid kubeconfig file.")

	// configContext is a string that can be used to override the default context
	configContext = flag.String("topo_k8s_context", "", "The kubeconfig context to use, overrides the 'current-context' from the config")

	// configNamespace is a string that can be used to override the default namespace for objects
	configNamespace = flag.String("topo_k8s_namespace", "", "The kubernetes namespace to use for all objects. Default comes from the context or in-cluster config")
)

// Factory is the Kubernetes topo.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(serverAddr, root)
}

// Server is the implementation of topo.Server for Kubernetes.
type Server struct {
	// kubeClient is the entire kubernetes interface
	kubeClient kubernetes.Interface

	// vtKubeClient is the client for vitess api types
	vtKubeClient vtkube.Interface

	// resource is a scoped-down kubernetes.Interface used for convenience
	resourceClient vttyped.VitessTopoNodeInterface

	// stopChan is used to tell the client-go informers to quit
	stopChan chan struct{}

	// memberInformer is the controller that syncronized the cache of data
	memberInformer cache.Controller

	// memberIndexer is the cache of tree data
	memberIndexer cache.Indexer

	// namespace is the Kubernetes namespace to be used for all resources
	namespace string

	// root is the root path for this client.
	// used for resource prefixing
	root string
}

// Close implements topo.Server.Close.
func (s *Server) Close() {
	close(s.stopChan)
}

func getKeyParents(key string) []string {
	parents := []string{""}
	parent := []string{}
	for _, segment := range strings.Split(filepath.Dir(key), "/") {
		parent = append(parent, segment)
		parents = append(parents, strings.Join(parent, "/"))
	}
	return parents
}

func indexByParent(obj interface{}) ([]string, error) {
	return getKeyParents(obj.(*vtv1beta1.VitessTopoNode).Data.Key), nil
}

// syncTree starts and syncs the member objects that form the directory "tree"
func (s *Server) syncTree() error {
	// Create the informer / indexer
	restClient := s.vtKubeClient.TopoV1beta1().RESTClient()
	listwatch := cache.NewListWatchFromClient(restClient, "vitesstoponodes", s.namespace, fields.Everything())

	// set up index funcs
	indexers := cache.Indexers{}
	indexers["by_parent"] = indexByParent

	s.memberIndexer, s.memberInformer = cache.NewIndexerInformer(listwatch, &vtv1beta1.VitessTopoNode{}, 0,
		cache.ResourceEventHandlerFuncs{}, indexers)

	// Start indexer
	go s.memberInformer.Run(s.stopChan)

	// Wait for sync
	log.Info("Waiting for Kubernetes topo cache sync")
	if !cache.WaitForCacheSync(s.stopChan, s.memberInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for caches to sync")
	}
	log.Info("Kubernetes topo cache sync completed")

	return nil
}

// NewServer returns a new k8stopo.Server.
func NewServer(_, root string) (*Server, error) {
	log.Info("Creating new Kubernetes topo server with root: ", root)

	var config *rest.Config
	var err error
	namespace := "default" //nolint

	if *kubeconfigPath == "" {
		log.Info("Creating new in-cluster Kubernetes config")

		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error getting Kubernetes in-cluster client config: %s", err)
		}

		// When running in the cluster, use the namespace file to detect the current namespace
		nsBytes, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			return nil, err
		}
		namespace = string(nsBytes)
	} else {
		log.Info("Creating new Kubernetes config from kubeconfig", *kubeconfigPath)

		configOverrides := &clientcmd.ConfigOverrides{}

		// respect the context flag
		if *configContext != "" {
			configOverrides.CurrentContext = *configContext
			log.V(7).Info("Overriding Kubernetes config context with: ", configOverrides.CurrentContext)
		}

		configLoader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfigPath},
			configOverrides,
		)

		config, err = configLoader.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("error getting Kubernetes client config: %s", err)
		}

		// When given a kubeconfig file, use the namespace from the current context
		namespace, _, err = configLoader.Namespace()
		if err != nil {
			return nil, fmt.Errorf("error getting namespace from Kubernetes client config: %s", err)
		}
	}

	// respect the namespace flag
	if *configNamespace != "" {
		namespace = *configNamespace
		log.V(7).Info("Overriding Kubernetes config namespace with: ", namespace)
	}

	// create the kubernetes client
	kubeClientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating official Kubernetes client: %s", err)
	}

	vtKubeClientset, err := vtkube.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating vitess Kubernetes client: %s", err)
	}

	// Create the server
	s := &Server{
		namespace:      namespace,
		kubeClient:     kubeClientset,
		vtKubeClient:   vtKubeClientset,
		resourceClient: vtKubeClientset.TopoV1beta1().VitessTopoNodes(namespace),
		root:           root,
		stopChan:       make(chan struct{}),
	}

	// Sync cache
	if err = s.syncTree(); err != nil {
		return nil, err
	}

	return s, nil
}

func init() {
	topo.RegisterFactory("k8s", Factory{})
}

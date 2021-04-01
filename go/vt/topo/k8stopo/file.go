/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8stopo

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
)

// NodeReference contains the data relating to a node
type NodeReference struct {
	id    string
	key   string
	value string
}

func packValue(value []byte) ([]byte, error) {
	encoded := &bytes.Buffer{}
	encoder := base64.NewEncoder(base64.StdEncoding, encoded)

	zw := gzip.NewWriter(encoder)
	_, err := zw.Write(value)
	if err != nil {
		return []byte{}, fmt.Errorf("gzip write error: %s", err)
	}

	err = zw.Close()
	if err != nil {
		return []byte{}, fmt.Errorf("gzip close error: %s", err)
	}

	err = encoder.Close()
	if err != nil {
		return []byte{}, fmt.Errorf("base64 encoder close error: %s", err)
	}

	return encoded.Bytes(), nil
}

func unpackValue(value []byte) ([]byte, error) {
	decoder := base64.NewDecoder(base64.StdEncoding, bytes.NewBuffer(value))

	zr, err := gzip.NewReader(decoder)
	if err != nil {
		return []byte{}, fmt.Errorf("unable to create new gzip reader: %s", err)
	}

	decoded := &bytes.Buffer{}
	if _, err := io.Copy(decoded, zr); err != nil {
		return []byte{}, fmt.Errorf("error coppying uncompressed data: %s", err)
	}

	if err := zr.Close(); err != nil {
		return []byte{}, fmt.Errorf("unable to close gzip reader: %s", err)
	}

	return decoded.Bytes(), nil
}

// ToData converts a nodeReference to the data type used in the VitessTopoNode
func (n *NodeReference) ToData() vtv1beta1.VitessTopoNodeData {
	return vtv1beta1.VitessTopoNodeData{
		Key:   n.key,
		Value: string(n.value),
	}
}

func getHash(parent string) string {
	hasher := fnv.New64a()
	hasher.Write([]byte(parent))
	return strconv.FormatUint(hasher.Sum64(), 10)
}

func (s *Server) newNodeReference(key string) *NodeReference {
	key = filepath.Join(s.root, key)

	node := &NodeReference{
		id:  fmt.Sprintf("vt-%s", getHash(key)),
		key: key,
	}

	return node
}

func (s *Server) buildFileResource(filePath string, contents []byte) (*vtv1beta1.VitessTopoNode, error) {
	node := s.newNodeReference(filePath)

	value, err := packValue(contents)
	if err != nil {
		return nil, err
	}

	// create data
	node.value = string(value)

	// Create "file" object
	return &vtv1beta1.VitessTopoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      node.id,
			Namespace: s.namespace,
		},
		Data: node.ToData(),
	}, nil
}

// Create is part of the topo.Conn interface.
func (s *Server) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	log.V(7).Infof("Create at '%s' Contents: '%s'", filePath, string(contents))

	resource, err := s.buildFileResource(filePath, contents)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	final, err := s.resourceClient.Create(resource)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	// Update the internal cache
	err = s.memberIndexer.Update(final)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	return KubernetesVersion(final.GetResourceVersion()), nil
}

// Update is part of the topo.Conn interface.
func (s *Server) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	log.V(7).Infof("Update at '%s' Contents: '%s'", filePath, string(contents))

	resource, err := s.buildFileResource(filePath, contents)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	var finalVersion KubernetesVersion

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		result, err := s.resourceClient.Get(resource.Name, metav1.GetOptions{})
		if err != nil && errors.IsNotFound(err) && version == nil {
			// Update should create objects when the version is nil and the object is not found
			createdVersion, err := s.Create(ctx, filePath, contents)
			if err != nil {
				return err
			}
			finalVersion = KubernetesVersion(createdVersion.String())
			return nil
		}

		// If a non-nil version is given to update, fail on mismatched version
		if version != nil && KubernetesVersion(result.GetResourceVersion()) != version {
			return topo.NewError(topo.BadVersion, filePath)
		}

		// set new contents
		result.Data.Value = resource.Data.Value

		// get result or err
		final, err := s.resourceClient.Update(result)
		if err != nil {
			return convertError(err, filePath)
		}

		// Update the internal cache
		err = s.memberIndexer.Update(final)
		if err != nil {
			return convertError(err, filePath)
		}

		finalVersion = KubernetesVersion(final.GetResourceVersion())

		return nil
	})
	if err != nil {
		return nil, err
	}

	return finalVersion, nil
}

// Get is part of the topo.Conn interface.
func (s *Server) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	log.V(7).Infof("Get at '%s'", filePath)

	node := s.newNodeReference(filePath)

	result, err := s.resourceClient.Get(node.id, metav1.GetOptions{})
	if err != nil {
		return []byte{}, nil, convertError(err, filePath)
	}

	out, err := unpackValue([]byte(result.Data.Value))
	if err != nil {
		return []byte{}, nil, convertError(err, filePath)
	}

	return out, KubernetesVersion(result.GetResourceVersion()), nil
}

// Delete is part of the topo.Conn interface.
func (s *Server) Delete(ctx context.Context, filePath string, version topo.Version) error {
	log.V(7).Infof("Delete at '%s'", filePath)

	node := s.newNodeReference(filePath)

	// Check version before delete
	current, err := s.resourceClient.Get(node.id, metav1.GetOptions{})
	if err != nil {
		return convertError(err, filePath)
	}
	if version != nil {
		if KubernetesVersion(current.GetResourceVersion()) != version {
			return topo.NewError(topo.BadVersion, filePath)
		}
	}

	err = s.resourceClient.Delete(node.id, &metav1.DeleteOptions{})
	if err != nil {
		return convertError(err, filePath)
	}

	// Wait for one of the following conditions
	// 1. Context is cancelled
	// 2. The object is no longer in the cache
	// 3. The object in the cache has a new uid (was deleted but recreated since we last checked)
	for {
		select {
		case <-ctx.Done():
			return convertError(ctx.Err(), filePath)
		case <-time.After(50 * time.Millisecond):
		}

		obj, ok, err := s.memberIndexer.Get(current)
		if err != nil { // error getting from cache
			return convertError(err, filePath)
		}
		if !ok { // deleted from cache
			break
		}
		cached := obj.(*vtv1beta1.VitessTopoNode)
		if cached.GetUID() != current.GetUID() {
			break // deleted and recreated
		}
	}

	return nil
}

/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"maps"
	"os"
	"strings"
)

// Image resolves the Docker image for Vitess components, for tests that run
// extra containers from the same image. The VITESST_IMAGE environment
// variable overrides everything; otherwise the image tag is derived from the
// MySQL version (e.g. "vitesst:mysql84").
func Image(mysqlVersion string) string {
	return vitesstImage(mysqlVersion)
}

// vitesstImage resolves the Docker image for Vitess components. The
// VITESST_IMAGE environment variable overrides everything; otherwise the
// image tag is derived from the MySQL version (e.g. "vitesst:mysql84").
func vitesstImage(mysqlVersion string) string {
	if image := os.Getenv("VITESST_IMAGE"); image != "" {
		return image
	}
	return "vitesst:mysql" + strings.ReplaceAll(mysqlVersion, ".", "")
}

// vtgateImage returns the Docker image the cluster's vtgate containers run:
// the VITESST_VTGATE_IMAGE override, or the cluster image.
func (c *Cluster) vtgateImage() string {
	if image := os.Getenv("VITESST_VTGATE_IMAGE"); image != "" {
		return image
	}
	return c.image
}

// vtctldImage returns the Docker image the cluster's vtctld container runs:
// the VITESST_VTCTLD_IMAGE override, or the cluster image.
func (c *Cluster) vtctldImage() string {
	if image := os.Getenv("VITESST_VTCTLD_IMAGE"); image != "" {
		return image
	}
	return c.image
}

// mergeEnv overlays extra environment variables onto base.
func mergeEnv(base, extra map[string]string) map[string]string {
	maps.Copy(base, extra)
	return base
}

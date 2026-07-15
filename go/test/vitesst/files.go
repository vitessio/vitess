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
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/testcontainers/testcontainers-go"
)

type (
	// ContainerFile describes a file placed into a component container before
	// it starts. Either Content or HostPath must be set; Content wins when
	// both are. Files are copied in as root, so Mode must grant the vitess
	// user access; it defaults to 0o644. The parent directory must already
	// exist in the image: /vt/files is provided for exactly this purpose.
	ContainerFile struct {
		// Content is the literal file content.
		Content []byte

		// HostPath is a file on the host to copy in.
		HostPath string

		// ContainerPath is the absolute destination path in the container.
		ContainerPath string

		// Mode is the file mode; 0 means 0o644.
		Mode int64
	}

	tabletFilesOption []ContainerFile

	vtgateFilesOption []ContainerFile

	initDBSQLOption string

	initDBSQLExtraOption string
)

// toTestcontainers converts a ContainerFile into the testcontainers
// representation, validating it.
func (f ContainerFile) toTestcontainers() (testcontainers.ContainerFile, error) {
	mode := f.Mode
	if mode == 0 {
		mode = 0o644
	}

	tcf := testcontainers.ContainerFile{
		ContainerFilePath: f.ContainerPath,
		FileMode:          mode,
	}

	switch {
	case f.ContainerPath == "":
		return tcf, errors.New("ContainerFile.ContainerPath cannot be empty")
	case len(f.Content) > 0:
		tcf.Reader = bytes.NewReader(f.Content)
	case f.HostPath != "":
		if _, err := os.Stat(f.HostPath); err != nil {
			return tcf, fmt.Errorf("ContainerFile.HostPath %s: %w", f.HostPath, err)
		}
		tcf.HostFilePath = f.HostPath
	default:
		return tcf, fmt.Errorf("ContainerFile for %s needs Content or HostPath", f.ContainerPath)
	}

	return tcf, nil
}

// withContainerFiles converts ContainerFiles into a testcontainers customizer.
func withContainerFiles(files []ContainerFile) (testcontainers.CustomizeRequestOption, error) {
	tcFiles := make([]testcontainers.ContainerFile, 0, len(files))
	for _, f := range files {
		tcf, err := f.toTestcontainers()
		if err != nil {
			return nil, err
		}
		tcFiles = append(tcFiles, tcf)
	}
	return testcontainers.WithFiles(tcFiles...), nil
}

func (o tabletFilesOption) apply(opts *clusterOptions) {
	opts.tabletFiles = append(opts.tabletFiles, o...)
}

// WithTabletFiles places files into every tablet container before it starts.
func WithTabletFiles(files ...ContainerFile) ClusterOption {
	return tabletFilesOption(files)
}

func (o vtgateFilesOption) apply(opts *clusterOptions) {
	opts.vtgateFiles = append(opts.vtgateFiles, o...)
}

// WithVTGateFiles places files into every vtgate container before it starts.
func WithVTGateFiles(files ...ContainerFile) ClusterOption {
	return vtgateFilesOption(files)
}

func (o initDBSQLOption) apply(opts *clusterOptions) {
	opts.initDBSQL = string(o)
}

// WithInitDBSQL replaces the init_db.sql content used by mysqlctl init in
// every tablet container. The framework does not modify the given content, so
// it must grant whatever host access the test needs (the default content
// grants vt_dba access from the host).
func WithInitDBSQL(sql string) ClusterOption {
	return initDBSQLOption(sql)
}

func (o initDBSQLExtraOption) apply(opts *clusterOptions) {
	opts.initDBSQLExtra = string(o)
}

// WithInitDBSQLExtra splices extra SQL into the default init_db.sql at its
// custom-SQL marker, after the framework's own vt_dba host-access grants.
func WithInitDBSQLExtra(sql string) ClusterOption {
	return initDBSQLExtraOption(sql)
}

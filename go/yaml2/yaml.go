// Package yaml2 ensures that the right yaml package gets imported.
// The default package that goimports adds is not the one we want to use.
package yaml2

import "sigs.k8s.io/yaml"

var (
	// Marshal marshals to YAML.
	Marshal = yaml.Marshal
	// Unmarshal unmarshals from YAML.
	Unmarshal = yaml.Unmarshal
)

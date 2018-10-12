/*
Copyright 2014 The Kubernetes Authors.

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

package k8s

// Where possible, json tags match the cli argument names.
// Top level config objects and all values required for proper functioning are not "omitempty".  Any truly optional piece of config is allowed to be omitted.

// Config holds the information needed to build connect to remote kubernetes clusters as a given user
type Config struct {
	// Legacy field from pkg/api/types.go TypeMeta.
	// TODO(jlowdermilk): remove this after eliminating downstream dependencies.
	// +optional
	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`
	// DEPRECATED: APIVersion is the preferred api version for communicating with the kubernetes cluster (v1, v2, etc).
	// Because a cluster can run multiple API groups and potentially multiple versions of each, it no longer makes sense to specify
	// a single value for the cluster version.
	// This field isn't really needed anyway, so we are deprecating it without replacement.
	// It will be ignored if it is present.
	// +optional
	APIVersion string `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	// Preferences holds general information to be use for cli interactions
	Preferences Preferences `json:"preferences" yaml:"preferences"`
	// Clusters is a map of referencable names to cluster configs
	Clusters []NamedCluster `json:"clusters" yaml:"clusters"`
	// AuthInfos is a map of referencable names to user configs
	AuthInfos []NamedAuthInfo `json:"users" yaml:"users"`
	// Contexts is a map of referencable names to context configs
	Contexts []NamedContext `json:"contexts" yaml:"contexts"`
	// CurrentContext is the name of the context that you would like to use by default
	CurrentContext string `json:"current-context" yaml:"current-context"`
	// Extensions holds additional information. This is useful for extenders so that reads and writes don't clobber unknown fields
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty" yaml:"extensions,omitempty"`
}

type Preferences struct {
	// +optional
	Colors bool `json:"colors,omitempty" yaml:"colors,omitempty"`
	// Extensions holds additional information. This is useful for extenders so that reads and writes don't clobber unknown fields
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty" yaml:"extensions,omitempty"`
}

// Cluster contains information about how to communicate with a kubernetes cluster
type Cluster struct {
	// Server is the address of the kubernetes cluster (https://hostname:port).
	Server string `json:"server" yaml:"server"`
	// APIVersion is the preferred api version for communicating with the kubernetes cluster (v1, v2, etc).
	// +optional
	APIVersion string `json:"api-version,omitempty" yaml:"api-version,omitempty"`
	// InsecureSkipTLSVerify skips the validity check for the server's certificate. This will make your HTTPS connections insecure.
	// +optional
	InsecureSkipTLSVerify bool `json:"insecure-skip-tls-verify,omitempty" yaml:"insecure-skip-tls-verify,omitempty"`
	// CertificateAuthority is the path to a cert file for the certificate authority.
	// +optional
	CertificateAuthority string `json:"certificate-authority,omitempty" yaml:"certificate-authority,omitempty"`
	// CertificateAuthorityData contains PEM-encoded certificate authority certificates. Overrides CertificateAuthority
	// +optional
	CertificateAuthorityData []byte `json:"certificate-authority-data,omitempty" yaml:"certificate-authority-data,omitempty"`
	// Extensions holds additional information. This is useful for extenders so that reads and writes don't clobber unknown fields
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty" yaml:"extensions,omitempty"`
}

// AuthInfo contains information that describes identity information.  This is use to tell the kubernetes cluster who you are.
type AuthInfo struct {
	// ClientCertificate is the path to a client cert file for TLS.
	// +optional
	ClientCertificate string `json:"client-certificate,omitempty" yaml:"client-certificate,omitempty"`
	// ClientCertificateData contains PEM-encoded data from a client cert file for TLS. Overrides ClientCertificate
	// +optional
	ClientCertificateData []byte `json:"client-certificate-data,omitempty" yaml:"client-certificate-data,omitempty"`
	// ClientKey is the path to a client key file for TLS.
	// +optional
	ClientKey string `json:"client-key,omitempty" yaml:"client-key,omitempty"`
	// ClientKeyData contains PEM-encoded data from a client key file for TLS. Overrides ClientKey
	// +optional
	ClientKeyData []byte `json:"client-key-data,omitempty" yaml:"client-key-data,omitempty"`
	// Token is the bearer token for authentication to the kubernetes cluster.
	// +optional
	Token string `json:"token,omitempty" yaml:"token,omitempty"`
	// TokenFile is a pointer to a file that contains a bearer token (as described above).  If both Token and TokenFile are present, Token takes precedence.
	// +optional
	TokenFile string `json:"tokenFile,omitempty" yaml:"tokenFile,omitempty"`
	// Impersonate is the username to imperonate.  The name matches the flag.
	// +optional
	Impersonate string `json:"as,omitempty" yaml:"as,omitempty"`
	// Username is the username for basic authentication to the kubernetes cluster.
	// +optional
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	// Password is the password for basic authentication to the kubernetes cluster.
	// +optional
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	// AuthProvider specifies a custom authentication plugin for the kubernetes cluster.
	// +optional
	AuthProvider *AuthProviderConfig `json:"auth-provider,omitempty" yaml:"auth-provider,omitempty"`
	// Extensions holds additional information. This is useful for extenders so that reads and writes don't clobber unknown fields
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty" yaml:"extensions,omitempty"`
}

// Context is a tuple of references to a cluster (how do I communicate with a kubernetes cluster), a user (how do I identify myself), and a namespace (what subset of resources do I want to work with)
type Context struct {
	// Cluster is the name of the cluster for this context
	Cluster string `json:"cluster" yaml:"cluster"`
	// AuthInfo is the name of the authInfo for this context
	AuthInfo string `json:"user" yaml:"user"`
	// Namespace is the default namespace to use on unspecified requests
	// +optional
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// Extensions holds additional information. This is useful for extenders so that reads and writes don't clobber unknown fields
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty" yaml:"extensions,omitempty"`
}

// NamedCluster relates nicknames to cluster information
type NamedCluster struct {
	// Name is the nickname for this Cluster
	Name string `json:"name" yaml:"name"`
	// Cluster holds the cluster information
	Cluster Cluster `json:"cluster" yaml:"cluster"`
}

// NamedContext relates nicknames to context information
type NamedContext struct {
	// Name is the nickname for this Context
	Name string `json:"name" yaml:"name"`
	// Context holds the context information
	Context Context `json:"context" yaml:"context"`
}

// NamedAuthInfo relates nicknames to auth information
type NamedAuthInfo struct {
	// Name is the nickname for this AuthInfo
	Name string `json:"name" yaml:"name"`
	// AuthInfo holds the auth information
	AuthInfo AuthInfo `json:"user" yaml:"user"`
}

// NamedExtension relates nicknames to extension information
type NamedExtension struct {
	// Name is the nickname for this Extension
	Name string `json:"name" yaml:"name"`
	// Extension holds the extension information
	Extension interface{} `json:"extension" yaml:"extension"`
}

// AuthProviderConfig holds the configuration for a specified auth provider.
type AuthProviderConfig struct {
	Name   string            `json:"name" yaml:"name"`
	Config map[string]string `json:"config" yaml:"config"`
}

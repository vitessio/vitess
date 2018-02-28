// Package replicapool provides access to the Replica Pool API.
//
// See https://developers.google.com/compute/docs/replica-pool/
//
// Usage example:
//
//   import "google.golang.org/api/replicapool/v1beta1"
//   ...
//   replicapoolService, err := replicapool.New(oauthHttpClient)
package replicapool // import "google.golang.org/api/replicapool/v1beta1"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	context "golang.org/x/net/context"
	ctxhttp "golang.org/x/net/context/ctxhttp"
	gensupport "google.golang.org/api/gensupport"
	googleapi "google.golang.org/api/googleapi"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Always reference these packages, just in case the auto-generated code
// below doesn't.
var _ = bytes.NewBuffer
var _ = strconv.Itoa
var _ = fmt.Sprintf
var _ = json.NewDecoder
var _ = io.Copy
var _ = url.Parse
var _ = gensupport.MarshalJSON
var _ = googleapi.Version
var _ = errors.New
var _ = strings.Replace
var _ = context.Canceled
var _ = ctxhttp.Do

const apiId = "replicapool:v1beta1"
const apiName = "replicapool"
const apiVersion = "v1beta1"
const basePath = "https://www.googleapis.com/replicapool/v1beta1/projects/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// View your data across Google Cloud Platform services
	CloudPlatformReadOnlyScope = "https://www.googleapis.com/auth/cloud-platform.read-only"

	// View and manage your Google Cloud Platform management resources and
	// deployment status information
	NdevCloudmanScope = "https://www.googleapis.com/auth/ndev.cloudman"

	// View your Google Cloud Platform management resources and deployment
	// status information
	NdevCloudmanReadonlyScope = "https://www.googleapis.com/auth/ndev.cloudman.readonly"

	// View and manage replica pools
	ReplicapoolScope = "https://www.googleapis.com/auth/replicapool"

	// View replica pools
	ReplicapoolReadonlyScope = "https://www.googleapis.com/auth/replicapool.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Pools = NewPoolsService(s)
	s.Replicas = NewReplicasService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Pools *PoolsService

	Replicas *ReplicasService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewPoolsService(s *Service) *PoolsService {
	rs := &PoolsService{s: s}
	return rs
}

type PoolsService struct {
	s *Service
}

func NewReplicasService(s *Service) *ReplicasService {
	rs := &ReplicasService{s: s}
	return rs
}

type ReplicasService struct {
	s *Service
}

// AccessConfig: A Compute Engine network accessConfig. Identical to the
// accessConfig on corresponding Compute Engine resource.
type AccessConfig struct {
	// Name: Name of this access configuration.
	Name string `json:"name,omitempty"`

	// NatIp: An external IP address associated with this instance.
	NatIp string `json:"natIp,omitempty"`

	// Type: Type of this access configuration file. Currently only
	// ONE_TO_ONE_NAT is supported.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Name") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Name") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AccessConfig) MarshalJSON() ([]byte, error) {
	type noMethod AccessConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Action: An action that gets executed during initialization of the
// replicas.
type Action struct {
	// Commands: A list of commands to run, one per line. If any command
	// fails, the whole action is considered a failure and no further
	// actions are run. This also marks the virtual machine or replica as a
	// failure.
	Commands []string `json:"commands,omitempty"`

	// EnvVariables: A list of environment variables to use for the commands
	// in this action.
	EnvVariables []*EnvVariable `json:"envVariables,omitempty"`

	// TimeoutMilliSeconds: If an action's commands on a particular replica
	// do not finish in the specified timeoutMilliSeconds, the replica is
	// considered to be in a FAILING state. No efforts are made to stop any
	// processes that were spawned or created as the result of running the
	// action's commands. The default is the max allowed value, 1 hour (i.e.
	// 3600000 milliseconds).
	TimeoutMilliSeconds int64 `json:"timeoutMilliSeconds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Commands") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Commands") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Action) MarshalJSON() ([]byte, error) {
	type noMethod Action
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DiskAttachment: Specifies how to attach a disk to a Replica.
type DiskAttachment struct {
	// DeviceName: The device name of this disk.
	DeviceName string `json:"deviceName,omitempty"`

	// Index: A zero-based index to assign to this disk, where 0 is reserved
	// for the boot disk. If not specified, this is assigned by the server.
	Index int64 `json:"index,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DiskAttachment) MarshalJSON() ([]byte, error) {
	type noMethod DiskAttachment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EnvVariable: An environment variable to set for an action.
type EnvVariable struct {
	// Hidden: Deprecated, do not use.
	Hidden bool `json:"hidden,omitempty"`

	// Name: The name of the environment variable.
	Name string `json:"name,omitempty"`

	// Value: The value of the variable.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Hidden") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Hidden") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EnvVariable) MarshalJSON() ([]byte, error) {
	type noMethod EnvVariable
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ExistingDisk: A pre-existing persistent disk that will be attached to
// every Replica in the Pool in READ_ONLY mode.
type ExistingDisk struct {
	// Attachment: How the disk will be attached to the Replica.
	Attachment *DiskAttachment `json:"attachment,omitempty"`

	// Source: The name of the Persistent Disk resource. The Persistent Disk
	// resource must be in the same zone as the Pool.
	Source string `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Attachment") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Attachment") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ExistingDisk) MarshalJSON() ([]byte, error) {
	type noMethod ExistingDisk
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type HealthCheck struct {
	// CheckIntervalSec: How often (in seconds) to make HTTP requests for
	// this healthcheck. The default value is 5 seconds.
	CheckIntervalSec int64 `json:"checkIntervalSec,omitempty"`

	// Description: The description for this health check.
	Description string `json:"description,omitempty"`

	// HealthyThreshold: The number of consecutive health check requests
	// that need to succeed before the replica is considered healthy again.
	// The default value is 2.
	HealthyThreshold int64 `json:"healthyThreshold,omitempty"`

	// Host: The value of the host header in the HTTP health check request.
	// If left empty (default value), the localhost IP 127.0.0.1 will be
	// used.
	Host string `json:"host,omitempty"`

	// Name: The name of this health check.
	Name string `json:"name,omitempty"`

	// Path: The localhost request path to send this health check, in the
	// format /path/to/use. For example, /healthcheck.
	Path string `json:"path,omitempty"`

	// Port: The TCP port for the health check requests.
	Port int64 `json:"port,omitempty"`

	// TimeoutSec: How long (in seconds) to wait before a timeout failure
	// for this healthcheck. The default value is 5 seconds.
	TimeoutSec int64 `json:"timeoutSec,omitempty"`

	// UnhealthyThreshold: The number of consecutive health check requests
	// that need to fail in order to consider the replica unhealthy. The
	// default value is 2.
	UnhealthyThreshold int64 `json:"unhealthyThreshold,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CheckIntervalSec") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CheckIntervalSec") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *HealthCheck) MarshalJSON() ([]byte, error) {
	type noMethod HealthCheck
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Label: A label to apply to this replica pool.
type Label struct {
	// Key: The key for this label.
	Key string `json:"key,omitempty"`

	// Value: The value of this label.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Key") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Key") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Label) MarshalJSON() ([]byte, error) {
	type noMethod Label
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Metadata: A Compute Engine metadata entry. Identical to the metadata
// on the corresponding Compute Engine resource.
type Metadata struct {
	// FingerPrint: The fingerprint of the metadata. Required for updating
	// the metadata entries for this instance.
	FingerPrint string `json:"fingerPrint,omitempty"`

	// Items: A list of metadata items.
	Items []*MetadataItem `json:"items,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FingerPrint") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FingerPrint") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Metadata) MarshalJSON() ([]byte, error) {
	type noMethod Metadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MetadataItem: A Compute Engine metadata item, defined as a key:value
// pair. Identical to the metadata on the corresponding Compute Engine
// resource.
type MetadataItem struct {
	// Key: A metadata key.
	Key string `json:"key,omitempty"`

	// Value: A metadata value.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Key") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Key") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MetadataItem) MarshalJSON() ([]byte, error) {
	type noMethod MetadataItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// NetworkInterface: A Compute Engine NetworkInterface resource.
// Identical to the NetworkInterface on the corresponding Compute Engine
// resource.
type NetworkInterface struct {
	// AccessConfigs: An array of configurations for this interface. This
	// specifies how this interface is configured to interact with other
	// network services.
	AccessConfigs []*AccessConfig `json:"accessConfigs,omitempty"`

	// Network: Name the Network resource to which this interface applies.
	Network string `json:"network,omitempty"`

	// NetworkIp: An optional IPV4 internal network address to assign to the
	// instance for this network interface.
	NetworkIp string `json:"networkIp,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AccessConfigs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccessConfigs") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NetworkInterface) MarshalJSON() ([]byte, error) {
	type noMethod NetworkInterface
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// NewDisk: A Persistent Disk resource that will be created and attached
// to each Replica in the Pool. Each Replica will have a unique
// persistent disk that is created and attached to that Replica in
// READ_WRITE mode.
type NewDisk struct {
	// Attachment: How the disk will be attached to the Replica.
	Attachment *DiskAttachment `json:"attachment,omitempty"`

	// AutoDelete: If true, then this disk will be deleted when the instance
	// is deleted. The default value is true.
	AutoDelete bool `json:"autoDelete,omitempty"`

	// Boot: If true, indicates that this is the root persistent disk.
	Boot bool `json:"boot,omitempty"`

	// InitializeParams: Create the new disk using these parameters. The
	// name of the disk will be <instance_name>-<four_random_charactersgt;.
	InitializeParams *NewDiskInitializeParams `json:"initializeParams,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Attachment") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Attachment") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NewDisk) MarshalJSON() ([]byte, error) {
	type noMethod NewDisk
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// NewDiskInitializeParams: Initialization parameters for creating a new
// disk.
type NewDiskInitializeParams struct {
	// DiskSizeGb: The size of the created disk in gigabytes.
	DiskSizeGb int64 `json:"diskSizeGb,omitempty,string"`

	// DiskType: Name of the disk type resource describing which disk type
	// to use to create the disk. For example 'pd-ssd' or 'pd-standard'.
	// Default is 'pd-standard'
	DiskType string `json:"diskType,omitempty"`

	// SourceImage: The name or fully-qualified URL of a source image to use
	// to create this disk. If you provide a name of the source image,
	// Replica Pool will look for an image with that name in your project.
	// If you are specifying an image provided by Compute Engine, you will
	// need to provide the full URL with the correct project, such
	// as:
	// http://www.googleapis.com/compute/v1/projects/debian-cloud/
	// global/images/debian-wheezy-7-vYYYYMMDD
	SourceImage string `json:"sourceImage,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DiskSizeGb") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DiskSizeGb") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NewDiskInitializeParams) MarshalJSON() ([]byte, error) {
	type noMethod NewDiskInitializeParams
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Pool struct {
	// AutoRestart: Whether replicas in this pool should be restarted if
	// they experience a failure. The default value is true.
	AutoRestart bool `json:"autoRestart,omitempty"`

	// BaseInstanceName: The base instance name to use for the replicas in
	// this pool. This must match the regex [a-z]([-a-z0-9]*[a-z0-9])?. If
	// specified, the instances in this replica pool will be named in the
	// format <base-instance-name>-<ID>. The <ID> postfix will be a four
	// character alphanumeric identifier generated by the service.
	//
	// If this is not specified by the user, a random base instance name is
	// generated by the service.
	BaseInstanceName string `json:"baseInstanceName,omitempty"`

	// CurrentNumReplicas: [Output Only] The current number of replicas in
	// the pool.
	CurrentNumReplicas int64 `json:"currentNumReplicas,omitempty"`

	// Description: An optional description of the replica pool.
	Description string `json:"description,omitempty"`

	// HealthChecks: Deprecated. Please use template[].healthChecks instead.
	HealthChecks []*HealthCheck `json:"healthChecks,omitempty"`

	// InitialNumReplicas: The initial number of replicas this pool should
	// have. You must provide a value greater than or equal to 0.
	InitialNumReplicas int64 `json:"initialNumReplicas,omitempty"`

	// Labels: A list of labels to attach to this replica pool and all
	// created virtual machines in this replica pool.
	Labels []*Label `json:"labels,omitempty"`

	// Name: The name of the replica pool. Must follow the regex
	// [a-z]([-a-z0-9]*[a-z0-9])? and be 1-28 characters long.
	Name string `json:"name,omitempty"`

	// NumReplicas: Deprecated! Use initial_num_replicas instead.
	NumReplicas int64 `json:"numReplicas,omitempty"`

	// ResourceViews: The list of resource views that should be updated with
	// all the replicas that are managed by this pool.
	ResourceViews []string `json:"resourceViews,omitempty"`

	// SelfLink: [Output Only] A self-link to the replica pool.
	SelfLink string `json:"selfLink,omitempty"`

	// TargetPool: Deprecated, please use target_pools instead.
	TargetPool string `json:"targetPool,omitempty"`

	// TargetPools: A list of target pools to update with the replicas that
	// are managed by this pool. If specified, the replicas in this replica
	// pool will be added to the specified target pools for load balancing
	// purposes. The replica pool must live in the same region as the
	// specified target pools. These values must be the target pool resource
	// names, and not fully qualified URLs.
	TargetPools []string `json:"targetPools,omitempty"`

	// Template: The template to use when creating replicas in this pool.
	// This template is used during initial instance creation of the pool,
	// when growing the pool in size, or when a replica restarts.
	Template *Template `json:"template,omitempty"`

	// Type: Deprecated! Do not set.
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AutoRestart") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AutoRestart") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Pool) MarshalJSON() ([]byte, error) {
	type noMethod Pool
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PoolsDeleteRequest struct {
	// AbandonInstances: If there are instances you would like to keep, you
	// can specify them here. These instances won't be deleted, but the
	// associated replica objects will be removed.
	AbandonInstances []string `json:"abandonInstances,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AbandonInstances") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AbandonInstances") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *PoolsDeleteRequest) MarshalJSON() ([]byte, error) {
	type noMethod PoolsDeleteRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PoolsListResponse struct {
	NextPageToken string `json:"nextPageToken,omitempty"`

	Resources []*Pool `json:"resources,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "NextPageToken") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NextPageToken") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PoolsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod PoolsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Replica: An individual Replica within a Pool. Replicas are
// automatically created by the replica pool, using the template
// provided by the user. You cannot directly create replicas.
type Replica struct {
	// Name: [Output Only] The name of the Replica object.
	Name string `json:"name,omitempty"`

	// SelfLink: [Output Only] The self-link of the Replica.
	SelfLink string `json:"selfLink,omitempty"`

	// Status: [Output Only] Last known status of the Replica.
	Status *ReplicaStatus `json:"status,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Name") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Name") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Replica) MarshalJSON() ([]byte, error) {
	type noMethod Replica
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReplicaStatus: The current status of a Replica.
type ReplicaStatus struct {
	// Details: [Output Only] Human-readable details about the current state
	// of the replica
	Details string `json:"details,omitempty"`

	// State: [Output Only] The state of the Replica.
	State string `json:"state,omitempty"`

	// TemplateVersion: [Output Only] The template used to build the
	// replica.
	TemplateVersion string `json:"templateVersion,omitempty"`

	// VmLink: [Output Only] Link to the virtual machine that this Replica
	// represents.
	VmLink string `json:"vmLink,omitempty"`

	// VmStartTime: [Output Only] The time that this Replica got to the
	// RUNNING state, in RFC 3339 format. If the start time is unknown,
	// UNKNOWN is returned.
	VmStartTime string `json:"vmStartTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Details") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Details") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReplicaStatus) MarshalJSON() ([]byte, error) {
	type noMethod ReplicaStatus
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ReplicasDeleteRequest struct {
	// AbandonInstance: Whether the instance resource represented by this
	// replica should be deleted or abandoned. If abandoned, the replica
	// will be deleted but the virtual machine instance will remain. By
	// default, this is set to false and the instance will be deleted along
	// with the replica.
	AbandonInstance bool `json:"abandonInstance,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AbandonInstance") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AbandonInstance") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ReplicasDeleteRequest) MarshalJSON() ([]byte, error) {
	type noMethod ReplicasDeleteRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ReplicasListResponse struct {
	NextPageToken string `json:"nextPageToken,omitempty"`

	Resources []*Replica `json:"resources,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "NextPageToken") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NextPageToken") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReplicasListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ReplicasListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ServiceAccount: A Compute Engine service account, identical to the
// Compute Engine resource.
type ServiceAccount struct {
	// Email: The service account email address, for example:
	// 123845678986@project.gserviceaccount.com
	Email string `json:"email,omitempty"`

	// Scopes: The list of OAuth2 scopes to obtain for the service account,
	// for example: https://www.googleapis.com/auth/devstorage.full_control
	Scopes []string `json:"scopes,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Email") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Email") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ServiceAccount) MarshalJSON() ([]byte, error) {
	type noMethod ServiceAccount
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Tag: A Compute Engine Instance tag, identical to the tags on the
// corresponding Compute Engine Instance resource.
type Tag struct {
	// FingerPrint: The fingerprint of the tag. Required for updating the
	// list of tags.
	FingerPrint string `json:"fingerPrint,omitempty"`

	// Items: Items contained in this tag.
	Items []string `json:"items,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FingerPrint") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FingerPrint") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Tag) MarshalJSON() ([]byte, error) {
	type noMethod Tag
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Template: The template used for creating replicas in the pool.
type Template struct {
	// Action: An action to run during initialization of your replicas. An
	// action is run as shell commands which are executed one after the
	// other in the same bash shell, so any state established by one command
	// is inherited by later commands.
	Action *Action `json:"action,omitempty"`

	// HealthChecks: A list of HTTP Health Checks to configure for this
	// replica pool and all virtual machines in this replica pool.
	HealthChecks []*HealthCheck `json:"healthChecks,omitempty"`

	// Version: A free-form string describing the version of this template.
	// You can provide any versioning string you would like. For example,
	// version1 or template-v1.
	Version string `json:"version,omitempty"`

	// VmParams: The virtual machine parameters to use for creating
	// replicas. You can define settings such as the machine type and the
	// image of replicas in this pool. This is required if replica type is
	// SMART_VM.
	VmParams *VmParams `json:"vmParams,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Action") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Action") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Template) MarshalJSON() ([]byte, error) {
	type noMethod Template
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VmParams: Parameters for creating a Compute Engine Instance resource.
// Most fields are identical to the corresponding Compute Engine
// resource.
type VmParams struct {
	// BaseInstanceName: Deprecated. Please use baseInstanceName instead.
	BaseInstanceName string `json:"baseInstanceName,omitempty"`

	// CanIpForward: Enables IP Forwarding, which allows this instance to
	// receive packets destined for a different IP address, and send packets
	// with a different source IP. See IP Forwarding for more information.
	CanIpForward bool `json:"canIpForward,omitempty"`

	// Description: An optional textual description of the instance.
	Description string `json:"description,omitempty"`

	// DisksToAttach: A list of existing Persistent Disk resources to attach
	// to each replica in the pool. Each disk will be attached in read-only
	// mode to every replica.
	DisksToAttach []*ExistingDisk `json:"disksToAttach,omitempty"`

	// DisksToCreate: A list of Disk resources to create and attach to each
	// Replica in the Pool. Currently, you can only define one disk and it
	// must be a root persistent disk. Note that Replica Pool will create a
	// root persistent disk for each replica.
	DisksToCreate []*NewDisk `json:"disksToCreate,omitempty"`

	// MachineType: The machine type for this instance. The resource name
	// (e.g. n1-standard-1).
	MachineType string `json:"machineType,omitempty"`

	// Metadata: The metadata key/value pairs assigned to this instance.
	Metadata *Metadata `json:"metadata,omitempty"`

	// NetworkInterfaces: A list of network interfaces for the instance.
	// Currently only one interface is supported by Google Compute Engine,
	// ONE_TO_ONE_NAT.
	NetworkInterfaces []*NetworkInterface `json:"networkInterfaces,omitempty"`

	OnHostMaintenance string `json:"onHostMaintenance,omitempty"`

	// ServiceAccounts: A list of Service Accounts to enable for this
	// instance.
	ServiceAccounts []*ServiceAccount `json:"serviceAccounts,omitempty"`

	// Tags: A list of tags to apply to the Google Compute Engine instance
	// to identify resources.
	Tags *Tag `json:"tags,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BaseInstanceName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BaseInstanceName") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VmParams) MarshalJSON() ([]byte, error) {
	type noMethod VmParams
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "replicapool.pools.delete":

type PoolsDeleteCall struct {
	s                  *Service
	projectName        string
	zone               string
	poolName           string
	poolsdeleterequest *PoolsDeleteRequest
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Delete: Deletes a replica pool.
func (r *PoolsService) Delete(projectName string, zone string, poolName string, poolsdeleterequest *PoolsDeleteRequest) *PoolsDeleteCall {
	c := &PoolsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	c.poolsdeleterequest = poolsdeleterequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PoolsDeleteCall) Fields(s ...googleapi.Field) *PoolsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PoolsDeleteCall) Context(ctx context.Context) *PoolsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PoolsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PoolsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.poolsdeleterequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.pools.delete" call.
func (c *PoolsDeleteCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Deletes a replica pool.",
	//   "httpMethod": "POST",
	//   "id": "replicapool.pools.delete",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName"
	//   ],
	//   "parameters": {
	//     "poolName": {
	//       "description": "The name of the replica pool for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}",
	//   "request": {
	//     "$ref": "PoolsDeleteRequest"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/replicapool"
	//   ]
	// }

}

// method id "replicapool.pools.get":

type PoolsGetCall struct {
	s            *Service
	projectName  string
	zone         string
	poolName     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a single replica pool.
func (r *PoolsService) Get(projectName string, zone string, poolName string) *PoolsGetCall {
	c := &PoolsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PoolsGetCall) Fields(s ...googleapi.Field) *PoolsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PoolsGetCall) IfNoneMatch(entityTag string) *PoolsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PoolsGetCall) Context(ctx context.Context) *PoolsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PoolsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PoolsGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.pools.get" call.
// Exactly one of *Pool or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Pool.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *PoolsGetCall) Do(opts ...googleapi.CallOption) (*Pool, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Pool{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Gets information about a single replica pool.",
	//   "httpMethod": "GET",
	//   "id": "replicapool.pools.get",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName"
	//   ],
	//   "parameters": {
	//     "poolName": {
	//       "description": "The name of the replica pool for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}",
	//   "response": {
	//     "$ref": "Pool"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/ndev.cloudman.readonly",
	//     "https://www.googleapis.com/auth/replicapool",
	//     "https://www.googleapis.com/auth/replicapool.readonly"
	//   ]
	// }

}

// method id "replicapool.pools.insert":

type PoolsInsertCall struct {
	s           *Service
	projectName string
	zone        string
	pool        *Pool
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Insert: Inserts a new replica pool.
func (r *PoolsService) Insert(projectName string, zone string, pool *Pool) *PoolsInsertCall {
	c := &PoolsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.pool = pool
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PoolsInsertCall) Fields(s ...googleapi.Field) *PoolsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PoolsInsertCall) Context(ctx context.Context) *PoolsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PoolsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PoolsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pool)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.pools.insert" call.
// Exactly one of *Pool or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Pool.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *PoolsInsertCall) Do(opts ...googleapi.CallOption) (*Pool, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Pool{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Inserts a new replica pool.",
	//   "httpMethod": "POST",
	//   "id": "replicapool.pools.insert",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone"
	//   ],
	//   "parameters": {
	//     "projectName": {
	//       "description": "The project ID for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools",
	//   "request": {
	//     "$ref": "Pool"
	//   },
	//   "response": {
	//     "$ref": "Pool"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/replicapool"
	//   ]
	// }

}

// method id "replicapool.pools.list":

type PoolsListCall struct {
	s            *Service
	projectName  string
	zone         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List all replica pools.
func (r *PoolsService) List(projectName string, zone string) *PoolsListCall {
	c := &PoolsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum count of
// results to be returned. Acceptable values are 0 to 100, inclusive.
// (Default: 50)
func (c *PoolsListCall) MaxResults(maxResults int64) *PoolsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Set this to the
// nextPageToken value returned by a previous list request to obtain the
// next page of results from the previous list request.
func (c *PoolsListCall) PageToken(pageToken string) *PoolsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PoolsListCall) Fields(s ...googleapi.Field) *PoolsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PoolsListCall) IfNoneMatch(entityTag string) *PoolsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PoolsListCall) Context(ctx context.Context) *PoolsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PoolsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PoolsListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.pools.list" call.
// Exactly one of *PoolsListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PoolsListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PoolsListCall) Do(opts ...googleapi.CallOption) (*PoolsListResponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &PoolsListResponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "List all replica pools.",
	//   "httpMethod": "GET",
	//   "id": "replicapool.pools.list",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "default": "500",
	//       "description": "Maximum count of results to be returned. Acceptable values are 0 to 100, inclusive. (Default: 50)",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "1000",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Set this to the nextPageToken value returned by a previous list request to obtain the next page of results from the previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools",
	//   "response": {
	//     "$ref": "PoolsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/ndev.cloudman.readonly",
	//     "https://www.googleapis.com/auth/replicapool",
	//     "https://www.googleapis.com/auth/replicapool.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *PoolsListCall) Pages(ctx context.Context, f func(*PoolsListResponse) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "replicapool.pools.resize":

type PoolsResizeCall struct {
	s           *Service
	projectName string
	zone        string
	poolName    string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Resize: Resize a pool. This is an asynchronous operation, and
// multiple overlapping resize requests can be made. Replica Pools will
// use the information from the last resize request.
func (r *PoolsService) Resize(projectName string, zone string, poolName string) *PoolsResizeCall {
	c := &PoolsResizeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	return c
}

// NumReplicas sets the optional parameter "numReplicas": The desired
// number of replicas to resize to. If this number is larger than the
// existing number of replicas, new replicas will be added. If the
// number is smaller, then existing replicas will be deleted.
func (c *PoolsResizeCall) NumReplicas(numReplicas int64) *PoolsResizeCall {
	c.urlParams_.Set("numReplicas", fmt.Sprint(numReplicas))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PoolsResizeCall) Fields(s ...googleapi.Field) *PoolsResizeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PoolsResizeCall) Context(ctx context.Context) *PoolsResizeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PoolsResizeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PoolsResizeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}/resize")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.pools.resize" call.
// Exactly one of *Pool or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Pool.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *PoolsResizeCall) Do(opts ...googleapi.CallOption) (*Pool, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Pool{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Resize a pool. This is an asynchronous operation, and multiple overlapping resize requests can be made. Replica Pools will use the information from the last resize request.",
	//   "httpMethod": "POST",
	//   "id": "replicapool.pools.resize",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName"
	//   ],
	//   "parameters": {
	//     "numReplicas": {
	//       "description": "The desired number of replicas to resize to. If this number is larger than the existing number of replicas, new replicas will be added. If the number is smaller, then existing replicas will be deleted.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "poolName": {
	//       "description": "The name of the replica pool for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}/resize",
	//   "response": {
	//     "$ref": "Pool"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/replicapool"
	//   ]
	// }

}

// method id "replicapool.pools.updatetemplate":

type PoolsUpdatetemplateCall struct {
	s           *Service
	projectName string
	zone        string
	poolName    string
	template    *Template
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Updatetemplate: Update the template used by the pool.
func (r *PoolsService) Updatetemplate(projectName string, zone string, poolName string, template *Template) *PoolsUpdatetemplateCall {
	c := &PoolsUpdatetemplateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	c.template = template
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PoolsUpdatetemplateCall) Fields(s ...googleapi.Field) *PoolsUpdatetemplateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PoolsUpdatetemplateCall) Context(ctx context.Context) *PoolsUpdatetemplateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PoolsUpdatetemplateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PoolsUpdatetemplateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.template)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}/updateTemplate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.pools.updatetemplate" call.
func (c *PoolsUpdatetemplateCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Update the template used by the pool.",
	//   "httpMethod": "POST",
	//   "id": "replicapool.pools.updatetemplate",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName"
	//   ],
	//   "parameters": {
	//     "poolName": {
	//       "description": "The name of the replica pool for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone for this replica pool.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}/updateTemplate",
	//   "request": {
	//     "$ref": "Template"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/replicapool"
	//   ]
	// }

}

// method id "replicapool.replicas.delete":

type ReplicasDeleteCall struct {
	s                     *Service
	projectName           string
	zone                  string
	poolName              string
	replicaName           string
	replicasdeleterequest *ReplicasDeleteRequest
	urlParams_            gensupport.URLParams
	ctx_                  context.Context
	header_               http.Header
}

// Delete: Deletes a replica from the pool.
func (r *ReplicasService) Delete(projectName string, zone string, poolName string, replicaName string, replicasdeleterequest *ReplicasDeleteRequest) *ReplicasDeleteCall {
	c := &ReplicasDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	c.replicaName = replicaName
	c.replicasdeleterequest = replicasdeleterequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReplicasDeleteCall) Fields(s ...googleapi.Field) *ReplicasDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReplicasDeleteCall) Context(ctx context.Context) *ReplicasDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReplicasDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReplicasDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.replicasdeleterequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}/replicas/{replicaName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
		"replicaName": c.replicaName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.replicas.delete" call.
// Exactly one of *Replica or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Replica.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ReplicasDeleteCall) Do(opts ...googleapi.CallOption) (*Replica, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Replica{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Deletes a replica from the pool.",
	//   "httpMethod": "POST",
	//   "id": "replicapool.replicas.delete",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName",
	//     "replicaName"
	//   ],
	//   "parameters": {
	//     "poolName": {
	//       "description": "The replica pool name for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "replicaName": {
	//       "description": "The name of the replica for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone where the replica lives.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}/replicas/{replicaName}",
	//   "request": {
	//     "$ref": "ReplicasDeleteRequest"
	//   },
	//   "response": {
	//     "$ref": "Replica"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/replicapool"
	//   ]
	// }

}

// method id "replicapool.replicas.get":

type ReplicasGetCall struct {
	s            *Service
	projectName  string
	zone         string
	poolName     string
	replicaName  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a specific replica.
func (r *ReplicasService) Get(projectName string, zone string, poolName string, replicaName string) *ReplicasGetCall {
	c := &ReplicasGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	c.replicaName = replicaName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReplicasGetCall) Fields(s ...googleapi.Field) *ReplicasGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ReplicasGetCall) IfNoneMatch(entityTag string) *ReplicasGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReplicasGetCall) Context(ctx context.Context) *ReplicasGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReplicasGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReplicasGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}/replicas/{replicaName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
		"replicaName": c.replicaName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.replicas.get" call.
// Exactly one of *Replica or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Replica.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ReplicasGetCall) Do(opts ...googleapi.CallOption) (*Replica, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Replica{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Gets information about a specific replica.",
	//   "httpMethod": "GET",
	//   "id": "replicapool.replicas.get",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName",
	//     "replicaName"
	//   ],
	//   "parameters": {
	//     "poolName": {
	//       "description": "The replica pool name for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "replicaName": {
	//       "description": "The name of the replica for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone where the replica lives.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}/replicas/{replicaName}",
	//   "response": {
	//     "$ref": "Replica"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/ndev.cloudman.readonly",
	//     "https://www.googleapis.com/auth/replicapool",
	//     "https://www.googleapis.com/auth/replicapool.readonly"
	//   ]
	// }

}

// method id "replicapool.replicas.list":

type ReplicasListCall struct {
	s            *Service
	projectName  string
	zone         string
	poolName     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists all replicas in a pool.
func (r *ReplicasService) List(projectName string, zone string, poolName string) *ReplicasListCall {
	c := &ReplicasListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum count of
// results to be returned. Acceptable values are 0 to 100, inclusive.
// (Default: 50)
func (c *ReplicasListCall) MaxResults(maxResults int64) *ReplicasListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Set this to the
// nextPageToken value returned by a previous list request to obtain the
// next page of results from the previous list request.
func (c *ReplicasListCall) PageToken(pageToken string) *ReplicasListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReplicasListCall) Fields(s ...googleapi.Field) *ReplicasListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ReplicasListCall) IfNoneMatch(entityTag string) *ReplicasListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReplicasListCall) Context(ctx context.Context) *ReplicasListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReplicasListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReplicasListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}/replicas")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.replicas.list" call.
// Exactly one of *ReplicasListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ReplicasListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ReplicasListCall) Do(opts ...googleapi.CallOption) (*ReplicasListResponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &ReplicasListResponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Lists all replicas in a pool.",
	//   "httpMethod": "GET",
	//   "id": "replicapool.replicas.list",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "default": "500",
	//       "description": "Maximum count of results to be returned. Acceptable values are 0 to 100, inclusive. (Default: 50)",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "1000",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Set this to the nextPageToken value returned by a previous list request to obtain the next page of results from the previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "poolName": {
	//       "description": "The replica pool name for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone where the replica pool lives.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}/replicas",
	//   "response": {
	//     "$ref": "ReplicasListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/ndev.cloudman.readonly",
	//     "https://www.googleapis.com/auth/replicapool",
	//     "https://www.googleapis.com/auth/replicapool.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ReplicasListCall) Pages(ctx context.Context, f func(*ReplicasListResponse) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "replicapool.replicas.restart":

type ReplicasRestartCall struct {
	s           *Service
	projectName string
	zone        string
	poolName    string
	replicaName string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Restart: Restarts a replica in a pool.
func (r *ReplicasService) Restart(projectName string, zone string, poolName string, replicaName string) *ReplicasRestartCall {
	c := &ReplicasRestartCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.zone = zone
	c.poolName = poolName
	c.replicaName = replicaName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReplicasRestartCall) Fields(s ...googleapi.Field) *ReplicasRestartCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReplicasRestartCall) Context(ctx context.Context) *ReplicasRestartCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReplicasRestartCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReplicasRestartCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{projectName}/zones/{zone}/pools/{poolName}/replicas/{replicaName}/restart")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
		"zone":        c.zone,
		"poolName":    c.poolName,
		"replicaName": c.replicaName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "replicapool.replicas.restart" call.
// Exactly one of *Replica or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Replica.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ReplicasRestartCall) Do(opts ...googleapi.CallOption) (*Replica, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Replica{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Restarts a replica in a pool.",
	//   "httpMethod": "POST",
	//   "id": "replicapool.replicas.restart",
	//   "parameterOrder": [
	//     "projectName",
	//     "zone",
	//     "poolName",
	//     "replicaName"
	//   ],
	//   "parameters": {
	//     "poolName": {
	//       "description": "The replica pool name for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "The project ID for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "replicaName": {
	//       "description": "The name of the replica for this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "zone": {
	//       "description": "The zone where the replica lives.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{projectName}/zones/{zone}/pools/{poolName}/replicas/{replicaName}/restart",
	//   "response": {
	//     "$ref": "Replica"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.cloudman",
	//     "https://www.googleapis.com/auth/replicapool"
	//   ]
	// }

}

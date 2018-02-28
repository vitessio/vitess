// Package appengine provides access to the Google App Engine Admin API.
//
// See https://cloud.google.com/appengine/docs/admin-api/
//
// Usage example:
//
//   import "google.golang.org/api/appengine/v1beta5"
//   ...
//   appengineService, err := appengine.New(oauthHttpClient)
package appengine // import "google.golang.org/api/appengine/v1beta5"

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

const apiId = "appengine:v1beta5"
const apiName = "appengine"
const apiVersion = "v1beta5"
const basePath = "https://appengine.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage your applications deployed on Google App Engine
	AppengineAdminScope = "https://www.googleapis.com/auth/appengine.admin"

	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// View your data across Google Cloud Platform services
	CloudPlatformReadOnlyScope = "https://www.googleapis.com/auth/cloud-platform.read-only"
)

func New(client *http.Client) (*APIService, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &APIService{client: client, BasePath: basePath}
	s.Apps = NewAppsService(s)
	return s, nil
}

type APIService struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Apps *AppsService
}

func (s *APIService) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewAppsService(s *APIService) *AppsService {
	rs := &AppsService{s: s}
	rs.Locations = NewAppsLocationsService(s)
	rs.Operations = NewAppsOperationsService(s)
	rs.Services = NewAppsServicesService(s)
	return rs
}

type AppsService struct {
	s *APIService

	Locations *AppsLocationsService

	Operations *AppsOperationsService

	Services *AppsServicesService
}

func NewAppsLocationsService(s *APIService) *AppsLocationsService {
	rs := &AppsLocationsService{s: s}
	return rs
}

type AppsLocationsService struct {
	s *APIService
}

func NewAppsOperationsService(s *APIService) *AppsOperationsService {
	rs := &AppsOperationsService{s: s}
	return rs
}

type AppsOperationsService struct {
	s *APIService
}

func NewAppsServicesService(s *APIService) *AppsServicesService {
	rs := &AppsServicesService{s: s}
	rs.Versions = NewAppsServicesVersionsService(s)
	return rs
}

type AppsServicesService struct {
	s *APIService

	Versions *AppsServicesVersionsService
}

func NewAppsServicesVersionsService(s *APIService) *AppsServicesVersionsService {
	rs := &AppsServicesVersionsService{s: s}
	rs.Instances = NewAppsServicesVersionsInstancesService(s)
	return rs
}

type AppsServicesVersionsService struct {
	s *APIService

	Instances *AppsServicesVersionsInstancesService
}

func NewAppsServicesVersionsInstancesService(s *APIService) *AppsServicesVersionsInstancesService {
	rs := &AppsServicesVersionsInstancesService{s: s}
	return rs
}

type AppsServicesVersionsInstancesService struct {
	s *APIService
}

// ApiConfigHandler: [Google Cloud
// Endpoints](https://cloud.google.com/appengine/docs/python/endpoints/)
// configuration for API handlers.
type ApiConfigHandler struct {
	// AuthFailAction: Action to take when users access resources that
	// require authentication. Defaults to `redirect`.
	//
	// Possible values:
	//   "AUTH_FAIL_ACTION_UNSPECIFIED"
	//   "AUTH_FAIL_ACTION_REDIRECT"
	//   "AUTH_FAIL_ACTION_UNAUTHORIZED"
	AuthFailAction string `json:"authFailAction,omitempty"`

	// Login: Level of login required to access this resource. Defaults to
	// `optional`.
	//
	// Possible values:
	//   "LOGIN_UNSPECIFIED"
	//   "LOGIN_OPTIONAL"
	//   "LOGIN_ADMIN"
	//   "LOGIN_REQUIRED"
	Login string `json:"login,omitempty"`

	// Script: Path to the script from the application root directory.
	Script string `json:"script,omitempty"`

	// SecurityLevel: Security (HTTPS) enforcement for this URL.
	//
	// Possible values:
	//   "SECURE_UNSPECIFIED"
	//   "SECURE_DEFAULT"
	//   "SECURE_NEVER"
	//   "SECURE_OPTIONAL"
	//   "SECURE_ALWAYS"
	SecurityLevel string `json:"securityLevel,omitempty"`

	// Url: URL to serve the endpoint at.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AuthFailAction") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuthFailAction") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ApiConfigHandler) MarshalJSON() ([]byte, error) {
	type noMethod ApiConfigHandler
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ApiEndpointHandler: Uses Google Cloud Endpoints to handle requests.
type ApiEndpointHandler struct {
	// ScriptPath: Path to the script from the application root directory.
	ScriptPath string `json:"scriptPath,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ScriptPath") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ScriptPath") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ApiEndpointHandler) MarshalJSON() ([]byte, error) {
	type noMethod ApiEndpointHandler
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Application: An Application resource contains the top-level
// configuration of an App Engine application.
type Application struct {
	// AuthDomain: Google Apps authentication domain that controls which
	// users can access this application. Defaults to open access for any
	// Google Account.
	AuthDomain string `json:"authDomain,omitempty"`

	// CodeBucket: A Google Cloud Storage bucket that can be used for
	// storing files associated with this application. This bucket is
	// associated with the application and can be used by the gcloud
	// deployment commands. @OutputOnly
	CodeBucket string `json:"codeBucket,omitempty"`

	// DefaultBucket: A Google Cloud Storage bucket that can be used by the
	// application to store content. @OutputOnly
	DefaultBucket string `json:"defaultBucket,omitempty"`

	// DefaultCookieExpiration: Cookie expiration policy for this
	// application. @OutputOnly
	DefaultCookieExpiration string `json:"defaultCookieExpiration,omitempty"`

	// DefaultHostname: Hostname used to reach the application, as resolved
	// by App Engine. @OutputOnly
	DefaultHostname string `json:"defaultHostname,omitempty"`

	// DispatchRules: HTTP path dispatch rules for requests to the
	// application that do not explicitly target a service or version. Rules
	// are order-dependent. @OutputOnly
	DispatchRules []*UrlDispatchRule `json:"dispatchRules,omitempty"`

	// Id: Identifier of the Application resource. This identifier is
	// equivalent to the project ID of the Google Cloud Platform project
	// where you want to deploy your application. Example: `myapp`.
	Id string `json:"id,omitempty"`

	// Location: Location from which this application will be run.
	// Application instances will run out of data centers in the chosen
	// location, which is also where all of the application's end user
	// content is stored. Defaults to `us-central`. Options are:
	// `us-central` - Central US `europe-west` - Western Europe `us-east1` -
	// Eastern US
	Location string `json:"location,omitempty"`

	// Name: Full path to the Application resource in the API. Example:
	// `apps/myapp`. @OutputOnly
	Name string `json:"name,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AuthDomain") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuthDomain") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Application) MarshalJSON() ([]byte, error) {
	type noMethod Application
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AutomaticScaling: Automatic scaling is based on request rate,
// response latencies, and other application metrics.
type AutomaticScaling struct {
	// CoolDownPeriod: Amount of time that the
	// [Autoscaler](https://cloud.google.com/compute/docs/autoscaler/)
	// should wait between changes to the number of virtual machines. Only
	// applicable for VM runtimes.
	CoolDownPeriod string `json:"coolDownPeriod,omitempty"`

	// CpuUtilization: Target scaling by CPU usage.
	CpuUtilization *CpuUtilization `json:"cpuUtilization,omitempty"`

	// DiskUtilization: Target scaling by disk usage.
	DiskUtilization *DiskUtilization `json:"diskUtilization,omitempty"`

	// MaxConcurrentRequests: Number of concurrent requests an automatic
	// scaling instance can accept before the scheduler spawns a new
	// instance. Defaults to a runtime-specific value.
	MaxConcurrentRequests int64 `json:"maxConcurrentRequests,omitempty"`

	// MaxIdleInstances: Maximum number of idle instances that should be
	// maintained for this version.
	MaxIdleInstances int64 `json:"maxIdleInstances,omitempty"`

	// MaxPendingLatency: Maximum amount of time that a request should wait
	// in the pending queue before starting a new instance to handle it.
	MaxPendingLatency string `json:"maxPendingLatency,omitempty"`

	// MaxTotalInstances: Maximum number of instances that should be started
	// to handle requests.
	MaxTotalInstances int64 `json:"maxTotalInstances,omitempty"`

	// MinIdleInstances: Minimum number of idle instances that should be
	// maintained for this version. Only applicable for the default version
	// of a module.
	MinIdleInstances int64 `json:"minIdleInstances,omitempty"`

	// MinPendingLatency: Minimum amount of time a request should wait in
	// the pending queue before starting a new instance to handle it.
	MinPendingLatency string `json:"minPendingLatency,omitempty"`

	// MinTotalInstances: Minimum number of instances that should be
	// maintained for this version.
	MinTotalInstances int64 `json:"minTotalInstances,omitempty"`

	// NetworkUtilization: Target scaling by network usage.
	NetworkUtilization *NetworkUtilization `json:"networkUtilization,omitempty"`

	// RequestUtilization: Target scaling by request utilization.
	RequestUtilization *RequestUtilization `json:"requestUtilization,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CoolDownPeriod") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CoolDownPeriod") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *AutomaticScaling) MarshalJSON() ([]byte, error) {
	type noMethod AutomaticScaling
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BasicScaling: A service with basic scaling will create an instance
// when the application receives a request. The instance will be turned
// down when the app becomes idle. Basic scaling is ideal for work that
// is intermittent or driven by user activity.
type BasicScaling struct {
	// IdleTimeout: Duration of time after the last request that an instance
	// must wait before the instance is shut down.
	IdleTimeout string `json:"idleTimeout,omitempty"`

	// MaxInstances: Maximum number of instances to create for this version.
	MaxInstances int64 `json:"maxInstances,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IdleTimeout") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IdleTimeout") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BasicScaling) MarshalJSON() ([]byte, error) {
	type noMethod BasicScaling
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ContainerInfo: Docker image that is used to start a VM container for
// the version you deploy.
type ContainerInfo struct {
	// Image: URI to the hosted container image in a Docker repository. The
	// URI must be fully qualified and include a tag or digest. Examples:
	// "gcr.io/my-project/image:tag" or "gcr.io/my-project/image@digest"
	Image string `json:"image,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Image") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Image") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ContainerInfo) MarshalJSON() ([]byte, error) {
	type noMethod ContainerInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CpuUtilization: Target scaling by CPU usage.
type CpuUtilization struct {
	// AggregationWindowLength: Period of time over which CPU utilization is
	// calculated.
	AggregationWindowLength string `json:"aggregationWindowLength,omitempty"`

	// TargetUtilization: Target CPU utilization ratio to maintain when
	// scaling. Must be between 0 and 1.
	TargetUtilization float64 `json:"targetUtilization,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AggregationWindowLength") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AggregationWindowLength")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CpuUtilization) MarshalJSON() ([]byte, error) {
	type noMethod CpuUtilization
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DebugInstanceRequest: Request message for `Instances.DebugInstance`.
type DebugInstanceRequest struct {
	// SshKey: Public SSH key to add to the instance. Example:
	// `[USERNAME]:ssh-rsa KEY_VALUE` or `[USERNAME]:ssh-rsa [KEY_VALUE]
	// google-ssh {"userName":"[USERNAME]","expireOn":"[EXPIRE_TIME]"}` For
	// more information, see [Adding and Removing SSH
	// Keys](https://cloud.google.com/compute/docs/instances/adding-removing-
	// ssh-keys)
	SshKey string `json:"sshKey,omitempty"`

	// ForceSendFields is a list of field names (e.g. "SshKey") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "SshKey") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DebugInstanceRequest) MarshalJSON() ([]byte, error) {
	type noMethod DebugInstanceRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Deployment: Code and application artifacts used to deploy a version
// to App Engine.
type Deployment struct {
	// Container: A Docker image that App Engine uses the run the version.
	// Only applicable for instances in App Engine flexible environment.
	Container *ContainerInfo `json:"container,omitempty"`

	// Files: Manifest of the files stored in Google Cloud Storage that are
	// included as part of this version. All files must be readable using
	// the credentials supplied with this call.
	Files map[string]FileInfo `json:"files,omitempty"`

	// SourceReferences: Origin of the source code for this deployment.
	// There can be more than one source reference per version if source
	// code is distributed among multiple repositories.
	SourceReferences []*SourceReference `json:"sourceReferences,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Container") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Container") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Deployment) MarshalJSON() ([]byte, error) {
	type noMethod Deployment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DiskUtilization: Target scaling by disk usage. Only applicable for VM
// runtimes.
type DiskUtilization struct {
	// TargetReadBytesPerSec: Target bytes read per second.
	TargetReadBytesPerSec int64 `json:"targetReadBytesPerSec,omitempty"`

	// TargetReadOpsPerSec: Target ops read per second.
	TargetReadOpsPerSec int64 `json:"targetReadOpsPerSec,omitempty"`

	// TargetWriteBytesPerSec: Target bytes written per second.
	TargetWriteBytesPerSec int64 `json:"targetWriteBytesPerSec,omitempty"`

	// TargetWriteOpsPerSec: Target ops written per second.
	TargetWriteOpsPerSec int64 `json:"targetWriteOpsPerSec,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "TargetReadBytesPerSec") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "TargetReadBytesPerSec") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DiskUtilization) MarshalJSON() ([]byte, error) {
	type noMethod DiskUtilization
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ErrorHandler: Custom static error page to be served when an error
// occurs.
type ErrorHandler struct {
	// ErrorCode: Error condition this handler applies to.
	//
	// Possible values:
	//   "ERROR_CODE_UNSPECIFIED"
	//   "ERROR_CODE_DEFAULT"
	//   "ERROR_CODE_OVER_QUOTA"
	//   "ERROR_CODE_DOS_API_DENIAL"
	//   "ERROR_CODE_TIMEOUT"
	ErrorCode string `json:"errorCode,omitempty"`

	// MimeType: MIME type of file. Defaults to `text/html`.
	MimeType string `json:"mimeType,omitempty"`

	// StaticFile: Static file content to be served for this error.
	StaticFile string `json:"staticFile,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ErrorCode") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ErrorCode") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ErrorHandler) MarshalJSON() ([]byte, error) {
	type noMethod ErrorHandler
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FileInfo: Single source file that is part of the version to be
// deployed. Each source file that is deployed must be specified
// separately.
type FileInfo struct {
	// MimeType: The MIME type of the file. Defaults to the value from
	// Google Cloud Storage.
	MimeType string `json:"mimeType,omitempty"`

	// Sha1Sum: The SHA1 hash of the file, in hex.
	Sha1Sum string `json:"sha1Sum,omitempty"`

	// SourceUrl: URL source to use to fetch this file. Must be a URL to a
	// resource in Google Cloud Storage in the form
	// 'http(s)://storage.googleapis.com/\/\'.
	SourceUrl string `json:"sourceUrl,omitempty"`

	// ForceSendFields is a list of field names (e.g. "MimeType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MimeType") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FileInfo) MarshalJSON() ([]byte, error) {
	type noMethod FileInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// HealthCheck: Health checking configuration for VM instances.
// Unhealthy instances are killed and replaced with new instances. Only
// applicable for instances in App Engine flexible environment.
type HealthCheck struct {
	// CheckInterval: Interval between health checks.
	CheckInterval string `json:"checkInterval,omitempty"`

	// DisableHealthCheck: Whether to explicitly disable health checks for
	// this instance.
	DisableHealthCheck bool `json:"disableHealthCheck,omitempty"`

	// HealthyThreshold: Number of consecutive successful health checks
	// required before receiving traffic.
	HealthyThreshold int64 `json:"healthyThreshold,omitempty"`

	// Host: Host header to send when performing an HTTP health check.
	// Example: "myapp.appspot.com"
	Host string `json:"host,omitempty"`

	// RestartThreshold: Number of consecutive failed health checks required
	// before an instance is restarted.
	RestartThreshold int64 `json:"restartThreshold,omitempty"`

	// Timeout: Time before the health check is considered failed.
	Timeout string `json:"timeout,omitempty"`

	// UnhealthyThreshold: Number of consecutive failed health checks
	// required before removing traffic.
	UnhealthyThreshold int64 `json:"unhealthyThreshold,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CheckInterval") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CheckInterval") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *HealthCheck) MarshalJSON() ([]byte, error) {
	type noMethod HealthCheck
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Instance: An Instance resource is the computing unit that App Engine
// uses to automatically scale an application.
type Instance struct {
	// AppEngineRelease: App Engine release this instance is running on.
	// @OutputOnly
	AppEngineRelease string `json:"appEngineRelease,omitempty"`

	// Availability: Availability of the instance. @OutputOnly
	//
	// Possible values:
	//   "UNSPECIFIED"
	//   "RESIDENT"
	//   "DYNAMIC"
	Availability string `json:"availability,omitempty"`

	// AverageLatency: Average latency (ms) over the last minute.
	// @OutputOnly
	AverageLatency int64 `json:"averageLatency,omitempty"`

	// Errors: Number of errors since this instance was started. @OutputOnly
	Errors int64 `json:"errors,omitempty"`

	// Id: Relative name of the instance within the version. Example:
	// `instance-1`. @OutputOnly
	Id string `json:"id,omitempty"`

	// MemoryUsage: Total memory in use (bytes). @OutputOnly
	MemoryUsage int64 `json:"memoryUsage,omitempty,string"`

	// Name: Full path to the Instance resource in the API. Example:
	// `apps/myapp/services/default/versions/v1/instances/instance-1`.
	// @OutputOnly
	Name string `json:"name,omitempty"`

	// Qps: Average queries per second (QPS) over the last minute.
	// @OutputOnly
	Qps float64 `json:"qps,omitempty"`

	// Requests: Number of requests since this instance was started.
	// @OutputOnly
	Requests int64 `json:"requests,omitempty"`

	// StartTimestamp: Time that this instance was started. @OutputOnly
	StartTimestamp string `json:"startTimestamp,omitempty"`

	// VmId: Virtual machine ID of this instance. Only applicable for
	// instances in App Engine flexible environment. @OutputOnly
	VmId string `json:"vmId,omitempty"`

	// VmIp: The IP address of this instance. Only applicable for instances
	// in App Engine flexible environment. @OutputOnly
	VmIp string `json:"vmIp,omitempty"`

	// VmName: Name of the virtual machine where this instance lives. Only
	// applicable for instances in App Engine flexible environment.
	// @OutputOnly
	VmName string `json:"vmName,omitempty"`

	// VmStatus: Status of the virtual machine where this instance lives.
	// Only applicable for instances in App Engine flexible environment.
	// @OutputOnly
	VmStatus string `json:"vmStatus,omitempty"`

	// VmUnlocked: Whether this instance is in debug mode. Only applicable
	// for instances in App Engine flexible environment. @OutputOnly
	VmUnlocked bool `json:"vmUnlocked,omitempty"`

	// VmZoneName: Zone where the virtual machine is located. Only
	// applicable for instances in App Engine flexible environment.
	// @OutputOnly
	VmZoneName string `json:"vmZoneName,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AppEngineRelease") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AppEngineRelease") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Instance) MarshalJSON() ([]byte, error) {
	type noMethod Instance
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Library: Third-party Python runtime library that is required by the
// application.
type Library struct {
	// Name: Name of the library. Example: "django".
	Name string `json:"name,omitempty"`

	// Version: Version of the library to select, or "latest".
	Version string `json:"version,omitempty"`

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

func (s *Library) MarshalJSON() ([]byte, error) {
	type noMethod Library
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListInstancesResponse: Response message for
// `Instances.ListInstances`.
type ListInstancesResponse struct {
	// Instances: The instances belonging to the requested version.
	Instances []*Instance `json:"instances,omitempty"`

	// NextPageToken: Continuation token for fetching the next page of
	// results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Instances") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Instances") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListInstancesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListInstancesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListLocationsResponse: The response message for
// LocationService.ListLocations.
type ListLocationsResponse struct {
	// Locations: A list of locations that matches the specified filter in
	// the request.
	Locations []*Location `json:"locations,omitempty"`

	// NextPageToken: The standard List next-page token.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Locations") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Locations") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListLocationsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListLocationsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListOperationsResponse: The response message for
// Operations.ListOperations.
type ListOperationsResponse struct {
	// NextPageToken: The standard List next-page token.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Operations: A list of operations that matches the specified filter in
	// the request.
	Operations []*Operation `json:"operations,omitempty"`

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

func (s *ListOperationsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListOperationsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListServicesResponse: Response message for `Services.ListServices`.
type ListServicesResponse struct {
	// NextPageToken: Continuation token for fetching the next page of
	// results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Services: The services belonging to the requested application.
	Services []*Service `json:"services,omitempty"`

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

func (s *ListServicesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListServicesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListVersionsResponse: Response message for `Versions.ListVersions`.
type ListVersionsResponse struct {
	// NextPageToken: Continuation token for fetching the next page of
	// results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Versions: The versions belonging to the requested service.
	Versions []*Version `json:"versions,omitempty"`

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

func (s *ListVersionsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListVersionsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Location: A resource that represents Google Cloud Platform location.
type Location struct {
	// Labels: Cross-service attributes for the location. For example
	// {"cloud.googleapis.com/region": "us-east1"}
	Labels map[string]string `json:"labels,omitempty"`

	// LocationId: The canonical id for this location. For example:
	// "us-east1".
	LocationId string `json:"locationId,omitempty"`

	// Metadata: Service-specific metadata. For example the available
	// capacity at the given location.
	Metadata googleapi.RawMessage `json:"metadata,omitempty"`

	// Name: Resource name for the location, which may vary between
	// implementations. For example:
	// "projects/example-project/locations/us-east1"
	Name string `json:"name,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Labels") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Labels") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Location) MarshalJSON() ([]byte, error) {
	type noMethod Location
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LocationMetadata: Metadata for the given
// google.cloud.location.Location.
type LocationMetadata struct {
	// FlexibleEnvironmentAvailable: App Engine Flexible Environment is
	// available in the given location. @OutputOnly
	FlexibleEnvironmentAvailable bool `json:"flexibleEnvironmentAvailable,omitempty"`

	// StandardEnvironmentAvailable: App Engine Standard Environment is
	// available in the given location. @OutputOnly
	StandardEnvironmentAvailable bool `json:"standardEnvironmentAvailable,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "FlexibleEnvironmentAvailable") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "FlexibleEnvironmentAvailable") to include in API requests with the
	// JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LocationMetadata) MarshalJSON() ([]byte, error) {
	type noMethod LocationMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ManualScaling: A service with manual scaling runs continuously,
// allowing you to perform complex initialization and rely on the state
// of its memory over time.
type ManualScaling struct {
	// Instances: Number of instances to assign to the service at the start.
	// This number can later be altered by using the [Modules
	// API](https://cloud.google.com/appengine/docs/python/modules/functions)
	//  `set_num_instances()` function.
	Instances int64 `json:"instances,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Instances") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Instances") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ManualScaling) MarshalJSON() ([]byte, error) {
	type noMethod ManualScaling
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Network: Extra network settings. Only applicable for VM runtimes.
type Network struct {
	// ForwardedPorts: List of ports, or port pairs, to forward from the
	// virtual machine to the application container.
	ForwardedPorts []string `json:"forwardedPorts,omitempty"`

	// InstanceTag: Tag to apply to the VM instance during creation.
	InstanceTag string `json:"instanceTag,omitempty"`

	// Name: Google Cloud Platform network where the virtual machines are
	// created. Specify the short name, not the resource path. Defaults to
	// `default`.
	Name string `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ForwardedPorts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ForwardedPorts") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Network) MarshalJSON() ([]byte, error) {
	type noMethod Network
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// NetworkUtilization: Target scaling by network usage. Only applicable
// for VM runtimes.
type NetworkUtilization struct {
	// TargetReceivedBytesPerSec: Target bytes received per second.
	TargetReceivedBytesPerSec int64 `json:"targetReceivedBytesPerSec,omitempty"`

	// TargetReceivedPacketsPerSec: Target packets received per second.
	TargetReceivedPacketsPerSec int64 `json:"targetReceivedPacketsPerSec,omitempty"`

	// TargetSentBytesPerSec: Target bytes sent per second.
	TargetSentBytesPerSec int64 `json:"targetSentBytesPerSec,omitempty"`

	// TargetSentPacketsPerSec: Target packets sent per second.
	TargetSentPacketsPerSec int64 `json:"targetSentPacketsPerSec,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "TargetReceivedBytesPerSec") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "TargetReceivedBytesPerSec") to include in API requests with the JSON
	// null value. By default, fields with empty values are omitted from API
	// requests. However, any field with an empty value appearing in
	// NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NetworkUtilization) MarshalJSON() ([]byte, error) {
	type noMethod NetworkUtilization
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Operation: This resource represents a long-running operation that is
// the result of a network API call.
type Operation struct {
	// Done: If the value is `false`, it means the operation is still in
	// progress. If true, the operation is completed, and either `error` or
	// `response` is available.
	Done bool `json:"done,omitempty"`

	// Error: The error result of the operation in case of failure or
	// cancellation.
	Error *Status `json:"error,omitempty"`

	// Metadata: Service-specific metadata associated with the operation. It
	// typically contains progress information and common metadata such as
	// create time. Some services might not provide such metadata. Any
	// method that returns a long-running operation should document the
	// metadata type, if any.
	Metadata googleapi.RawMessage `json:"metadata,omitempty"`

	// Name: The server-assigned name, which is only unique within the same
	// service that originally returns it. If you use the default HTTP
	// mapping, the `name` should have the format of
	// `operations/some/unique/name`.
	Name string `json:"name,omitempty"`

	// Response: The normal response of the operation in case of success. If
	// the original method returns no data on success, such as `Delete`, the
	// response is `google.protobuf.Empty`. If the original method is
	// standard `Get`/`Create`/`Update`, the response should be the
	// resource. For other methods, the response should have the type
	// `XxxResponse`, where `Xxx` is the original method name. For example,
	// if the original method name is `TakeSnapshot()`, the inferred
	// response type is `TakeSnapshotResponse`.
	Response googleapi.RawMessage `json:"response,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Done") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Done") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Operation) MarshalJSON() ([]byte, error) {
	type noMethod Operation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OperationMetadata: Metadata for the given
// google.longrunning.Operation.
type OperationMetadata struct {
	// EndTime: Timestamp that this operation completed. @OutputOnly
	EndTime string `json:"endTime,omitempty"`

	// InsertTime: Timestamp that this operation was created. @OutputOnly
	InsertTime string `json:"insertTime,omitempty"`

	// Method: API method that initiated this operation. Example:
	// `google.appengine.v1beta4.Version.CreateVersion`. @OutputOnly
	Method string `json:"method,omitempty"`

	// OperationType: Type of this operation. Deprecated, use method field
	// instead. Example: "create_version". @OutputOnly
	OperationType string `json:"operationType,omitempty"`

	// Target: Name of the resource that this operation is acting on.
	// Example: `apps/myapp/modules/default`. @OutputOnly
	Target string `json:"target,omitempty"`

	// User: User who requested this operation. @OutputOnly
	User string `json:"user,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EndTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OperationMetadata) MarshalJSON() ([]byte, error) {
	type noMethod OperationMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OperationMetadataExperimental: Metadata for the given
// google.longrunning.Operation.
type OperationMetadataExperimental struct {
	// EndTime: Time that this operation completed. @OutputOnly
	EndTime string `json:"endTime,omitempty"`

	// InsertTime: Time that this operation was created. @OutputOnly
	InsertTime string `json:"insertTime,omitempty"`

	// Method: API method that initiated this operation. Example:
	// `google.appengine.experimental.CustomDomains.CreateCustomDomain`.
	// @OutputOnly
	Method string `json:"method,omitempty"`

	// Target: Name of the resource that this operation is acting on.
	// Example: `apps/myapp/customDomains/example.com`. @OutputOnly
	Target string `json:"target,omitempty"`

	// User: User who requested this operation. @OutputOnly
	User string `json:"user,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EndTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OperationMetadataExperimental) MarshalJSON() ([]byte, error) {
	type noMethod OperationMetadataExperimental
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OperationMetadataV1: Metadata for the given
// google.longrunning.Operation.
type OperationMetadataV1 struct {
	// EndTime: Time that this operation completed. @OutputOnly
	EndTime string `json:"endTime,omitempty"`

	// InsertTime: Time that this operation was created. @OutputOnly
	InsertTime string `json:"insertTime,omitempty"`

	// Method: API method that initiated this operation. Example:
	// `google.appengine.v1.Versions.CreateVersion`. @OutputOnly
	Method string `json:"method,omitempty"`

	// Target: Name of the resource that this operation is acting on.
	// Example: `apps/myapp/services/default`. @OutputOnly
	Target string `json:"target,omitempty"`

	// User: User who requested this operation. @OutputOnly
	User string `json:"user,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EndTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OperationMetadataV1) MarshalJSON() ([]byte, error) {
	type noMethod OperationMetadataV1
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OperationMetadataV1Beta5: Metadata for the given
// google.longrunning.Operation.
type OperationMetadataV1Beta5 struct {
	// EndTime: Timestamp that this operation completed. @OutputOnly
	EndTime string `json:"endTime,omitempty"`

	// InsertTime: Timestamp that this operation was created. @OutputOnly
	InsertTime string `json:"insertTime,omitempty"`

	// Method: API method name that initiated this operation. Example:
	// `google.appengine.v1beta5.Version.CreateVersion`. @OutputOnly
	Method string `json:"method,omitempty"`

	// Target: Name of the resource that this operation is acting on.
	// Example: `apps/myapp/services/default`. @OutputOnly
	Target string `json:"target,omitempty"`

	// User: User who requested this operation. @OutputOnly
	User string `json:"user,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EndTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OperationMetadataV1Beta5) MarshalJSON() ([]byte, error) {
	type noMethod OperationMetadataV1Beta5
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RequestUtilization: Target scaling by request utilization. Only
// applicable for VM runtimes.
type RequestUtilization struct {
	// TargetConcurrentRequests: Target number of concurrent requests.
	TargetConcurrentRequests int64 `json:"targetConcurrentRequests,omitempty"`

	// TargetRequestCountPerSec: Target requests per second.
	TargetRequestCountPerSec int64 `json:"targetRequestCountPerSec,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "TargetConcurrentRequests") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "TargetConcurrentRequests")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *RequestUtilization) MarshalJSON() ([]byte, error) {
	type noMethod RequestUtilization
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Resources: Machine resources for a version.
type Resources struct {
	// Cpu: Number of CPU cores needed.
	Cpu float64 `json:"cpu,omitempty"`

	// DiskGb: Disk size (GB) needed.
	DiskGb float64 `json:"diskGb,omitempty"`

	// MemoryGb: Memory (GB) needed.
	MemoryGb float64 `json:"memoryGb,omitempty"`

	// Volumes: Volumes mounted within the app container.
	Volumes []*Volume `json:"volumes,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Cpu") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Cpu") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Resources) MarshalJSON() ([]byte, error) {
	type noMethod Resources
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ScriptHandler: Executes a script to handle the request that matches
// the URL pattern.
type ScriptHandler struct {
	// ScriptPath: Path to the script from the application root directory.
	ScriptPath string `json:"scriptPath,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ScriptPath") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ScriptPath") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ScriptHandler) MarshalJSON() ([]byte, error) {
	type noMethod ScriptHandler
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Service: A Service resource is a logical component of an application
// that can share state and communicate in a secure fashion with other
// services. For example, an application that handles customer requests
// might include separate services to handle other tasks such as API
// requests from mobile devices or backend data analysis. Each service
// has a collection of versions that define a specific set of code used
// to implement the functionality of that service.
type Service struct {
	// Id: Relative name of the service within the application. Example:
	// `default`. @OutputOnly
	Id string `json:"id,omitempty"`

	// Name: Full path to the Service resource in the API. Example:
	// `apps/myapp/services/default`. @OutputOnly
	Name string `json:"name,omitempty"`

	// Split: Mapping that defines fractional HTTP traffic diversion to
	// different versions within the service.
	Split *TrafficSplit `json:"split,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Id") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Id") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Service) MarshalJSON() ([]byte, error) {
	type noMethod Service
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceReference: Reference to a particular snapshot of the source
// tree used to build and deploy the application.
type SourceReference struct {
	// Repository: URI string identifying the repository. Example:
	// "https://source.developers.google.com/p/app-123/r/default"
	Repository string `json:"repository,omitempty"`

	// RevisionId: The canonical, persistent identifier of the deployed
	// revision. Aliases that include tags or branch names are not allowed.
	// Example (git): "2198322f89e0bb2e25021667c2ed489d1fd34e6b"
	RevisionId string `json:"revisionId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Repository") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Repository") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceReference) MarshalJSON() ([]byte, error) {
	type noMethod SourceReference
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StaticFilesHandler: Files served directly to the user for a given
// URL, such as images, CSS stylesheets, or JavaScript source files.
// Static file handlers describe which files in the application
// directory are static files, and which URLs serve them.
type StaticFilesHandler struct {
	// ApplicationReadable: Whether files should also be uploaded as code
	// data. By default, files declared in static file handlers are uploaded
	// as static data and are only served to end users; they cannot be read
	// by the application. If enabled, uploads are charged against both your
	// code and static data storage resource quotas.
	ApplicationReadable bool `json:"applicationReadable,omitempty"`

	// Expiration: Time a static file served by this handler should be
	// cached.
	Expiration string `json:"expiration,omitempty"`

	// HttpHeaders: HTTP headers to use for all responses from these URLs.
	HttpHeaders map[string]string `json:"httpHeaders,omitempty"`

	// MimeType: MIME type used to serve all files served by this handler.
	// Defaults to file-specific MIME types, which are derived from each
	// file's filename extension.
	MimeType string `json:"mimeType,omitempty"`

	// Path: Path to the static files matched by the URL pattern, from the
	// application root directory. The path can refer to text matched in
	// groupings in the URL pattern.
	Path string `json:"path,omitempty"`

	// RequireMatchingFile: Whether this handler should match the request if
	// the file referenced by the handler does not exist.
	RequireMatchingFile bool `json:"requireMatchingFile,omitempty"`

	// UploadPathRegex: Regular expression that matches the file paths for
	// all files that should be referenced by this handler.
	UploadPathRegex string `json:"uploadPathRegex,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ApplicationReadable")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ApplicationReadable") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *StaticFilesHandler) MarshalJSON() ([]byte, error) {
	type noMethod StaticFilesHandler
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Status: The `Status` type defines a logical error model that is
// suitable for different programming environments, including REST APIs
// and RPC APIs. It is used by [gRPC](https://github.com/grpc). The
// error model is designed to be: - Simple to use and understand for
// most users - Flexible enough to meet unexpected needs # Overview The
// `Status` message contains three pieces of data: error code, error
// message, and error details. The error code should be an enum value of
// google.rpc.Code, but it may accept additional error codes if needed.
// The error message should be a developer-facing English message that
// helps developers *understand* and *resolve* the error. If a localized
// user-facing error message is needed, put the localized message in the
// error details or localize it in the client. The optional error
// details may contain arbitrary information about the error. There is a
// predefined set of error detail types in the package `google.rpc`
// which can be used for common error conditions. # Language mapping The
// `Status` message is the logical representation of the error model,
// but it is not necessarily the actual wire format. When the `Status`
// message is exposed in different client libraries and different wire
// protocols, it can be mapped differently. For example, it will likely
// be mapped to some exceptions in Java, but more likely mapped to some
// error codes in C. # Other uses The error model and the `Status`
// message can be used in a variety of environments, either with or
// without APIs, to provide a consistent developer experience across
// different environments. Example uses of this error model include: -
// Partial errors. If a service needs to return partial errors to the
// client, it may embed the `Status` in the normal response to indicate
// the partial errors. - Workflow errors. A typical workflow has
// multiple steps. Each step may have a `Status` message for error
// reporting purpose. - Batch operations. If a client uses batch request
// and batch response, the `Status` message should be used directly
// inside batch response, one for each error sub-response. -
// Asynchronous operations. If an API call embeds asynchronous operation
// results in its response, the status of those operations should be
// represented directly using the `Status` message. - Logging. If some
// API errors are stored in logs, the message `Status` could be used
// directly after any stripping needed for security/privacy reasons.
type Status struct {
	// Code: The status code, which should be an enum value of
	// google.rpc.Code.
	Code int64 `json:"code,omitempty"`

	// Details: A list of messages that carry the error details. There will
	// be a common set of message types for APIs to use.
	Details []googleapi.RawMessage `json:"details,omitempty"`

	// Message: A developer-facing error message, which should be in
	// English. Any user-facing error message should be localized and sent
	// in the google.rpc.Status.details field, or localized by the client.
	Message string `json:"message,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Code") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Code") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Status) MarshalJSON() ([]byte, error) {
	type noMethod Status
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TrafficSplit: Traffic routing configuration for versions within a
// single service. Traffic splits define how traffic directed to the
// service is assigned to versions.
type TrafficSplit struct {
	// Allocations: Mapping from version IDs within the service to
	// fractional (0.000, 1] allocations of traffic for that version. Each
	// version can be specified only once, but some versions in the service
	// may not have any traffic allocation. Services that have traffic
	// allocated cannot be deleted until either the service is deleted or
	// their traffic allocation is removed. Allocations must sum to 1. Up to
	// two decimal place precision is supported for IP-based splits and up
	// to three decimal places is supported for cookie-based splits.
	Allocations map[string]float64 `json:"allocations,omitempty"`

	// ShardBy: Mechanism used to determine which version a request is sent
	// to. The traffic selection algorithm will be stable for either type
	// until allocations are changed.
	//
	// Possible values:
	//   "UNSPECIFIED"
	//   "COOKIE"
	//   "IP"
	ShardBy string `json:"shardBy,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Allocations") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Allocations") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TrafficSplit) MarshalJSON() ([]byte, error) {
	type noMethod TrafficSplit
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UrlDispatchRule: Rules to match an HTTP request and dispatch that
// request to a service.
type UrlDispatchRule struct {
	// Domain: Domain name to match against. The wildcard "*" is supported
	// if specified before a period: "*.". Defaults to matching all
	// domains: "*".
	Domain string `json:"domain,omitempty"`

	// Path: Pathname within the host. Must start with a "/". A single
	// "*" can be included at the end of the path. The sum of the lengths
	// of the domain and path may not exceed 100 characters.
	Path string `json:"path,omitempty"`

	// Service: Resource id of a service in this application that should
	// serve the matched request. The service must already exist. Example:
	// `default`.
	Service string `json:"service,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Domain") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Domain") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UrlDispatchRule) MarshalJSON() ([]byte, error) {
	type noMethod UrlDispatchRule
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UrlMap: URL pattern and description of how the URL should be handled.
// App Engine can handle URLs by executing application code, or by
// serving static files uploaded with the version, such as images, CSS,
// or JavaScript.
type UrlMap struct {
	// ApiEndpoint: Uses API Endpoints to handle requests.
	ApiEndpoint *ApiEndpointHandler `json:"apiEndpoint,omitempty"`

	// AuthFailAction: Action to take when users access resources that
	// require authentication. Defaults to `redirect`.
	//
	// Possible values:
	//   "AUTH_FAIL_ACTION_UNSPECIFIED"
	//   "AUTH_FAIL_ACTION_REDIRECT"
	//   "AUTH_FAIL_ACTION_UNAUTHORIZED"
	AuthFailAction string `json:"authFailAction,omitempty"`

	// Login: Level of login required to access this resource.
	//
	// Possible values:
	//   "LOGIN_UNSPECIFIED"
	//   "LOGIN_OPTIONAL"
	//   "LOGIN_ADMIN"
	//   "LOGIN_REQUIRED"
	Login string `json:"login,omitempty"`

	// RedirectHttpResponseCode: `30x` code to use when performing redirects
	// for the `secure` field. Defaults to `302`.
	//
	// Possible values:
	//   "REDIRECT_HTTP_RESPONSE_CODE_UNSPECIFIED"
	//   "REDIRECT_HTTP_RESPONSE_CODE_301"
	//   "REDIRECT_HTTP_RESPONSE_CODE_302"
	//   "REDIRECT_HTTP_RESPONSE_CODE_303"
	//   "REDIRECT_HTTP_RESPONSE_CODE_307"
	RedirectHttpResponseCode string `json:"redirectHttpResponseCode,omitempty"`

	// Script: Executes a script to handle the request that matches this URL
	// pattern.
	Script *ScriptHandler `json:"script,omitempty"`

	// SecurityLevel: Security (HTTPS) enforcement for this URL.
	//
	// Possible values:
	//   "SECURE_UNSPECIFIED"
	//   "SECURE_DEFAULT"
	//   "SECURE_NEVER"
	//   "SECURE_OPTIONAL"
	//   "SECURE_ALWAYS"
	SecurityLevel string `json:"securityLevel,omitempty"`

	// StaticFiles: Returns the contents of a file, such as an image, as the
	// response.
	StaticFiles *StaticFilesHandler `json:"staticFiles,omitempty"`

	// UrlRegex: A URL prefix. Uses regular expression syntax, which means
	// regexp special characters must be escaped, but should not contain
	// groupings. All URLs that begin with this prefix are handled by this
	// handler, using the portion of the URL after the prefix as part of the
	// file path.
	UrlRegex string `json:"urlRegex,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ApiEndpoint") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ApiEndpoint") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UrlMap) MarshalJSON() ([]byte, error) {
	type noMethod UrlMap
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Version: A Version resource is a specific set of source code and
// configuration files that are deployed into a service.
type Version struct {
	// ApiConfig: Serving configuration for [Google Cloud
	// Endpoints](https://cloud.google.com/appengine/docs/python/endpoints/).
	//  Only returned in `GET` requests if `view=FULL` is set.
	ApiConfig *ApiConfigHandler `json:"apiConfig,omitempty"`

	// AutomaticScaling: Automatic scaling is based on request rate,
	// response latencies, and other application metrics.
	AutomaticScaling *AutomaticScaling `json:"automaticScaling,omitempty"`

	// BasicScaling: A service with basic scaling will create an instance
	// when the application receives a request. The instance will be turned
	// down when the app becomes idle. Basic scaling is ideal for work that
	// is intermittent or driven by user activity.
	BasicScaling *BasicScaling `json:"basicScaling,omitempty"`

	// BetaSettings: Metadata settings that are supplied to this version to
	// enable beta runtime features.
	BetaSettings map[string]string `json:"betaSettings,omitempty"`

	// CreationTime: Time that this version was created. @OutputOnly
	CreationTime string `json:"creationTime,omitempty"`

	// DefaultExpiration: Duration that static files should be cached by web
	// proxies and browsers. Only applicable if the corresponding
	// [StaticFilesHandler](https://cloud.google.com/appengine/docs/admin-api
	// /reference/rest/v1/apps.services.versions#staticfileshandler) does
	// not specify its own expiration time. Only returned in `GET` requests
	// if `view=FULL` is set.
	DefaultExpiration string `json:"defaultExpiration,omitempty"`

	// Deployer: Email address of the user who created this version.
	// @OutputOnly
	Deployer string `json:"deployer,omitempty"`

	// Deployment: Code and application artifacts that make up this version.
	// Only returned in `GET` requests if `view=FULL` is set.
	Deployment *Deployment `json:"deployment,omitempty"`

	// DiskUsageBytes: Total size of version files hosted on App Engine disk
	// in bytes. @OutputOnly
	DiskUsageBytes int64 `json:"diskUsageBytes,omitempty,string"`

	// Env: App Engine execution environment to use for this version.
	// Defaults to `1`.
	Env string `json:"env,omitempty"`

	// EnvVariables: Environment variables made available to the
	// application. Only returned in `GET` requests if `view=FULL` is set.
	EnvVariables map[string]string `json:"envVariables,omitempty"`

	// ErrorHandlers: Custom static error pages. Limited to 10KB per page.
	// Only returned in `GET` requests if `view=FULL` is set.
	ErrorHandlers []*ErrorHandler `json:"errorHandlers,omitempty"`

	// Handlers: An ordered list of URL-matching patterns that should be
	// applied to incoming requests. The first matching URL handles the
	// request and other request handlers are not attempted. Only returned
	// in `GET` requests if `view=FULL` is set.
	Handlers []*UrlMap `json:"handlers,omitempty"`

	// HealthCheck: Configures health checking for VM instances. Unhealthy
	// instances are be stopped and replaced with new instances. Only
	// applicable for VM runtimes. Only returned in `GET` requests if
	// `view=FULL` is set.
	HealthCheck *HealthCheck `json:"healthCheck,omitempty"`

	// Id: Relative name of the version within the module. Example: `v1`.
	// Version names can contain only lowercase letters, numbers, or
	// hyphens. Reserved names: "default", "latest", and any name with the
	// prefix "ah-".
	Id string `json:"id,omitempty"`

	// InboundServices: Before an application can receive email or XMPP
	// messages, the application must be configured to enable the service.
	//
	// Possible values:
	//   "INBOUND_SERVICE_UNSPECIFIED" - Not specified.
	//   "INBOUND_SERVICE_MAIL" - Allows an application to receive mail.
	//   "INBOUND_SERVICE_MAIL_BOUNCE" - Allows an application to receive
	// email-bound notifications.
	//   "INBOUND_SERVICE_XMPP_ERROR" - Allows an application to receive
	// error stanzas.
	//   "INBOUND_SERVICE_XMPP_MESSAGE" - Allows an application to receive
	// instant messages.
	//   "INBOUND_SERVICE_XMPP_SUBSCRIBE" - Allows an application to receive
	// user subscription POSTs.
	//   "INBOUND_SERVICE_XMPP_PRESENCE" - Allows an application to receive
	// a user's chat presence.
	//   "INBOUND_SERVICE_CHANNEL_PRESENCE" - Registers an application for
	// notifications when a client connects or disconnects from a channel.
	//   "INBOUND_SERVICE_WARMUP" - Enables warmup requests.
	InboundServices []string `json:"inboundServices,omitempty"`

	// InstanceClass: Instance class that is used to run this version. Valid
	// values are: * AutomaticScaling: `F1`, `F2`, `F4`, `F4_1G` *
	// ManualScaling or BasicScaling: `B1`, `B2`, `B4`, `B8`, `B4_1G`
	// Defaults to `F1` for AutomaticScaling and `B1` for ManualScaling or
	// BasicScaling.
	InstanceClass string `json:"instanceClass,omitempty"`

	// Libraries: Configuration for third-party Python runtime libraries
	// required by the application. Only returned in `GET` requests if
	// `view=FULL` is set.
	Libraries []*Library `json:"libraries,omitempty"`

	// ManualScaling: A service with manual scaling runs continuously,
	// allowing you to perform complex initialization and rely on the state
	// of its memory over time.
	ManualScaling *ManualScaling `json:"manualScaling,omitempty"`

	// Name: Full path to the Version resource in the API. Example:
	// `apps/myapp/services/default/versions/v1`. @OutputOnly
	Name string `json:"name,omitempty"`

	// Network: Extra network settings. Only applicable for VM runtimes.
	Network *Network `json:"network,omitempty"`

	// NobuildFilesRegex: Files that match this pattern will not be built
	// into this version. Only applicable for Go runtimes. Only returned in
	// `GET` requests if `view=FULL` is set.
	NobuildFilesRegex string `json:"nobuildFilesRegex,omitempty"`

	// Resources: Machine resources for this version. Only applicable for VM
	// runtimes.
	Resources *Resources `json:"resources,omitempty"`

	// Runtime: Desired runtime. Example: `python27`.
	Runtime string `json:"runtime,omitempty"`

	// ServingStatus: Current serving status of this version. Only the
	// versions with a `SERVING` status create instances and can be billed.
	// `SERVING_STATUS_UNSPECIFIED` is an invalid value. Defaults to
	// `SERVING`.
	//
	// Possible values:
	//   "SERVING_STATUS_UNSPECIFIED"
	//   "SERVING"
	//   "STOPPED"
	ServingStatus string `json:"servingStatus,omitempty"`

	// Threadsafe: Whether multiple requests can be dispatched to this
	// version at once.
	Threadsafe bool `json:"threadsafe,omitempty"`

	// Vm: Whether to deploy this version in a container on a virtual
	// machine.
	Vm bool `json:"vm,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ApiConfig") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ApiConfig") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Version) MarshalJSON() ([]byte, error) {
	type noMethod Version
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Volume: Volumes mounted within the app container. Only applicable for
// VM runtimes.
type Volume struct {
	// Name: Unique name for the volume.
	Name string `json:"name,omitempty"`

	// SizeGb: Volume size in GB.
	SizeGb float64 `json:"sizeGb,omitempty"`

	// VolumeType: Underlying volume type, e.g. 'tmpfs'.
	VolumeType string `json:"volumeType,omitempty"`

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

func (s *Volume) MarshalJSON() ([]byte, error) {
	type noMethod Volume
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "appengine.apps.create":

type AppsCreateCall struct {
	s           *APIService
	application *Application
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Create: Creates an App Engine application for a Google Cloud Platform
// project. This requires a project that excludes an App Engine
// application. For details about creating a project without an
// application, see the [Google Cloud Resource Manager create project
// topic](https://cloud.google.com/resource-manager/docs/creating-project
// ).
func (r *AppsService) Create(application *Application) *AppsCreateCall {
	c := &AppsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.application = application
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsCreateCall) Fields(s ...googleapi.Field) *AppsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsCreateCall) Context(ctx context.Context) *AppsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.application)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.create" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsCreateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Creates an App Engine application for a Google Cloud Platform project. This requires a project that excludes an App Engine application. For details about creating a project without an application, see the [Google Cloud Resource Manager create project topic](https://cloud.google.com/resource-manager/docs/creating-project).",
	//   "httpMethod": "POST",
	//   "id": "appengine.apps.create",
	//   "path": "v1beta5/apps",
	//   "request": {
	//     "$ref": "Application"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.get":

type AppsGetCall struct {
	s            *APIService
	appsId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about an application.
func (r *AppsService) Get(appsId string) *AppsGetCall {
	c := &AppsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	return c
}

// EnsureResourcesExist sets the optional parameter
// "ensureResourcesExist": Certain resources associated with an
// application are created on-demand. Controls whether these resources
// should be created when performing the `GET` operation. If specified
// and any resources could not be created, the request will fail with an
// error code. Additionally, this parameter can cause the request to
// take longer to complete. Note: This parameter will be deprecated in a
// future version of the API.
func (c *AppsGetCall) EnsureResourcesExist(ensureResourcesExist bool) *AppsGetCall {
	c.urlParams_.Set("ensureResourcesExist", fmt.Sprint(ensureResourcesExist))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsGetCall) Fields(s ...googleapi.Field) *AppsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsGetCall) IfNoneMatch(entityTag string) *AppsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsGetCall) Context(ctx context.Context) *AppsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId": c.appsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.get" call.
// Exactly one of *Application or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Application.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsGetCall) Do(opts ...googleapi.CallOption) (*Application, error) {
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
	ret := &Application{
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
	//   "description": "Gets information about an application.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.get",
	//   "parameterOrder": [
	//     "appsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the application to get. Example: `apps/myapp`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "ensureResourcesExist": {
	//       "description": "Certain resources associated with an application are created on-demand. Controls whether these resources should be created when performing the `GET` operation. If specified and any resources could not be created, the request will fail with an error code. Additionally, this parameter can cause the request to take longer to complete. Note: This parameter will be deprecated in a future version of the API.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}",
	//   "response": {
	//     "$ref": "Application"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.patch":

type AppsPatchCall struct {
	s           *APIService
	appsId      string
	application *Application
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Patch: Updates the specified Application resource. You can update the
// following fields: *
// [`auth_domain`](https://cloud.google.com/appengine/docs/admin-api/refe
// rence/rest/v1beta5/apps#Application.FIELDS.auth_domain) *
// [`default_cookie_expiration`](https://cloud.google.com/appengine/docs/
// admin-api/reference/rest/v1beta5/apps#Application.FIELDS.default_cooki
// e_expiration)
func (r *AppsService) Patch(appsId string, application *Application) *AppsPatchCall {
	c := &AppsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.application = application
	return c
}

// Mask sets the optional parameter "mask": Standard field mask for the
// set of fields to be updated.
func (c *AppsPatchCall) Mask(mask string) *AppsPatchCall {
	c.urlParams_.Set("mask", mask)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsPatchCall) Fields(s ...googleapi.Field) *AppsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsPatchCall) Context(ctx context.Context) *AppsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.application)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId": c.appsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.patch" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsPatchCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Updates the specified Application resource. You can update the following fields: * [`auth_domain`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps#Application.FIELDS.auth_domain) * [`default_cookie_expiration`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps#Application.FIELDS.default_cookie_expiration)",
	//   "httpMethod": "PATCH",
	//   "id": "appengine.apps.patch",
	//   "parameterOrder": [
	//     "appsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the Application resource to update. Example: `apps/myapp`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "mask": {
	//       "description": "Standard field mask for the set of fields to be updated.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}",
	//   "request": {
	//     "$ref": "Application"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.locations.get":

type AppsLocationsGetCall struct {
	s            *APIService
	appsId       string
	locationsId  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Get information about a location.
func (r *AppsLocationsService) Get(appsId string, locationsId string) *AppsLocationsGetCall {
	c := &AppsLocationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.locationsId = locationsId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsLocationsGetCall) Fields(s ...googleapi.Field) *AppsLocationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsLocationsGetCall) IfNoneMatch(entityTag string) *AppsLocationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsLocationsGetCall) Context(ctx context.Context) *AppsLocationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsLocationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsLocationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/locations/{locationsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":      c.appsId,
		"locationsId": c.locationsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.locations.get" call.
// Exactly one of *Location or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Location.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsLocationsGetCall) Do(opts ...googleapi.CallOption) (*Location, error) {
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
	ret := &Location{
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
	//   "description": "Get information about a location.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.locations.get",
	//   "parameterOrder": [
	//     "appsId",
	//     "locationsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Resource name for the location.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "locationsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/locations/{locationsId}",
	//   "response": {
	//     "$ref": "Location"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// method id "appengine.apps.locations.list":

type AppsLocationsListCall struct {
	s            *APIService
	appsId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists information about the supported locations for this
// service.
func (r *AppsLocationsService) List(appsId string) *AppsLocationsListCall {
	c := &AppsLocationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	return c
}

// Filter sets the optional parameter "filter": The standard list
// filter.
func (c *AppsLocationsListCall) Filter(filter string) *AppsLocationsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": The standard list
// page size.
func (c *AppsLocationsListCall) PageSize(pageSize int64) *AppsLocationsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The standard list
// page token.
func (c *AppsLocationsListCall) PageToken(pageToken string) *AppsLocationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsLocationsListCall) Fields(s ...googleapi.Field) *AppsLocationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsLocationsListCall) IfNoneMatch(entityTag string) *AppsLocationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsLocationsListCall) Context(ctx context.Context) *AppsLocationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsLocationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsLocationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/locations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId": c.appsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.locations.list" call.
// Exactly one of *ListLocationsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListLocationsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *AppsLocationsListCall) Do(opts ...googleapi.CallOption) (*ListLocationsResponse, error) {
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
	ret := &ListLocationsResponse{
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
	//   "description": "Lists information about the supported locations for this service.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.locations.list",
	//   "parameterOrder": [
	//     "appsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. The resource that owns the locations collection, if applicable.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "filter": {
	//       "description": "The standard list filter.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "The standard list page size.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The standard list page token.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/locations",
	//   "response": {
	//     "$ref": "ListLocationsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AppsLocationsListCall) Pages(ctx context.Context, f func(*ListLocationsResponse) error) error {
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

// method id "appengine.apps.operations.get":

type AppsOperationsGetCall struct {
	s            *APIService
	appsId       string
	operationsId string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the latest state of a long-running operation. Clients can
// use this method to poll the operation result at intervals as
// recommended by the API service.
func (r *AppsOperationsService) Get(appsId string, operationsId string) *AppsOperationsGetCall {
	c := &AppsOperationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.operationsId = operationsId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsOperationsGetCall) Fields(s ...googleapi.Field) *AppsOperationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsOperationsGetCall) IfNoneMatch(entityTag string) *AppsOperationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsOperationsGetCall) Context(ctx context.Context) *AppsOperationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsOperationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsOperationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/operations/{operationsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":       c.appsId,
		"operationsId": c.operationsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.operations.get" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsOperationsGetCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Gets the latest state of a long-running operation. Clients can use this method to poll the operation result at intervals as recommended by the API service.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.operations.get",
	//   "parameterOrder": [
	//     "appsId",
	//     "operationsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. The name of the operation resource.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "operationsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/operations/{operationsId}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// method id "appengine.apps.operations.list":

type AppsOperationsListCall struct {
	s            *APIService
	appsId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists operations that match the specified filter in the
// request. If the server doesn't support this method, it returns
// `UNIMPLEMENTED`. NOTE: the `name` binding below allows API services
// to override the binding to use different resource name schemes, such
// as `users/*/operations`.
func (r *AppsOperationsService) List(appsId string) *AppsOperationsListCall {
	c := &AppsOperationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	return c
}

// Filter sets the optional parameter "filter": The standard list
// filter.
func (c *AppsOperationsListCall) Filter(filter string) *AppsOperationsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": The standard list
// page size.
func (c *AppsOperationsListCall) PageSize(pageSize int64) *AppsOperationsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The standard list
// page token.
func (c *AppsOperationsListCall) PageToken(pageToken string) *AppsOperationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsOperationsListCall) Fields(s ...googleapi.Field) *AppsOperationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsOperationsListCall) IfNoneMatch(entityTag string) *AppsOperationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsOperationsListCall) Context(ctx context.Context) *AppsOperationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsOperationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsOperationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/operations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId": c.appsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.operations.list" call.
// Exactly one of *ListOperationsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListOperationsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *AppsOperationsListCall) Do(opts ...googleapi.CallOption) (*ListOperationsResponse, error) {
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
	ret := &ListOperationsResponse{
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
	//   "description": "Lists operations that match the specified filter in the request. If the server doesn't support this method, it returns `UNIMPLEMENTED`. NOTE: the `name` binding below allows API services to override the binding to use different resource name schemes, such as `users/*/operations`.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.operations.list",
	//   "parameterOrder": [
	//     "appsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. The name of the operation collection.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "filter": {
	//       "description": "The standard list filter.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "The standard list page size.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The standard list page token.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/operations",
	//   "response": {
	//     "$ref": "ListOperationsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AppsOperationsListCall) Pages(ctx context.Context, f func(*ListOperationsResponse) error) error {
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

// method id "appengine.apps.services.delete":

type AppsServicesDeleteCall struct {
	s          *APIService
	appsId     string
	servicesId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes the specified service and all enclosed versions.
func (r *AppsServicesService) Delete(appsId string, servicesId string) *AppsServicesDeleteCall {
	c := &AppsServicesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesDeleteCall) Fields(s ...googleapi.Field) *AppsServicesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesDeleteCall) Context(ctx context.Context) *AppsServicesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.delete" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesDeleteCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Deletes the specified service and all enclosed versions.",
	//   "httpMethod": "DELETE",
	//   "id": "appengine.apps.services.delete",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.get":

type AppsServicesGetCall struct {
	s            *APIService
	appsId       string
	servicesId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the current configuration of the specified service.
func (r *AppsServicesService) Get(appsId string, servicesId string) *AppsServicesGetCall {
	c := &AppsServicesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesGetCall) Fields(s ...googleapi.Field) *AppsServicesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsServicesGetCall) IfNoneMatch(entityTag string) *AppsServicesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesGetCall) Context(ctx context.Context) *AppsServicesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.get" call.
// Exactly one of *Service or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Service.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AppsServicesGetCall) Do(opts ...googleapi.CallOption) (*Service, error) {
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
	ret := &Service{
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
	//   "description": "Gets the current configuration of the specified service.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.services.get",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}",
	//   "response": {
	//     "$ref": "Service"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// method id "appengine.apps.services.list":

type AppsServicesListCall struct {
	s            *APIService
	appsId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists all the services in the application.
func (r *AppsServicesService) List(appsId string) *AppsServicesListCall {
	c := &AppsServicesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum results to
// return per page.
func (c *AppsServicesListCall) PageSize(pageSize int64) *AppsServicesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// for fetching the next page of results.
func (c *AppsServicesListCall) PageToken(pageToken string) *AppsServicesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesListCall) Fields(s ...googleapi.Field) *AppsServicesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsServicesListCall) IfNoneMatch(entityTag string) *AppsServicesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesListCall) Context(ctx context.Context) *AppsServicesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId": c.appsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.list" call.
// Exactly one of *ListServicesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListServicesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *AppsServicesListCall) Do(opts ...googleapi.CallOption) (*ListServicesResponse, error) {
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
	ret := &ListServicesResponse{
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
	//   "description": "Lists all the services in the application.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.services.list",
	//   "parameterOrder": [
	//     "appsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum results to return per page.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token for fetching the next page of results.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services",
	//   "response": {
	//     "$ref": "ListServicesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AppsServicesListCall) Pages(ctx context.Context, f func(*ListServicesResponse) error) error {
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

// method id "appengine.apps.services.patch":

type AppsServicesPatchCall struct {
	s          *APIService
	appsId     string
	servicesId string
	service    *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates the configuration of the specified service.
func (r *AppsServicesService) Patch(appsId string, servicesId string, service *Service) *AppsServicesPatchCall {
	c := &AppsServicesPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.service = service
	return c
}

// Mask sets the optional parameter "mask": Standard field mask for the
// set of fields to be updated.
func (c *AppsServicesPatchCall) Mask(mask string) *AppsServicesPatchCall {
	c.urlParams_.Set("mask", mask)
	return c
}

// MigrateTraffic sets the optional parameter "migrateTraffic": Set to
// `true` to gradually shift traffic from one version to another single
// version. By default, traffic is shifted immediately. For gradual
// traffic migration, the target version must be located within
// instances that are configured for both [warmup
// requests](https://cloud.google.com/appengine/docs/admin-api/reference/
// rest/v1beta5/apps.services.versions#inboundservicetype) and
// [automatic
// scaling](https://cloud.google.com/appengine/docs/admin-api/reference/r
// est/v1beta5/apps.services.versions#automaticscaling). You must
// specify the
// [`shardBy`](https://cloud.google.com/appengine/docs/admin-api/referenc
// e/rest/v1beta5/apps.services#shardby) field in the Service resource.
// Gradual traffic migration is not supported in the App Engine flexible
// environment. For examples, see [Migrating and Splitting
// Traffic](https://cloud.google.com/appengine/docs/admin-api/migrating-s
// plitting-traffic).
func (c *AppsServicesPatchCall) MigrateTraffic(migrateTraffic bool) *AppsServicesPatchCall {
	c.urlParams_.Set("migrateTraffic", fmt.Sprint(migrateTraffic))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesPatchCall) Fields(s ...googleapi.Field) *AppsServicesPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesPatchCall) Context(ctx context.Context) *AppsServicesPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.service)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.patch" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesPatchCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Updates the configuration of the specified service.",
	//   "httpMethod": "PATCH",
	//   "id": "appengine.apps.services.patch",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource to update. Example: `apps/myapp/services/default`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "mask": {
	//       "description": "Standard field mask for the set of fields to be updated.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "migrateTraffic": {
	//       "description": "Set to `true` to gradually shift traffic from one version to another single version. By default, traffic is shifted immediately. For gradual traffic migration, the target version must be located within instances that are configured for both [warmup requests](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#inboundservicetype) and [automatic scaling](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#automaticscaling). You must specify the [`shardBy`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services#shardby) field in the Service resource. Gradual traffic migration is not supported in the App Engine flexible environment. For examples, see [Migrating and Splitting Traffic](https://cloud.google.com/appengine/docs/admin-api/migrating-splitting-traffic).",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}",
	//   "request": {
	//     "$ref": "Service"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.create":

type AppsServicesVersionsCreateCall struct {
	s          *APIService
	appsId     string
	servicesId string
	version    *Version
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Deploys new code and resource files to a new version.
func (r *AppsServicesVersionsService) Create(appsId string, servicesId string, version *Version) *AppsServicesVersionsCreateCall {
	c := &AppsServicesVersionsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.version = version
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsCreateCall) Fields(s ...googleapi.Field) *AppsServicesVersionsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsCreateCall) Context(ctx context.Context) *AppsServicesVersionsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.version)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.create" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesVersionsCreateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Deploys new code and resource files to a new version.",
	//   "httpMethod": "POST",
	//   "id": "appengine.apps.services.versions.create",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource to update. For example: \"apps/myapp/services/default\".",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions",
	//   "request": {
	//     "$ref": "Version"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.delete":

type AppsServicesVersionsDeleteCall struct {
	s          *APIService
	appsId     string
	servicesId string
	versionsId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an existing version.
func (r *AppsServicesVersionsService) Delete(appsId string, servicesId string, versionsId string) *AppsServicesVersionsDeleteCall {
	c := &AppsServicesVersionsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsDeleteCall) Fields(s ...googleapi.Field) *AppsServicesVersionsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsDeleteCall) Context(ctx context.Context) *AppsServicesVersionsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
		"versionsId": c.versionsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.delete" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesVersionsDeleteCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Deletes an existing version.",
	//   "httpMethod": "DELETE",
	//   "id": "appengine.apps.services.versions.delete",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default/versions/v1`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.get":

type AppsServicesVersionsGetCall struct {
	s            *APIService
	appsId       string
	servicesId   string
	versionsId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified Version resource. By default, only a
// `BASIC_VIEW` will be returned. Specify the `FULL_VIEW` parameter to
// get the full resource.
func (r *AppsServicesVersionsService) Get(appsId string, servicesId string, versionsId string) *AppsServicesVersionsGetCall {
	c := &AppsServicesVersionsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	return c
}

// View sets the optional parameter "view": Controls the set of fields
// returned in the `Get` response.
//
// Possible values:
//   "BASIC"
//   "FULL"
func (c *AppsServicesVersionsGetCall) View(view string) *AppsServicesVersionsGetCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsGetCall) Fields(s ...googleapi.Field) *AppsServicesVersionsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsServicesVersionsGetCall) IfNoneMatch(entityTag string) *AppsServicesVersionsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsGetCall) Context(ctx context.Context) *AppsServicesVersionsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
		"versionsId": c.versionsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.get" call.
// Exactly one of *Version or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Version.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AppsServicesVersionsGetCall) Do(opts ...googleapi.CallOption) (*Version, error) {
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
	ret := &Version{
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
	//   "description": "Gets the specified Version resource. By default, only a `BASIC_VIEW` will be returned. Specify the `FULL_VIEW` parameter to get the full resource.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.services.versions.get",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default/versions/v1`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Controls the set of fields returned in the `Get` response.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}",
	//   "response": {
	//     "$ref": "Version"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.list":

type AppsServicesVersionsListCall struct {
	s            *APIService
	appsId       string
	servicesId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the versions of a service.
func (r *AppsServicesVersionsService) List(appsId string, servicesId string) *AppsServicesVersionsListCall {
	c := &AppsServicesVersionsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum results to
// return per page.
func (c *AppsServicesVersionsListCall) PageSize(pageSize int64) *AppsServicesVersionsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// for fetching the next page of results.
func (c *AppsServicesVersionsListCall) PageToken(pageToken string) *AppsServicesVersionsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// View sets the optional parameter "view": Controls the set of fields
// returned in the `List` response.
//
// Possible values:
//   "BASIC"
//   "FULL"
func (c *AppsServicesVersionsListCall) View(view string) *AppsServicesVersionsListCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsListCall) Fields(s ...googleapi.Field) *AppsServicesVersionsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsServicesVersionsListCall) IfNoneMatch(entityTag string) *AppsServicesVersionsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsListCall) Context(ctx context.Context) *AppsServicesVersionsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.list" call.
// Exactly one of *ListVersionsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListVersionsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *AppsServicesVersionsListCall) Do(opts ...googleapi.CallOption) (*ListVersionsResponse, error) {
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
	ret := &ListVersionsResponse{
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
	//   "description": "Lists the versions of a service.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.services.versions.list",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum results to return per page.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token for fetching the next page of results.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Controls the set of fields returned in the `List` response.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions",
	//   "response": {
	//     "$ref": "ListVersionsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AppsServicesVersionsListCall) Pages(ctx context.Context, f func(*ListVersionsResponse) error) error {
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

// method id "appengine.apps.services.versions.patch":

type AppsServicesVersionsPatchCall struct {
	s          *APIService
	appsId     string
	servicesId string
	versionsId string
	version    *Version
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates the specified Version resource. You can specify the
// following fields depending on the App Engine environment and type of
// scaling that the version resource uses: *
// [`serving_status`](https://cloud.google.com/appengine/docs/admin-api/r
// eference/rest/v1beta5/apps.services.versions#Version.FIELDS.serving_st
// atus): For Version resources that use basic scaling, manual scaling,
// or run in the App Engine flexible environment. *
// [`instance_class`](https://cloud.google.com/appengine/docs/admin-api/r
// eference/rest/v1beta5/apps.services.versions#Version.FIELDS.instance_c
// lass): For Version resources that run in the App Engine standard
// environment. *
// [`automatic_scaling.min_idle_instances`](https://cloud.google.com/appe
// ngine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#Ver
// sion.FIELDS.automatic_scaling): For Version resources that use
// automatic scaling and run in the App Engine standard environment. *
// [`automatic_scaling.max_idle_instances`](https://cloud.google.com/appe
// ngine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#Ver
// sion.FIELDS.automatic_scaling): For Version resources that use
// automatic scaling and run in the App Engine standard environment.
func (r *AppsServicesVersionsService) Patch(appsId string, servicesId string, versionsId string, version *Version) *AppsServicesVersionsPatchCall {
	c := &AppsServicesVersionsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	c.version = version
	return c
}

// Mask sets the optional parameter "mask": Standard field mask for the
// set of fields to be updated.
func (c *AppsServicesVersionsPatchCall) Mask(mask string) *AppsServicesVersionsPatchCall {
	c.urlParams_.Set("mask", mask)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsPatchCall) Fields(s ...googleapi.Field) *AppsServicesVersionsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsPatchCall) Context(ctx context.Context) *AppsServicesVersionsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.version)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
		"versionsId": c.versionsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.patch" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesVersionsPatchCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Updates the specified Version resource. You can specify the following fields depending on the App Engine environment and type of scaling that the version resource uses: * [`serving_status`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#Version.FIELDS.serving_status): For Version resources that use basic scaling, manual scaling, or run in the App Engine flexible environment. * [`instance_class`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#Version.FIELDS.instance_class): For Version resources that run in the App Engine standard environment. * [`automatic_scaling.min_idle_instances`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#Version.FIELDS.automatic_scaling): For Version resources that use automatic scaling and run in the App Engine standard environment. * [`automatic_scaling.max_idle_instances`](https://cloud.google.com/appengine/docs/admin-api/reference/rest/v1beta5/apps.services.versions#Version.FIELDS.automatic_scaling): For Version resources that use automatic scaling and run in the App Engine standard environment.",
	//   "httpMethod": "PATCH",
	//   "id": "appengine.apps.services.versions.patch",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource to update. Example: `apps/myapp/services/default/versions/1`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "mask": {
	//       "description": "Standard field mask for the set of fields to be updated.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}",
	//   "request": {
	//     "$ref": "Version"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.instances.debug":

type AppsServicesVersionsInstancesDebugCall struct {
	s                    *APIService
	appsId               string
	servicesId           string
	versionsId           string
	instancesId          string
	debuginstancerequest *DebugInstanceRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Debug: Enables debugging on a VM instance. This allows you to use the
// SSH command to connect to the virtual machine where the instance
// lives. While in "debug mode", the instance continues to serve live
// traffic. You should delete the instance when you are done debugging
// and then allow the system to take over and determine if another
// instance should be started. Only applicable for instances in App
// Engine flexible environment.
func (r *AppsServicesVersionsInstancesService) Debug(appsId string, servicesId string, versionsId string, instancesId string, debuginstancerequest *DebugInstanceRequest) *AppsServicesVersionsInstancesDebugCall {
	c := &AppsServicesVersionsInstancesDebugCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	c.instancesId = instancesId
	c.debuginstancerequest = debuginstancerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsInstancesDebugCall) Fields(s ...googleapi.Field) *AppsServicesVersionsInstancesDebugCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsInstancesDebugCall) Context(ctx context.Context) *AppsServicesVersionsInstancesDebugCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsInstancesDebugCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsInstancesDebugCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.debuginstancerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances/{instancesId}:debug")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":      c.appsId,
		"servicesId":  c.servicesId,
		"versionsId":  c.versionsId,
		"instancesId": c.instancesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.instances.debug" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesVersionsInstancesDebugCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Enables debugging on a VM instance. This allows you to use the SSH command to connect to the virtual machine where the instance lives. While in \"debug mode\", the instance continues to serve live traffic. You should delete the instance when you are done debugging and then allow the system to take over and determine if another instance should be started. Only applicable for instances in App Engine flexible environment.",
	//   "httpMethod": "POST",
	//   "id": "appengine.apps.services.versions.instances.debug",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId",
	//     "instancesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default/versions/v1/instances/instance-1`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "instancesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances/{instancesId}:debug",
	//   "request": {
	//     "$ref": "DebugInstanceRequest"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.instances.delete":

type AppsServicesVersionsInstancesDeleteCall struct {
	s           *APIService
	appsId      string
	servicesId  string
	versionsId  string
	instancesId string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Delete: Stops a running instance.
func (r *AppsServicesVersionsInstancesService) Delete(appsId string, servicesId string, versionsId string, instancesId string) *AppsServicesVersionsInstancesDeleteCall {
	c := &AppsServicesVersionsInstancesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	c.instancesId = instancesId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsInstancesDeleteCall) Fields(s ...googleapi.Field) *AppsServicesVersionsInstancesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsInstancesDeleteCall) Context(ctx context.Context) *AppsServicesVersionsInstancesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsInstancesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsInstancesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances/{instancesId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":      c.appsId,
		"servicesId":  c.servicesId,
		"versionsId":  c.versionsId,
		"instancesId": c.instancesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.instances.delete" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesVersionsInstancesDeleteCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Stops a running instance.",
	//   "httpMethod": "DELETE",
	//   "id": "appengine.apps.services.versions.instances.delete",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId",
	//     "instancesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. For example: \"apps/myapp/services/default/versions/v1/instances/instance-1\".",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "instancesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances/{instancesId}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.instances.get":

type AppsServicesVersionsInstancesGetCall struct {
	s            *APIService
	appsId       string
	servicesId   string
	versionsId   string
	instancesId  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets instance information.
func (r *AppsServicesVersionsInstancesService) Get(appsId string, servicesId string, versionsId string, instancesId string) *AppsServicesVersionsInstancesGetCall {
	c := &AppsServicesVersionsInstancesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	c.instancesId = instancesId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsInstancesGetCall) Fields(s ...googleapi.Field) *AppsServicesVersionsInstancesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsServicesVersionsInstancesGetCall) IfNoneMatch(entityTag string) *AppsServicesVersionsInstancesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsInstancesGetCall) Context(ctx context.Context) *AppsServicesVersionsInstancesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsInstancesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsInstancesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances/{instancesId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":      c.appsId,
		"servicesId":  c.servicesId,
		"versionsId":  c.versionsId,
		"instancesId": c.instancesId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.instances.get" call.
// Exactly one of *Instance or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Instance.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AppsServicesVersionsInstancesGetCall) Do(opts ...googleapi.CallOption) (*Instance, error) {
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
	ret := &Instance{
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
	//   "description": "Gets instance information.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.services.versions.instances.get",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId",
	//     "instancesId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default/versions/v1/instances/instance-1`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "instancesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances/{instancesId}",
	//   "response": {
	//     "$ref": "Instance"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// method id "appengine.apps.services.versions.instances.list":

type AppsServicesVersionsInstancesListCall struct {
	s            *APIService
	appsId       string
	servicesId   string
	versionsId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the instances of a version.
func (r *AppsServicesVersionsInstancesService) List(appsId string, servicesId string, versionsId string) *AppsServicesVersionsInstancesListCall {
	c := &AppsServicesVersionsInstancesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.appsId = appsId
	c.servicesId = servicesId
	c.versionsId = versionsId
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum results to
// return per page.
func (c *AppsServicesVersionsInstancesListCall) PageSize(pageSize int64) *AppsServicesVersionsInstancesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// for fetching the next page of results.
func (c *AppsServicesVersionsInstancesListCall) PageToken(pageToken string) *AppsServicesVersionsInstancesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AppsServicesVersionsInstancesListCall) Fields(s ...googleapi.Field) *AppsServicesVersionsInstancesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AppsServicesVersionsInstancesListCall) IfNoneMatch(entityTag string) *AppsServicesVersionsInstancesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AppsServicesVersionsInstancesListCall) Context(ctx context.Context) *AppsServicesVersionsInstancesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AppsServicesVersionsInstancesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AppsServicesVersionsInstancesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"appsId":     c.appsId,
		"servicesId": c.servicesId,
		"versionsId": c.versionsId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "appengine.apps.services.versions.instances.list" call.
// Exactly one of *ListInstancesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListInstancesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *AppsServicesVersionsInstancesListCall) Do(opts ...googleapi.CallOption) (*ListInstancesResponse, error) {
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
	ret := &ListInstancesResponse{
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
	//   "description": "Lists the instances of a version.",
	//   "httpMethod": "GET",
	//   "id": "appengine.apps.services.versions.instances.list",
	//   "parameterOrder": [
	//     "appsId",
	//     "servicesId",
	//     "versionsId"
	//   ],
	//   "parameters": {
	//     "appsId": {
	//       "description": "Part of `name`. Name of the resource requested. Example: `apps/myapp/services/default/versions/v1`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum results to return per page.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token for fetching the next page of results.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "servicesId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "versionsId": {
	//       "description": "Part of `name`. See documentation of `appsId`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta5/apps/{appsId}/services/{servicesId}/versions/{versionsId}/instances",
	//   "response": {
	//     "$ref": "ListInstancesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/appengine.admin",
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AppsServicesVersionsInstancesListCall) Pages(ctx context.Context, f func(*ListInstancesResponse) error) error {
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

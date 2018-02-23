// Package runtimeconfig provides access to the Google Cloud RuntimeConfig API.
//
// See https://cloud.google.com/deployment-manager/runtime-configurator/
//
// Usage example:
//
//   import "google.golang.org/api/runtimeconfig/v1beta1"
//   ...
//   runtimeconfigService, err := runtimeconfig.New(oauthHttpClient)
package runtimeconfig // import "google.golang.org/api/runtimeconfig/v1beta1"

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

const apiId = "runtimeconfig:v1beta1"
const apiName = "runtimeconfig"
const apiVersion = "v1beta1"
const basePath = "https://runtimeconfig.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// Manage your Google Cloud Platform services' runtime configuration
	CloudruntimeconfigScope = "https://www.googleapis.com/auth/cloudruntimeconfig"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Projects = NewProjectsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Projects *ProjectsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewProjectsService(s *Service) *ProjectsService {
	rs := &ProjectsService{s: s}
	rs.Configs = NewProjectsConfigsService(s)
	return rs
}

type ProjectsService struct {
	s *Service

	Configs *ProjectsConfigsService
}

func NewProjectsConfigsService(s *Service) *ProjectsConfigsService {
	rs := &ProjectsConfigsService{s: s}
	rs.Operations = NewProjectsConfigsOperationsService(s)
	rs.Variables = NewProjectsConfigsVariablesService(s)
	rs.Waiters = NewProjectsConfigsWaitersService(s)
	return rs
}

type ProjectsConfigsService struct {
	s *Service

	Operations *ProjectsConfigsOperationsService

	Variables *ProjectsConfigsVariablesService

	Waiters *ProjectsConfigsWaitersService
}

func NewProjectsConfigsOperationsService(s *Service) *ProjectsConfigsOperationsService {
	rs := &ProjectsConfigsOperationsService{s: s}
	return rs
}

type ProjectsConfigsOperationsService struct {
	s *Service
}

func NewProjectsConfigsVariablesService(s *Service) *ProjectsConfigsVariablesService {
	rs := &ProjectsConfigsVariablesService{s: s}
	return rs
}

type ProjectsConfigsVariablesService struct {
	s *Service
}

func NewProjectsConfigsWaitersService(s *Service) *ProjectsConfigsWaitersService {
	rs := &ProjectsConfigsWaitersService{s: s}
	return rs
}

type ProjectsConfigsWaitersService struct {
	s *Service
}

// Cardinality: A Cardinality condition for the Waiter resource. A
// cardinality condition is
// met when the number of variables under a specified path prefix
// reaches a
// predefined number. For example, if you set a Cardinality condition
// where
// the `path` is set to `/foo` and the number of paths is set to 2,
// the
// following variables would meet the condition in a RuntimeConfig
// resource:
//
// + `/foo/variable1 = "value1"
// + `/foo/variable2 = "value2"
// + `/bar/variable3 = "value3"
//
// It would not would not satisify the same condition with the `number`
// set to
// 3, however, because there is only 2 paths that start with
// `/foo`.
// Cardinality conditions are recursive; all subtrees under the
// specific
// path prefix are counted.
type Cardinality struct {
	// Number: The number variables under the `path` that must exist to meet
	// this
	// condition. Defaults to 1 if not specified.
	Number int64 `json:"number,omitempty"`

	// Path: The root of the variable subtree to monitor. For example,
	// `/foo`.
	Path string `json:"path,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Number") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Number") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Cardinality) MarshalJSON() ([]byte, error) {
	type noMethod Cardinality
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Empty: A generic empty message that you can re-use to avoid defining
// duplicated
// empty messages in your APIs. A typical example is to use it as the
// request
// or the response type of an API method. For instance:
//
//     service Foo {
//       rpc Bar(google.protobuf.Empty) returns
// (google.protobuf.Empty);
//     }
//
// The JSON representation for `Empty` is empty JSON object `{}`.
type Empty struct {
	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`
}

// EndCondition: The condition that a Waiter resource is waiting for.
type EndCondition struct {
	// Cardinality: The cardinality of the `EndCondition`.
	Cardinality *Cardinality `json:"cardinality,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Cardinality") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Cardinality") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EndCondition) MarshalJSON() ([]byte, error) {
	type noMethod EndCondition
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListConfigsResponse: `ListConfigs()` returns the following response.
// The order of returned
// objects is arbitrary; that is, it is not ordered in any particular
// way.
type ListConfigsResponse struct {
	// Configs: A list of the configurations in the project. The order of
	// returned
	// objects is arbitrary; that is, it is not ordered in any particular
	// way.
	Configs []*RuntimeConfig `json:"configs,omitempty"`

	// NextPageToken: This token allows you to get the next page of results
	// for list requests.
	// If the number of results is larger than `pageSize`, use the
	// `nextPageToken`
	// as a value for the query parameter `pageToken` in the next list
	// request.
	// Subsequent list requests will have their own `nextPageToken` to
	// continue
	// paging through the results
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Configs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Configs") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListConfigsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListConfigsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListVariablesResponse: Response for the `ListVariables()` method.
type ListVariablesResponse struct {
	// NextPageToken: This token allows you to get the next page of results
	// for list requests.
	// If the number of results is larger than `pageSize`, use the
	// `nextPageToken`
	// as a value for the query parameter `pageToken` in the next list
	// request.
	// Subsequent list requests will have their own `nextPageToken` to
	// continue
	// paging through the results
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Variables: A list of variables and their values. The order of
	// returned variable
	// objects is arbitrary.
	Variables []*Variable `json:"variables,omitempty"`

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

func (s *ListVariablesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListVariablesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListWaitersResponse: Response for the `ListWaiters()` method.
// Order of returned waiter objects is arbitrary.
type ListWaitersResponse struct {
	// NextPageToken: This token allows you to get the next page of results
	// for list requests.
	// If the number of results is larger than `pageSize`, use the
	// `nextPageToken`
	// as a value for the query parameter `pageToken` in the next list
	// request.
	// Subsequent list requests will have their own `nextPageToken` to
	// continue
	// paging through the results
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Waiters: Found waiters in the project.
	Waiters []*Waiter `json:"waiters,omitempty"`

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

func (s *ListWaitersResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListWaitersResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Operation: This resource represents a long-running operation that is
// the result of a
// network API call.
type Operation struct {
	// Done: If the value is `false`, it means the operation is still in
	// progress.
	// If true, the operation is completed, and either `error` or `response`
	// is
	// available.
	Done bool `json:"done,omitempty"`

	// Error: The error result of the operation in case of failure or
	// cancellation.
	Error *Status `json:"error,omitempty"`

	// Metadata: Service-specific metadata associated with the operation.
	// It typically
	// contains progress information and common metadata such as create
	// time.
	// Some services might not provide such metadata.  Any method that
	// returns a
	// long-running operation should document the metadata type, if any.
	Metadata googleapi.RawMessage `json:"metadata,omitempty"`

	// Name: The server-assigned name, which is only unique within the same
	// service that
	// originally returns it. If you use the default HTTP mapping,
	// the
	// `name` should have the format of `operations/some/unique/name`.
	Name string `json:"name,omitempty"`

	// Response: The normal response of the operation in case of success.
	// If the original
	// method returns no data on success, such as `Delete`, the response
	// is
	// `google.protobuf.Empty`.  If the original method is
	// standard
	// `Get`/`Create`/`Update`, the response should be the resource.  For
	// other
	// methods, the response should have the type `XxxResponse`, where
	// `Xxx`
	// is the original method name.  For example, if the original method
	// name
	// is `TakeSnapshot()`, the inferred response type
	// is
	// `TakeSnapshotResponse`.
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

// RuntimeConfig: A RuntimeConfig resource is the primary resource in
// the Cloud RuntimeConfig
// service. A RuntimeConfig resource consists of metadata and a
// hierarchy of
// variables.
type RuntimeConfig struct {
	// Description: An optional description of the RuntimeConfig object.
	Description string `json:"description,omitempty"`

	// Name: The resource name of a runtime config. The name must have the
	// format:
	//
	//     projects/[PROJECT_ID]/configs/[CONFIG_NAME]
	//
	// The `[PROJECT_ID]` must be a valid project ID, and `[CONFIG_NAME]` is
	// an
	// arbitrary name that matches RFC 1035 segment specification. The
	// length of
	// `[CONFIG_NAME]` must be less than 64 bytes.
	//
	// You pick the RuntimeConfig resource name, but the server will
	// validate that
	// the name adheres to this format. After you create the resource, you
	// cannot
	// change the resource's name.
	Name string `json:"name,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Description") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Description") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RuntimeConfig) MarshalJSON() ([]byte, error) {
	type noMethod RuntimeConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Status: The `Status` type defines a logical error model that is
// suitable for different
// programming environments, including REST APIs and RPC APIs. It is
// used by
// [gRPC](https://github.com/grpc). The error model is designed to
// be:
//
// - Simple to use and understand for most users
// - Flexible enough to meet unexpected needs
//
// # Overview
//
// The `Status` message contains three pieces of data: error code, error
// message,
// and error details. The error code should be an enum value
// of
// google.rpc.Code, but it may accept additional error codes if needed.
// The
// error message should be a developer-facing English message that
// helps
// developers *understand* and *resolve* the error. If a localized
// user-facing
// error message is needed, put the localized message in the error
// details or
// localize it in the client. The optional error details may contain
// arbitrary
// information about the error. There is a predefined set of error
// detail types
// in the package `google.rpc` which can be used for common error
// conditions.
//
// # Language mapping
//
// The `Status` message is the logical representation of the error
// model, but it
// is not necessarily the actual wire format. When the `Status` message
// is
// exposed in different client libraries and different wire protocols,
// it can be
// mapped differently. For example, it will likely be mapped to some
// exceptions
// in Java, but more likely mapped to some error codes in C.
//
// # Other uses
//
// The error model and the `Status` message can be used in a variety
// of
// environments, either with or without APIs, to provide a
// consistent developer experience across different
// environments.
//
// Example uses of this error model include:
//
// - Partial errors. If a service needs to return partial errors to the
// client,
//     it may embed the `Status` in the normal response to indicate the
// partial
//     errors.
//
// - Workflow errors. A typical workflow has multiple steps. Each step
// may
//     have a `Status` message for error reporting purpose.
//
// - Batch operations. If a client uses batch request and batch
// response, the
//     `Status` message should be used directly inside batch response,
// one for
//     each error sub-response.
//
// - Asynchronous operations. If an API call embeds asynchronous
// operation
//     results in its response, the status of those operations should
// be
//     represented directly using the `Status` message.
//
// - Logging. If some API errors are stored in logs, the message
// `Status` could
//     be used directly after any stripping needed for security/privacy
// reasons.
type Status struct {
	// Code: The status code, which should be an enum value of
	// google.rpc.Code.
	Code int64 `json:"code,omitempty"`

	// Details: A list of messages that carry the error details.  There will
	// be a
	// common set of message types for APIs to use.
	Details []googleapi.RawMessage `json:"details,omitempty"`

	// Message: A developer-facing error message, which should be in
	// English. Any
	// user-facing error message should be localized and sent in
	// the
	// google.rpc.Status.details field, or localized by the client.
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

// Variable: Describes a single variable within a RuntimeConfig
// resource.
// The name denotes the hierarchical variable name. For
// example,
// `ports/serving_port` is a valid variable name. The variable value is
// an
// opaque string and only leaf variables can have values (that is,
// variables
// that do not have any child variables).
type Variable struct {
	// Name: The name of the variable resource, in the format:
	//
	//
	// projects/[PROJECT_ID]/configs/[CONFIG_NAME]/variables/[VARIABLE_NAME]
	//
	//
	// The `[PROJECT_ID]` must be a valid project ID, `[CONFIG_NAME]` must
	// be a
	// valid RuntimeConfig reource and `[VARIABLE_NAME]` follows Unix file
	// system
	// file path naming.
	//
	// The `[VARIABLE_NAME]` can contain ASCII letters, numbers, slashes
	// and
	// dashes. Slashes are used as path element separators and are not part
	// of the
	// `[VARIABLE_NAME]` itself, so `[VARIABLE_NAME]` must contain at least
	// one
	// non-slash character. Multiple slashes are coalesced into single
	// slash
	// character. Each path segment should follow RFC 1035 segment
	// specification.
	// The length of a `[VARIABLE_NAME]` must be less than 256 bytes.
	//
	// Once you create a variable, you cannot change the variable name.
	Name string `json:"name,omitempty"`

	// State: [Ouput only] The current state of the variable. The variable
	// state indicates
	// the outcome of the `variables().watch` call and is visible through
	// the
	// `get` and `list` calls.
	//
	// Possible values:
	//   "VARIABLE_STATE_UNSPECIFIED" - Default variable state.
	//   "UPDATED" - The variable was updated, while `variables().watch` was
	// executing.
	//   "DELETED" - The variable was deleted, while `variables().watch` was
	// executing.
	State string `json:"state,omitempty"`

	// Text: The string value of the variable. The length of the value must
	// be less
	// than 4096 bytes. Empty values are also accepted. For
	// example,
	// <code>text: "my text value"</code>.
	Text string `json:"text,omitempty"`

	// UpdateTime: [Output Only] The time of the last variable update.
	UpdateTime string `json:"updateTime,omitempty"`

	// Value: The binary value of the variable. The length of the value must
	// be less
	// than 4096 bytes. Empty values are also accepted. The value must
	// be
	// base64 encoded. Only one of `value` or `text` can be set.
	Value string `json:"value,omitempty"`

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

func (s *Variable) MarshalJSON() ([]byte, error) {
	type noMethod Variable
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Waiter: A Waiter resource waits for some end condition within a
// RuntimeConfig resource
// to be met before it returns. For example, assume you have a
// distributed
// system where each node writes to a Variable resource indidicating the
// node's
// readiness as part of the startup process.
//
// You then configure a Waiter resource with the success condition set
// to wait
// until some number of nodes have checked in. Afterwards, your
// application
// runs some arbitrary code after the condition has been met and the
// waiter
// returns successfully.
//
// Once created, a Waiter resource is immutable.
//
// To learn more about using waiters, read the
// [Creating a
// Waiter](/deployment-manager/runtime-configurator/creating-a-waiter)
// do
// cumentation.
type Waiter struct {
	// CreateTime: [Output Only] The instant at which this Waiter resource
	// was created. Adding
	// the value of `timeout` to this instant yields the timeout deadline
	// for the
	// waiter.
	CreateTime string `json:"createTime,omitempty"`

	// Done: [Output Only] If the value is `false`, it means the waiter is
	// still waiting
	// for one of its conditions to be met.
	//
	// If true, the waiter has finished. If the waiter finished due to a
	// timeout
	// or failure, `error` will be set.
	Done bool `json:"done,omitempty"`

	// Error: [Output Only] If the waiter ended due to a failure or timeout,
	// this value
	// will be set.
	Error *Status `json:"error,omitempty"`

	// Failure: [Optional] The failure condition of this waiter. If this
	// condition is met,
	// `done` will be set to `true` and the `error` code will be set to
	// `ABORTED`.
	// The failure condition takes precedence over the success condition. If
	// both
	// conditions are met, a failure will be indicated. This value is
	// optional; if
	// no failure condition is set, the only failure scenario will be a
	// timeout.
	Failure *EndCondition `json:"failure,omitempty"`

	// Name: The name of the Waiter resource, in the format:
	//
	//
	// projects/[PROJECT_ID]/configs/[CONFIG_NAME]/waiters/[WAITER_NAME]
	//
	// The
	//  `[PROJECT_ID]` must be a valid Google Cloud project ID,
	// the `[CONFIG_NAME]` must be a valid RuntimeConfig resource,
	// the
	// `[WAITER_NAME]` must match RFC 1035 segment specification, and the
	// length
	// of `[WAITER_NAME]` must be less than 64 bytes.
	//
	// After you create a Waiter resource, you cannot change the resource
	// name.
	Name string `json:"name,omitempty"`

	// Success: [Required] The success condition. If this condition is met,
	// `done` will be
	// set to `true` and the `error` value will remain unset. The failure
	// condition
	// takes precedence over the success condition. If both conditions are
	// met, a
	// failure will be indicated.
	Success *EndCondition `json:"success,omitempty"`

	// Timeout: [Required] Specifies the timeout of the waiter in seconds,
	// beginning from
	// the instant that `waiters().create` method is called. If this time
	// elapses
	// before the success or failure conditions are met, the waiter fails
	// and sets
	// the `error` code to `DEADLINE_EXCEEDED`.
	Timeout string `json:"timeout,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CreateTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreateTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Waiter) MarshalJSON() ([]byte, error) {
	type noMethod Waiter
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WatchVariableRequest: Request for the `WatchVariable()` method.
type WatchVariableRequest struct {
	// NewerThan: If specified, checks the current timestamp of the variable
	// and if the
	// current timestamp is newer than `newerThan` timestamp, the method
	// returns
	// immediately.
	//
	// If not specified or the variable has an older timestamp, the watcher
	// waits
	// for a the value to change before returning.
	NewerThan string `json:"newerThan,omitempty"`

	// ForceSendFields is a list of field names (e.g. "NewerThan") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NewerThan") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WatchVariableRequest) MarshalJSON() ([]byte, error) {
	type noMethod WatchVariableRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "runtimeconfig.projects.configs.create":

type ProjectsConfigsCreateCall struct {
	s             *Service
	parent        string
	runtimeconfig *RuntimeConfig
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// Create: Creates a new RuntimeConfig resource. The configuration name
// must be
// unique within project.
func (r *ProjectsConfigsService) Create(parent string, runtimeconfig *RuntimeConfig) *ProjectsConfigsCreateCall {
	c := &ProjectsConfigsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.runtimeconfig = runtimeconfig
	return c
}

// RequestId sets the optional parameter "requestId": An optional but
// recommended unique <code>request_id</code>. If the server
// receives two <code>create()</code> requests  with the
// same
// <code>request_id</code>, then the second request will be ignored and
// the
// first resource created and stored in the backend is returned.
// Empty <code>request_id</code> fields are ignored.
//
// It is responsibility of the client to ensure uniqueness of
// the
// <code>request_id</code> strings.
//
// <code>request_id</code> strings are limited to 64 characters.
func (c *ProjectsConfigsCreateCall) RequestId(requestId string) *ProjectsConfigsCreateCall {
	c.urlParams_.Set("requestId", requestId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsCreateCall) Fields(s ...googleapi.Field) *ProjectsConfigsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsCreateCall) Context(ctx context.Context) *ProjectsConfigsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.runtimeconfig)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/configs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.create" call.
// Exactly one of *RuntimeConfig or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RuntimeConfig.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsConfigsCreateCall) Do(opts ...googleapi.CallOption) (*RuntimeConfig, error) {
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
	ret := &RuntimeConfig{
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
	//   "description": "Creates a new RuntimeConfig resource. The configuration name must be\nunique within project.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs",
	//   "httpMethod": "POST",
	//   "id": "runtimeconfig.projects.configs.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "The [project ID](https://support.google.com/cloud/answer/6158840?hl=en\u0026ref_topic=6158848)\nfor this request, in the format `projects/[PROJECT_ID]`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "requestId": {
	//       "description": "An optional but recommended unique \u003ccode\u003erequest_id\u003c/code\u003e. If the server\nreceives two \u003ccode\u003ecreate()\u003c/code\u003e requests  with the same\n\u003ccode\u003erequest_id\u003c/code\u003e, then the second request will be ignored and the\nfirst resource created and stored in the backend is returned.\nEmpty \u003ccode\u003erequest_id\u003c/code\u003e fields are ignored.\n\nIt is responsibility of the client to ensure uniqueness of the\n\u003ccode\u003erequest_id\u003c/code\u003e strings.\n\n\u003ccode\u003erequest_id\u003c/code\u003e strings are limited to 64 characters.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/configs",
	//   "request": {
	//     "$ref": "RuntimeConfig"
	//   },
	//   "response": {
	//     "$ref": "RuntimeConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.delete":

type ProjectsConfigsDeleteCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a RuntimeConfig resource.
func (r *ProjectsConfigsService) Delete(name string) *ProjectsConfigsDeleteCall {
	c := &ProjectsConfigsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsDeleteCall) Fields(s ...googleapi.Field) *ProjectsConfigsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsDeleteCall) Context(ctx context.Context) *ProjectsConfigsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProjectsConfigsDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Deletes a RuntimeConfig resource.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}",
	//   "httpMethod": "DELETE",
	//   "id": "runtimeconfig.projects.configs.delete",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The RuntimeConfig resource to delete, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.get":

type ProjectsConfigsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a RuntimeConfig resource.
func (r *ProjectsConfigsService) Get(name string) *ProjectsConfigsGetCall {
	c := &ProjectsConfigsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsGetCall) Fields(s ...googleapi.Field) *ProjectsConfigsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsGetCall) IfNoneMatch(entityTag string) *ProjectsConfigsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsGetCall) Context(ctx context.Context) *ProjectsConfigsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.get" call.
// Exactly one of *RuntimeConfig or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RuntimeConfig.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsConfigsGetCall) Do(opts ...googleapi.CallOption) (*RuntimeConfig, error) {
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
	ret := &RuntimeConfig{
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
	//   "description": "Gets information about a RuntimeConfig resource.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the RuntimeConfig resource to retrieve, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "RuntimeConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.list":

type ProjectsConfigsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists all the RuntimeConfig resources within project.
func (r *ProjectsConfigsService) List(parent string) *ProjectsConfigsListCall {
	c := &ProjectsConfigsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// PageSize sets the optional parameter "pageSize": Specifies the number
// of results to return per page. If there are fewer
// elements than the specified number, returns all elements.
func (c *ProjectsConfigsListCall) PageSize(pageSize int64) *ProjectsConfigsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Specifies a page
// token to use. Set `pageToken` to a `nextPageToken`
// returned by a previous list request to get the next page of results.
func (c *ProjectsConfigsListCall) PageToken(pageToken string) *ProjectsConfigsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsListCall) Fields(s ...googleapi.Field) *ProjectsConfigsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsListCall) IfNoneMatch(entityTag string) *ProjectsConfigsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsListCall) Context(ctx context.Context) *ProjectsConfigsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/configs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.list" call.
// Exactly one of *ListConfigsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListConfigsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsConfigsListCall) Do(opts ...googleapi.CallOption) (*ListConfigsResponse, error) {
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
	ret := &ListConfigsResponse{
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
	//   "description": "Lists all the RuntimeConfig resources within project.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "pageSize": {
	//       "description": "Specifies the number of results to return per page. If there are fewer\nelements than the specified number, returns all elements.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Specifies a page token to use. Set `pageToken` to a `nextPageToken`\nreturned by a previous list request to get the next page of results.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "The [project ID](https://support.google.com/cloud/answer/6158840?hl=en\u0026ref_topic=6158848)\nfor this request, in the format `projects/[PROJECT_ID]`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/configs",
	//   "response": {
	//     "$ref": "ListConfigsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsConfigsListCall) Pages(ctx context.Context, f func(*ListConfigsResponse) error) error {
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

// method id "runtimeconfig.projects.configs.update":

type ProjectsConfigsUpdateCall struct {
	s             *Service
	name          string
	runtimeconfig *RuntimeConfig
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// Update: Updates a RuntimeConfig resource. The configuration must
// exist beforehand.
func (r *ProjectsConfigsService) Update(name string, runtimeconfig *RuntimeConfig) *ProjectsConfigsUpdateCall {
	c := &ProjectsConfigsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.runtimeconfig = runtimeconfig
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsUpdateCall) Fields(s ...googleapi.Field) *ProjectsConfigsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsUpdateCall) Context(ctx context.Context) *ProjectsConfigsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.runtimeconfig)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.update" call.
// Exactly one of *RuntimeConfig or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RuntimeConfig.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsConfigsUpdateCall) Do(opts ...googleapi.CallOption) (*RuntimeConfig, error) {
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
	ret := &RuntimeConfig{
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
	//   "description": "Updates a RuntimeConfig resource. The configuration must exist beforehand.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}",
	//   "httpMethod": "PUT",
	//   "id": "runtimeconfig.projects.configs.update",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the RuntimeConfig resource to update, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "request": {
	//     "$ref": "RuntimeConfig"
	//   },
	//   "response": {
	//     "$ref": "RuntimeConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.operations.get":

type ProjectsConfigsOperationsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the latest state of a long-running operation.  Clients can
// use this
// method to poll the operation result at intervals as recommended by
// the API
// service.
func (r *ProjectsConfigsOperationsService) Get(name string) *ProjectsConfigsOperationsGetCall {
	c := &ProjectsConfigsOperationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsOperationsGetCall) Fields(s ...googleapi.Field) *ProjectsConfigsOperationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsOperationsGetCall) IfNoneMatch(entityTag string) *ProjectsConfigsOperationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsOperationsGetCall) Context(ctx context.Context) *ProjectsConfigsOperationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsOperationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsOperationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.operations.get" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsConfigsOperationsGetCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	//   "description": "Gets the latest state of a long-running operation.  Clients can use this\nmethod to poll the operation result at intervals as recommended by the API\nservice.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/operations/{operationsId}",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.operations.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the operation resource.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/operations/.+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.variables.create":

type ProjectsConfigsVariablesCreateCall struct {
	s          *Service
	parent     string
	variable   *Variable
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a variable within the given configuration. You cannot
// create
// a variable with a name that is a prefix of an existing variable name,
// or a
// name that has an existing variable name as a prefix.
//
// To learn more about creating a variable, read the
// [Setting and Getting
// Data](/deployment-manager/runtime-configurator/set-and-get-variables)
//
// documentation.
func (r *ProjectsConfigsVariablesService) Create(parent string, variable *Variable) *ProjectsConfigsVariablesCreateCall {
	c := &ProjectsConfigsVariablesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.variable = variable
	return c
}

// RequestId sets the optional parameter "requestId": An optional but
// recommended unique <code>request_id</code>. If the server
// receives two <code>create()</code> requests  with the
// same
// <code>request_id</code>, then the second request will be ignored and
// the
// first resource created and stored in the backend is returned.
// Empty <code>request_id</code> fields are ignored.
//
// It is responsibility of the client to ensure uniqueness of
// the
// <code>request_id</code> strings.
//
// <code>request_id</code> strings are limited to 64 characters.
func (c *ProjectsConfigsVariablesCreateCall) RequestId(requestId string) *ProjectsConfigsVariablesCreateCall {
	c.urlParams_.Set("requestId", requestId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsVariablesCreateCall) Fields(s ...googleapi.Field) *ProjectsConfigsVariablesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsVariablesCreateCall) Context(ctx context.Context) *ProjectsConfigsVariablesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsVariablesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsVariablesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.variable)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/variables")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.variables.create" call.
// Exactly one of *Variable or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Variable.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsConfigsVariablesCreateCall) Do(opts ...googleapi.CallOption) (*Variable, error) {
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
	ret := &Variable{
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
	//   "description": "Creates a variable within the given configuration. You cannot create\na variable with a name that is a prefix of an existing variable name, or a\nname that has an existing variable name as a prefix.\n\nTo learn more about creating a variable, read the\n[Setting and Getting Data](/deployment-manager/runtime-configurator/set-and-get-variables)\ndocumentation.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/variables",
	//   "httpMethod": "POST",
	//   "id": "runtimeconfig.projects.configs.variables.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "The path to the RutimeConfig resource that this variable should belong to.\nThe configuration must exist beforehand; the path must by in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "requestId": {
	//       "description": "An optional but recommended unique \u003ccode\u003erequest_id\u003c/code\u003e. If the server\nreceives two \u003ccode\u003ecreate()\u003c/code\u003e requests  with the same\n\u003ccode\u003erequest_id\u003c/code\u003e, then the second request will be ignored and the\nfirst resource created and stored in the backend is returned.\nEmpty \u003ccode\u003erequest_id\u003c/code\u003e fields are ignored.\n\nIt is responsibility of the client to ensure uniqueness of the\n\u003ccode\u003erequest_id\u003c/code\u003e strings.\n\n\u003ccode\u003erequest_id\u003c/code\u003e strings are limited to 64 characters.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/variables",
	//   "request": {
	//     "$ref": "Variable"
	//   },
	//   "response": {
	//     "$ref": "Variable"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.variables.delete":

type ProjectsConfigsVariablesDeleteCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a variable or multiple variables.
//
// If you specify a variable name, then that variable is deleted. If
// you
// specify a prefix and `recursive` is true, then all variables with
// that
// prefix are deleted. You must set a `recursive` to true if you
// delete
// variables by prefix.
func (r *ProjectsConfigsVariablesService) Delete(name string) *ProjectsConfigsVariablesDeleteCall {
	c := &ProjectsConfigsVariablesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Recursive sets the optional parameter "recursive": Set to `true` to
// recursively delete multiple variables with the same
// prefix.
func (c *ProjectsConfigsVariablesDeleteCall) Recursive(recursive bool) *ProjectsConfigsVariablesDeleteCall {
	c.urlParams_.Set("recursive", fmt.Sprint(recursive))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsVariablesDeleteCall) Fields(s ...googleapi.Field) *ProjectsConfigsVariablesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsVariablesDeleteCall) Context(ctx context.Context) *ProjectsConfigsVariablesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsVariablesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsVariablesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.variables.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProjectsConfigsVariablesDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Deletes a variable or multiple variables.\n\nIf you specify a variable name, then that variable is deleted. If you\nspecify a prefix and `recursive` is true, then all variables with that\nprefix are deleted. You must set a `recursive` to true if you delete\nvariables by prefix.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/variables/{variablesId}",
	//   "httpMethod": "DELETE",
	//   "id": "runtimeconfig.projects.configs.variables.delete",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the variable to delete, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]/variables/[VARIABLE_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/variables/.+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "recursive": {
	//       "description": "Set to `true` to recursively delete multiple variables with the same\nprefix.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.variables.get":

type ProjectsConfigsVariablesGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a single variable.
func (r *ProjectsConfigsVariablesService) Get(name string) *ProjectsConfigsVariablesGetCall {
	c := &ProjectsConfigsVariablesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsVariablesGetCall) Fields(s ...googleapi.Field) *ProjectsConfigsVariablesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsVariablesGetCall) IfNoneMatch(entityTag string) *ProjectsConfigsVariablesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsVariablesGetCall) Context(ctx context.Context) *ProjectsConfigsVariablesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsVariablesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsVariablesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.variables.get" call.
// Exactly one of *Variable or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Variable.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsConfigsVariablesGetCall) Do(opts ...googleapi.CallOption) (*Variable, error) {
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
	ret := &Variable{
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
	//   "description": "Gets information about a single variable.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/variables/{variablesId}",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.variables.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the variable to return, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]/variables/[VARIBLE_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/variables/.+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "Variable"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.variables.list":

type ProjectsConfigsVariablesListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists variables within given a configuration, matching any
// provided filters.
// This only lists variable names, not the values.
func (r *ProjectsConfigsVariablesService) List(parent string) *ProjectsConfigsVariablesListCall {
	c := &ProjectsConfigsVariablesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// Filter sets the optional parameter "filter": Filters variables by
// matching the specified filter. For
// example:
//
// `projects/example-project/config/[CONFIG_NAME]/variables/exa
// mple-variable`.
func (c *ProjectsConfigsVariablesListCall) Filter(filter string) *ProjectsConfigsVariablesListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": Specifies the number
// of results to return per page. If there are fewer
// elements than the specified number, returns all elements.
func (c *ProjectsConfigsVariablesListCall) PageSize(pageSize int64) *ProjectsConfigsVariablesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Specifies a page
// token to use. Set `pageToken` to a `nextPageToken`
// returned by a previous list request to get the next page of results.
func (c *ProjectsConfigsVariablesListCall) PageToken(pageToken string) *ProjectsConfigsVariablesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsVariablesListCall) Fields(s ...googleapi.Field) *ProjectsConfigsVariablesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsVariablesListCall) IfNoneMatch(entityTag string) *ProjectsConfigsVariablesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsVariablesListCall) Context(ctx context.Context) *ProjectsConfigsVariablesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsVariablesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsVariablesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/variables")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.variables.list" call.
// Exactly one of *ListVariablesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListVariablesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsConfigsVariablesListCall) Do(opts ...googleapi.CallOption) (*ListVariablesResponse, error) {
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
	ret := &ListVariablesResponse{
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
	//   "description": "Lists variables within given a configuration, matching any provided filters.\nThis only lists variable names, not the values.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/variables",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.variables.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "Filters variables by matching the specified filter. For example:\n\n`projects/example-project/config/[CONFIG_NAME]/variables/example-variable`.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Specifies the number of results to return per page. If there are fewer\nelements than the specified number, returns all elements.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Specifies a page token to use. Set `pageToken` to a `nextPageToken`\nreturned by a previous list request to get the next page of results.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "The path to the RuntimeConfig resource for which you want to list variables.\nThe configuration must exist beforehand; the path must by in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/variables",
	//   "response": {
	//     "$ref": "ListVariablesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsConfigsVariablesListCall) Pages(ctx context.Context, f func(*ListVariablesResponse) error) error {
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

// method id "runtimeconfig.projects.configs.variables.update":

type ProjectsConfigsVariablesUpdateCall struct {
	s          *Service
	name       string
	variable   *Variable
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates an existing variable with a new value.
func (r *ProjectsConfigsVariablesService) Update(name string, variable *Variable) *ProjectsConfigsVariablesUpdateCall {
	c := &ProjectsConfigsVariablesUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.variable = variable
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsVariablesUpdateCall) Fields(s ...googleapi.Field) *ProjectsConfigsVariablesUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsVariablesUpdateCall) Context(ctx context.Context) *ProjectsConfigsVariablesUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsVariablesUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsVariablesUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.variable)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.variables.update" call.
// Exactly one of *Variable or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Variable.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsConfigsVariablesUpdateCall) Do(opts ...googleapi.CallOption) (*Variable, error) {
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
	ret := &Variable{
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
	//   "description": "Updates an existing variable with a new value.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/variables/{variablesId}",
	//   "httpMethod": "PUT",
	//   "id": "runtimeconfig.projects.configs.variables.update",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the variable to update, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]/variables/[VARIABLE_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/variables/.+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "request": {
	//     "$ref": "Variable"
	//   },
	//   "response": {
	//     "$ref": "Variable"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.variables.watch":

type ProjectsConfigsVariablesWatchCall struct {
	s                    *Service
	name                 string
	watchvariablerequest *WatchVariableRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Watch: Watches a specific variable and waits for a change in the
// variable's value.
// When there is a change, this method returns the new value or times
// out.
//
// If a variable is deleted while being watched, the `variableState`
// state is
// set to `DELETED` and the method returns the last known variable
// `value`.
//
// If you set the deadline for watching to a larger value than internal
// timeout
// (60 seconds), the current variable value is returned and the
// `variableState`
// will be `VARIABLE_STATE_UNSPECIFIED`.
//
// To learn more about creating a watcher, read the
// [Watching a Variable for
// Changes](/deployment-manager/runtime-configurator/watching-a-variable)
//
// documentation.
func (r *ProjectsConfigsVariablesService) Watch(name string, watchvariablerequest *WatchVariableRequest) *ProjectsConfigsVariablesWatchCall {
	c := &ProjectsConfigsVariablesWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.watchvariablerequest = watchvariablerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsVariablesWatchCall) Fields(s ...googleapi.Field) *ProjectsConfigsVariablesWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsVariablesWatchCall) Context(ctx context.Context) *ProjectsConfigsVariablesWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsVariablesWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsVariablesWatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.watchvariablerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}:watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.variables.watch" call.
// Exactly one of *Variable or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Variable.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsConfigsVariablesWatchCall) Do(opts ...googleapi.CallOption) (*Variable, error) {
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
	ret := &Variable{
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
	//   "description": "Watches a specific variable and waits for a change in the variable's value.\nWhen there is a change, this method returns the new value or times out.\n\nIf a variable is deleted while being watched, the `variableState` state is\nset to `DELETED` and the method returns the last known variable `value`.\n\nIf you set the deadline for watching to a larger value than internal timeout\n(60 seconds), the current variable value is returned and the `variableState`\nwill be `VARIABLE_STATE_UNSPECIFIED`.\n\nTo learn more about creating a watcher, read the\n[Watching a Variable for Changes](/deployment-manager/runtime-configurator/watching-a-variable)\ndocumentation.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/variables/{variablesId}:watch",
	//   "httpMethod": "POST",
	//   "id": "runtimeconfig.projects.configs.variables.watch",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the variable to watch, in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/variables/.+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}:watch",
	//   "request": {
	//     "$ref": "WatchVariableRequest"
	//   },
	//   "response": {
	//     "$ref": "Variable"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.waiters.create":

type ProjectsConfigsWaitersCreateCall struct {
	s          *Service
	parent     string
	waiter     *Waiter
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a Waiter resource. This operation returns a
// long-running Operation
// resource which can be polled for completion. However, a waiter with
// the
// given name will exist (and can be retrieved) prior to the
// operation
// completing. If the operation fails, the failed Waiter resource
// will
// still exist and must be deleted prior to subsequent creation
// attempts.
func (r *ProjectsConfigsWaitersService) Create(parent string, waiter *Waiter) *ProjectsConfigsWaitersCreateCall {
	c := &ProjectsConfigsWaitersCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.waiter = waiter
	return c
}

// RequestId sets the optional parameter "requestId": An optional but
// recommended unique <code>request_id</code>. If the server
// receives two <code>create()</code> requests  with the
// same
// <code>request_id</code>, then the second request will be ignored and
// the
// first resource created and stored in the backend is returned.
// Empty <code>request_id</code> fields are ignored.
//
// It is responsibility of the client to ensure uniqueness of
// the
// <code>request_id</code> strings.
//
// <code>request_id</code> strings are limited to 64 characters.
func (c *ProjectsConfigsWaitersCreateCall) RequestId(requestId string) *ProjectsConfigsWaitersCreateCall {
	c.urlParams_.Set("requestId", requestId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsWaitersCreateCall) Fields(s ...googleapi.Field) *ProjectsConfigsWaitersCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsWaitersCreateCall) Context(ctx context.Context) *ProjectsConfigsWaitersCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsWaitersCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsWaitersCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.waiter)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/waiters")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.waiters.create" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsConfigsWaitersCreateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	//   "description": "Creates a Waiter resource. This operation returns a long-running Operation\nresource which can be polled for completion. However, a waiter with the\ngiven name will exist (and can be retrieved) prior to the operation\ncompleting. If the operation fails, the failed Waiter resource will\nstill exist and must be deleted prior to subsequent creation attempts.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/waiters",
	//   "httpMethod": "POST",
	//   "id": "runtimeconfig.projects.configs.waiters.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "The path to the configuration that will own the waiter.\nThe configuration must exist beforehand; the path must by in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "requestId": {
	//       "description": "An optional but recommended unique \u003ccode\u003erequest_id\u003c/code\u003e. If the server\nreceives two \u003ccode\u003ecreate()\u003c/code\u003e requests  with the same\n\u003ccode\u003erequest_id\u003c/code\u003e, then the second request will be ignored and the\nfirst resource created and stored in the backend is returned.\nEmpty \u003ccode\u003erequest_id\u003c/code\u003e fields are ignored.\n\nIt is responsibility of the client to ensure uniqueness of the\n\u003ccode\u003erequest_id\u003c/code\u003e strings.\n\n\u003ccode\u003erequest_id\u003c/code\u003e strings are limited to 64 characters.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/waiters",
	//   "request": {
	//     "$ref": "Waiter"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.waiters.delete":

type ProjectsConfigsWaitersDeleteCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes the waiter with the specified name.
func (r *ProjectsConfigsWaitersService) Delete(name string) *ProjectsConfigsWaitersDeleteCall {
	c := &ProjectsConfigsWaitersDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsWaitersDeleteCall) Fields(s ...googleapi.Field) *ProjectsConfigsWaitersDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsWaitersDeleteCall) Context(ctx context.Context) *ProjectsConfigsWaitersDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsWaitersDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsWaitersDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.waiters.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProjectsConfigsWaitersDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Deletes the waiter with the specified name.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/waiters/{waitersId}",
	//   "httpMethod": "DELETE",
	//   "id": "runtimeconfig.projects.configs.waiters.delete",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The Waiter resource to delete, in the format:\n\n `projects/[PROJECT_ID]/configs/[CONFIG_NAME]/waiters/[WAITER_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/waiters/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.waiters.get":

type ProjectsConfigsWaitersGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a single waiter.
func (r *ProjectsConfigsWaitersService) Get(name string) *ProjectsConfigsWaitersGetCall {
	c := &ProjectsConfigsWaitersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsWaitersGetCall) Fields(s ...googleapi.Field) *ProjectsConfigsWaitersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsWaitersGetCall) IfNoneMatch(entityTag string) *ProjectsConfigsWaitersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsWaitersGetCall) Context(ctx context.Context) *ProjectsConfigsWaitersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsWaitersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsWaitersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.waiters.get" call.
// Exactly one of *Waiter or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Waiter.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProjectsConfigsWaitersGetCall) Do(opts ...googleapi.CallOption) (*Waiter, error) {
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
	ret := &Waiter{
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
	//   "description": "Gets information about a single waiter.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/waiters/{waitersId}",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.waiters.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The fully-qualified name of the Waiter resource object to retrieve, in the\nformat:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]/waiters/[WAITER_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+/waiters/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "Waiter"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// method id "runtimeconfig.projects.configs.waiters.list":

type ProjectsConfigsWaitersListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List waiters within the given configuration.
func (r *ProjectsConfigsWaitersService) List(parent string) *ProjectsConfigsWaitersListCall {
	c := &ProjectsConfigsWaitersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// PageSize sets the optional parameter "pageSize": Specifies the number
// of results to return per page. If there are fewer
// elements than the specified number, returns all elements.
func (c *ProjectsConfigsWaitersListCall) PageSize(pageSize int64) *ProjectsConfigsWaitersListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Specifies a page
// token to use. Set `pageToken` to a `nextPageToken`
// returned by a previous list request to get the next page of results.
func (c *ProjectsConfigsWaitersListCall) PageToken(pageToken string) *ProjectsConfigsWaitersListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsConfigsWaitersListCall) Fields(s ...googleapi.Field) *ProjectsConfigsWaitersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsConfigsWaitersListCall) IfNoneMatch(entityTag string) *ProjectsConfigsWaitersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsConfigsWaitersListCall) Context(ctx context.Context) *ProjectsConfigsWaitersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsConfigsWaitersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsConfigsWaitersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/waiters")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "runtimeconfig.projects.configs.waiters.list" call.
// Exactly one of *ListWaitersResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListWaitersResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsConfigsWaitersListCall) Do(opts ...googleapi.CallOption) (*ListWaitersResponse, error) {
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
	ret := &ListWaitersResponse{
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
	//   "description": "List waiters within the given configuration.",
	//   "flatPath": "v1beta1/projects/{projectsId}/configs/{configsId}/waiters",
	//   "httpMethod": "GET",
	//   "id": "runtimeconfig.projects.configs.waiters.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "pageSize": {
	//       "description": "Specifies the number of results to return per page. If there are fewer\nelements than the specified number, returns all elements.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Specifies a page token to use. Set `pageToken` to a `nextPageToken`\nreturned by a previous list request to get the next page of results.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "The path to the configuration for which you want to get a list of waiters.\nThe configuration must exist beforehand; the path must by in the format:\n\n`projects/[PROJECT_ID]/configs/[CONFIG_NAME]`",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/configs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/waiters",
	//   "response": {
	//     "$ref": "ListWaitersResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloudruntimeconfig"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsConfigsWaitersListCall) Pages(ctx context.Context, f func(*ListWaitersResponse) error) error {
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

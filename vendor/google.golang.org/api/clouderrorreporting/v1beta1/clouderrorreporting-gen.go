// Package clouderrorreporting provides access to the Stackdriver Error Reporting API.
//
// See https://cloud.google.com/error-reporting/
//
// Usage example:
//
//   import "google.golang.org/api/clouderrorreporting/v1beta1"
//   ...
//   clouderrorreportingService, err := clouderrorreporting.New(oauthHttpClient)
package clouderrorreporting // import "google.golang.org/api/clouderrorreporting/v1beta1"

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

const apiId = "clouderrorreporting:v1beta1"
const apiName = "clouderrorreporting"
const apiVersion = "v1beta1"
const basePath = "https://clouderrorreporting.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
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
	rs.Events = NewProjectsEventsService(s)
	rs.GroupStats = NewProjectsGroupStatsService(s)
	rs.Groups = NewProjectsGroupsService(s)
	return rs
}

type ProjectsService struct {
	s *Service

	Events *ProjectsEventsService

	GroupStats *ProjectsGroupStatsService

	Groups *ProjectsGroupsService
}

func NewProjectsEventsService(s *Service) *ProjectsEventsService {
	rs := &ProjectsEventsService{s: s}
	return rs
}

type ProjectsEventsService struct {
	s *Service
}

func NewProjectsGroupStatsService(s *Service) *ProjectsGroupStatsService {
	rs := &ProjectsGroupStatsService{s: s}
	return rs
}

type ProjectsGroupStatsService struct {
	s *Service
}

func NewProjectsGroupsService(s *Service) *ProjectsGroupsService {
	rs := &ProjectsGroupsService{s: s}
	return rs
}

type ProjectsGroupsService struct {
	s *Service
}

// DeleteEventsResponse: Response message for deleting error events.
type DeleteEventsResponse struct {
	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`
}

// ErrorContext: A description of the context in which an error
// occurred.
// This data should be provided by the application when reporting an
// error,
// unless the
// error report has been generated automatically from Google App Engine
// logs.
type ErrorContext struct {
	// HttpRequest: The HTTP request which was processed when the error
	// was
	// triggered.
	HttpRequest *HttpRequestContext `json:"httpRequest,omitempty"`

	// ReportLocation: The location in the source code where the decision
	// was made to
	// report the error, usually the place where it was logged.
	// For a logged exception this would be the source line where
	// the
	// exception is logged, usually close to the place where it was
	// caught. This value is in contrast to
	// `Exception.cause_location`,
	// which describes the source line where the exception was thrown.
	ReportLocation *SourceLocation `json:"reportLocation,omitempty"`

	// User: The user who caused or was affected by the crash.
	// This can be a user ID, an email address, or an arbitrary token
	// that
	// uniquely identifies the user.
	// When sending an error report, leave this field empty if the user was
	// not
	// logged in. In this case the
	// Error Reporting system will use other data, such as remote IP
	// address, to
	// distinguish affected users. See `affected_users_count`
	// in
	// `ErrorGroupStats`.
	User string `json:"user,omitempty"`

	// ForceSendFields is a list of field names (e.g. "HttpRequest") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HttpRequest") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ErrorContext) MarshalJSON() ([]byte, error) {
	type noMethod ErrorContext
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ErrorEvent: An error event which is returned by the Error Reporting
// system.
type ErrorEvent struct {
	// Context: Data about the context in which the error occurred.
	Context *ErrorContext `json:"context,omitempty"`

	// EventTime: Time when the event occurred as provided in the error
	// report.
	// If the report did not contain a timestamp, the time the error was
	// received
	// by the Error Reporting system is used.
	EventTime string `json:"eventTime,omitempty"`

	// Message: The stack trace that was reported or logged by the service.
	Message string `json:"message,omitempty"`

	// ServiceContext: The `ServiceContext` for which this error was
	// reported.
	ServiceContext *ServiceContext `json:"serviceContext,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Context") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Context") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ErrorEvent) MarshalJSON() ([]byte, error) {
	type noMethod ErrorEvent
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ErrorGroup: Description of a group of similar error events.
type ErrorGroup struct {
	// GroupId: Group IDs are unique for a given project. If the same kind
	// of error
	// occurs in different service contexts, it will receive the same group
	// ID.
	GroupId string `json:"groupId,omitempty"`

	// Name: The group resource name.
	// Example: <code>projects/my-project-123/groups/my-groupid</code>
	Name string `json:"name,omitempty"`

	// TrackingIssues: Associated tracking issues.
	TrackingIssues []*TrackingIssue `json:"trackingIssues,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "GroupId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GroupId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ErrorGroup) MarshalJSON() ([]byte, error) {
	type noMethod ErrorGroup
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ErrorGroupStats: Data extracted for a specific group based on certain
// filter criteria,
// such as a given time period and/or service filter.
type ErrorGroupStats struct {
	// AffectedServices: Service contexts with a non-zero error count for
	// the given filter
	// criteria. This list can be truncated if multiple services are
	// affected.
	// Refer to `num_affected_services` for the total count.
	AffectedServices []*ServiceContext `json:"affectedServices,omitempty"`

	// AffectedUsersCount: Approximate number of affected users in the given
	// group that
	// match the filter criteria.
	// Users are distinguished by data in the `ErrorContext` of
	// the
	// individual error events, such as their login name or their remote
	// IP address in case of HTTP requests.
	// The number of affected users can be zero even if the number of
	// errors is non-zero if no data was provided from which the
	// affected user could be deduced.
	// Users are counted based on data in the request
	// context that was provided in the error report. If more users
	// are
	// implicitly affected, such as due to a crash of the whole
	// service,
	// this is not reflected here.
	AffectedUsersCount int64 `json:"affectedUsersCount,omitempty,string"`

	// Count: Approximate total number of events in the given group that
	// match
	// the filter criteria.
	Count int64 `json:"count,omitempty,string"`

	// FirstSeenTime: Approximate first occurrence that was ever seen for
	// this group
	// and which matches the given filter criteria, ignoring the
	// time_range that was specified in the request.
	FirstSeenTime string `json:"firstSeenTime,omitempty"`

	// Group: Group data that is independent of the filter criteria.
	Group *ErrorGroup `json:"group,omitempty"`

	// LastSeenTime: Approximate last occurrence that was ever seen for this
	// group and
	// which matches the given filter criteria, ignoring the time_range
	// that was specified in the request.
	LastSeenTime string `json:"lastSeenTime,omitempty"`

	// NumAffectedServices: The total number of services with a non-zero
	// error count for the given
	// filter criteria.
	NumAffectedServices int64 `json:"numAffectedServices,omitempty"`

	// Representative: An arbitrary event that is chosen as representative
	// for the whole group.
	// The representative event is intended to be used as a quick preview
	// for
	// the whole group. Events in the group are usually sufficiently
	// similar
	// to each other such that showing an arbitrary representative
	// provides
	// insight into the characteristics of the group as a whole.
	Representative *ErrorEvent `json:"representative,omitempty"`

	// TimedCounts: Approximate number of occurrences over time.
	// Timed counts returned by ListGroups are guaranteed to be:
	//
	// - Inside the requested time interval
	// - Non-overlapping, and
	// - Ordered by ascending time.
	TimedCounts []*TimedCount `json:"timedCounts,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AffectedServices") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AffectedServices") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ErrorGroupStats) MarshalJSON() ([]byte, error) {
	type noMethod ErrorGroupStats
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// HttpRequestContext: HTTP request data that is related to a reported
// error.
// This data should be provided by the application when reporting an
// error,
// unless the
// error report has been generated automatically from Google App Engine
// logs.
type HttpRequestContext struct {
	// Method: The type of HTTP request, such as `GET`, `POST`, etc.
	Method string `json:"method,omitempty"`

	// Referrer: The referrer information that is provided with the request.
	Referrer string `json:"referrer,omitempty"`

	// RemoteIp: The IP address from which the request originated.
	// This can be IPv4, IPv6, or a token which is derived from the
	// IP address, depending on the data that has been provided
	// in the error report.
	RemoteIp string `json:"remoteIp,omitempty"`

	// ResponseStatusCode: The HTTP response status code for the request.
	ResponseStatusCode int64 `json:"responseStatusCode,omitempty"`

	// Url: The URL of the request.
	Url string `json:"url,omitempty"`

	// UserAgent: The user agent information that is provided with the
	// request.
	UserAgent string `json:"userAgent,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Method") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Method") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *HttpRequestContext) MarshalJSON() ([]byte, error) {
	type noMethod HttpRequestContext
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListEventsResponse: Contains a set of requested error events.
type ListEventsResponse struct {
	// ErrorEvents: The error events which match the given request.
	ErrorEvents []*ErrorEvent `json:"errorEvents,omitempty"`

	// NextPageToken: If non-empty, more results are available.
	// Pass this token, along with the same query parameters as the
	// first
	// request, to view the next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TimeRangeBegin: The timestamp specifies the start time to which the
	// request was restricted.
	TimeRangeBegin string `json:"timeRangeBegin,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ErrorEvents") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ErrorEvents") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListEventsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListEventsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListGroupStatsResponse: Contains a set of requested error group
// stats.
type ListGroupStatsResponse struct {
	// ErrorGroupStats: The error group stats which match the given request.
	ErrorGroupStats []*ErrorGroupStats `json:"errorGroupStats,omitempty"`

	// NextPageToken: If non-empty, more results are available.
	// Pass this token, along with the same query parameters as the
	// first
	// request, to view the next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TimeRangeBegin: The timestamp specifies the start time to which the
	// request was restricted.
	// The start time is set based on the requested time range. It may be
	// adjusted
	// to a later time if a project has exceeded the storage quota and older
	// data
	// has been deleted.
	TimeRangeBegin string `json:"timeRangeBegin,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ErrorGroupStats") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ErrorGroupStats") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ListGroupStatsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListGroupStatsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportErrorEventResponse: Response for reporting an individual error
// event.
// Data may be added to this message in the future.
type ReportErrorEventResponse struct {
	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`
}

// ReportedErrorEvent: An error event which is reported to the Error
// Reporting system.
type ReportedErrorEvent struct {
	// Context: [Optional] A description of the context in which the error
	// occurred.
	Context *ErrorContext `json:"context,omitempty"`

	// EventTime: [Optional] Time when the event occurred.
	// If not provided, the time when the event was received by the
	// Error Reporting system will be used.
	EventTime string `json:"eventTime,omitempty"`

	// Message: [Required] A message describing the error. The message can
	// contain an
	// exception stack in one of the supported programming languages and
	// formats.
	// In that case, the message is parsed and detailed exception
	// information
	// is returned when retrieving the error event again.
	Message string `json:"message,omitempty"`

	// ServiceContext: [Required] The service context in which this error
	// has occurred.
	ServiceContext *ServiceContext `json:"serviceContext,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Context") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Context") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportedErrorEvent) MarshalJSON() ([]byte, error) {
	type noMethod ReportedErrorEvent
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ServiceContext: Describes a running service that sends errors.
// Its version changes over time and multiple versions can run in
// parallel.
type ServiceContext struct {
	// Service: An identifier of the service, such as the name of
	// the
	// executable, job, or Google App Engine service name. This field is
	// expected
	// to have a low number of values that are relatively stable over time,
	// as
	// opposed to `version`, which can be changed whenever new code is
	// deployed.
	//
	// Contains the service name for error reports extracted from Google
	// App Engine logs or `default` if the App Engine default service is
	// used.
	Service string `json:"service,omitempty"`

	// Version: Represents the source code version that the developer
	// provided,
	// which could represent a version label or a Git SHA-1 hash, for
	// example.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Service") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Service") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ServiceContext) MarshalJSON() ([]byte, error) {
	type noMethod ServiceContext
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceLocation: Indicates a location in the source code of the
// service for which
// errors are reported.
// This data should be provided by the application when reporting an
// error,
// unless the error report has been generated automatically from Google
// App
// Engine logs. All fields are optional.
type SourceLocation struct {
	// FilePath: The source code filename, which can include a truncated
	// relative
	// path, or a full path from a production machine.
	FilePath string `json:"filePath,omitempty"`

	// FunctionName: Human-readable name of a function or method.
	// The value can include optional context like the class or package
	// name.
	// For example, `my.package.MyClass.method` in case of Java.
	FunctionName string `json:"functionName,omitempty"`

	// LineNumber: 1-based. 0 indicates that the line number is unknown.
	LineNumber int64 `json:"lineNumber,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FilePath") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FilePath") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceLocation) MarshalJSON() ([]byte, error) {
	type noMethod SourceLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TimedCount: The number of errors in a given time period.
// All numbers are approximate since the error events are sampled
// before counting them.
type TimedCount struct {
	// Count: Approximate number of occurrences in the given time period.
	Count int64 `json:"count,omitempty,string"`

	// EndTime: End of the time period to which `count` refers (excluded).
	EndTime string `json:"endTime,omitempty"`

	// StartTime: Start of the time period to which `count` refers
	// (included).
	StartTime string `json:"startTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Count") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Count") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TimedCount) MarshalJSON() ([]byte, error) {
	type noMethod TimedCount
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TrackingIssue: Information related to tracking the progress on
// resolving the error.
type TrackingIssue struct {
	// Url: A URL pointing to a related entry in an issue tracking
	// system.
	// Example: https://github.com/user/project/issues/4
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Url") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Url") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TrackingIssue) MarshalJSON() ([]byte, error) {
	type noMethod TrackingIssue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "clouderrorreporting.projects.deleteEvents":

type ProjectsDeleteEventsCall struct {
	s           *Service
	projectName string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// DeleteEvents: Deletes all error events of a given project.
func (r *ProjectsService) DeleteEvents(projectName string) *ProjectsDeleteEventsCall {
	c := &ProjectsDeleteEventsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsDeleteEventsCall) Fields(s ...googleapi.Field) *ProjectsDeleteEventsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsDeleteEventsCall) Context(ctx context.Context) *ProjectsDeleteEventsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsDeleteEventsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsDeleteEventsCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+projectName}/events")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "clouderrorreporting.projects.deleteEvents" call.
// Exactly one of *DeleteEventsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *DeleteEventsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsDeleteEventsCall) Do(opts ...googleapi.CallOption) (*DeleteEventsResponse, error) {
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
	ret := &DeleteEventsResponse{
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
	//   "description": "Deletes all error events of a given project.",
	//   "flatPath": "v1beta1/projects/{projectsId}/events",
	//   "httpMethod": "DELETE",
	//   "id": "clouderrorreporting.projects.deleteEvents",
	//   "parameterOrder": [
	//     "projectName"
	//   ],
	//   "parameters": {
	//     "projectName": {
	//       "description": "[Required] The resource name of the Google Cloud Platform project. Written\nas `projects/` plus the\n[Google Cloud Platform project ID](https://support.google.com/cloud/answer/6158840).\nExample: `projects/my-project-123`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+projectName}/events",
	//   "response": {
	//     "$ref": "DeleteEventsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "clouderrorreporting.projects.events.list":

type ProjectsEventsListCall struct {
	s            *Service
	projectName  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the specified events.
func (r *ProjectsEventsService) List(projectName string) *ProjectsEventsListCall {
	c := &ProjectsEventsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	return c
}

// GroupId sets the optional parameter "groupId": [Required] The group
// for which events shall be returned.
func (c *ProjectsEventsListCall) GroupId(groupId string) *ProjectsEventsListCall {
	c.urlParams_.Set("groupId", groupId)
	return c
}

// PageSize sets the optional parameter "pageSize": [Optional] The
// maximum number of results to return per response.
func (c *ProjectsEventsListCall) PageSize(pageSize int64) *ProjectsEventsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": [Optional] A
// `next_page_token` provided by a previous response.
func (c *ProjectsEventsListCall) PageToken(pageToken string) *ProjectsEventsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ServiceFilterService sets the optional parameter
// "serviceFilter.service": [Optional] The exact value to match
// against
// [`ServiceContext.service`](/error-reporting/reference/rest/v1b
// eta1/ServiceContext#FIELDS.service).
func (c *ProjectsEventsListCall) ServiceFilterService(serviceFilterService string) *ProjectsEventsListCall {
	c.urlParams_.Set("serviceFilter.service", serviceFilterService)
	return c
}

// ServiceFilterVersion sets the optional parameter
// "serviceFilter.version": [Optional] The exact value to match
// against
// [`ServiceContext.version`](/error-reporting/reference/rest/v1b
// eta1/ServiceContext#FIELDS.version).
func (c *ProjectsEventsListCall) ServiceFilterVersion(serviceFilterVersion string) *ProjectsEventsListCall {
	c.urlParams_.Set("serviceFilter.version", serviceFilterVersion)
	return c
}

// TimeRangePeriod sets the optional parameter "timeRange.period":
// Restricts the query to the specified time range.
//
// Possible values:
//   "PERIOD_UNSPECIFIED"
//   "PERIOD_1_HOUR"
//   "PERIOD_6_HOURS"
//   "PERIOD_1_DAY"
//   "PERIOD_1_WEEK"
//   "PERIOD_30_DAYS"
func (c *ProjectsEventsListCall) TimeRangePeriod(timeRangePeriod string) *ProjectsEventsListCall {
	c.urlParams_.Set("timeRange.period", timeRangePeriod)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsEventsListCall) Fields(s ...googleapi.Field) *ProjectsEventsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsEventsListCall) IfNoneMatch(entityTag string) *ProjectsEventsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsEventsListCall) Context(ctx context.Context) *ProjectsEventsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsEventsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsEventsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+projectName}/events")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "clouderrorreporting.projects.events.list" call.
// Exactly one of *ListEventsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListEventsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsEventsListCall) Do(opts ...googleapi.CallOption) (*ListEventsResponse, error) {
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
	ret := &ListEventsResponse{
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
	//   "description": "Lists the specified events.",
	//   "flatPath": "v1beta1/projects/{projectsId}/events",
	//   "httpMethod": "GET",
	//   "id": "clouderrorreporting.projects.events.list",
	//   "parameterOrder": [
	//     "projectName"
	//   ],
	//   "parameters": {
	//     "groupId": {
	//       "description": "[Required] The group for which events shall be returned.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "[Optional] The maximum number of results to return per response.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "[Optional] A `next_page_token` provided by a previous response.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "[Required] The resource name of the Google Cloud Platform project. Written\nas `projects/` plus the\n[Google Cloud Platform project ID](https://support.google.com/cloud/answer/6158840).\nExample: `projects/my-project-123`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "serviceFilter.service": {
	//       "description": "[Optional] The exact value to match against\n[`ServiceContext.service`](/error-reporting/reference/rest/v1beta1/ServiceContext#FIELDS.service).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "serviceFilter.version": {
	//       "description": "[Optional] The exact value to match against\n[`ServiceContext.version`](/error-reporting/reference/rest/v1beta1/ServiceContext#FIELDS.version).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeRange.period": {
	//       "description": "Restricts the query to the specified time range.",
	//       "enum": [
	//         "PERIOD_UNSPECIFIED",
	//         "PERIOD_1_HOUR",
	//         "PERIOD_6_HOURS",
	//         "PERIOD_1_DAY",
	//         "PERIOD_1_WEEK",
	//         "PERIOD_30_DAYS"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+projectName}/events",
	//   "response": {
	//     "$ref": "ListEventsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsEventsListCall) Pages(ctx context.Context, f func(*ListEventsResponse) error) error {
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

// method id "clouderrorreporting.projects.events.report":

type ProjectsEventsReportCall struct {
	s                  *Service
	projectName        string
	reportederrorevent *ReportedErrorEvent
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Report: Report an individual error event.
//
// This endpoint accepts <strong>either</strong> an OAuth
// token,
// <strong>or</strong> an
// <a href="https://support.google.com/cloud/answer/6158862">API
// key</a>
// for authentication. To use an API key, append it to the URL as the
// value of
// a `key` parameter. For example:
// <pre>POST
// https://clouderrorreporting.googleapis.com/v1beta1/projects/example-project/events:report?key=123ABC456</pre>
func (r *ProjectsEventsService) Report(projectName string, reportederrorevent *ReportedErrorEvent) *ProjectsEventsReportCall {
	c := &ProjectsEventsReportCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	c.reportederrorevent = reportederrorevent
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsEventsReportCall) Fields(s ...googleapi.Field) *ProjectsEventsReportCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsEventsReportCall) Context(ctx context.Context) *ProjectsEventsReportCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsEventsReportCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsEventsReportCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.reportederrorevent)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+projectName}/events:report")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "clouderrorreporting.projects.events.report" call.
// Exactly one of *ReportErrorEventResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ReportErrorEventResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsEventsReportCall) Do(opts ...googleapi.CallOption) (*ReportErrorEventResponse, error) {
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
	ret := &ReportErrorEventResponse{
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
	//   "description": "Report an individual error event.\n\nThis endpoint accepts \u003cstrong\u003eeither\u003c/strong\u003e an OAuth token,\n\u003cstrong\u003eor\u003c/strong\u003e an\n\u003ca href=\"https://support.google.com/cloud/answer/6158862\"\u003eAPI key\u003c/a\u003e\nfor authentication. To use an API key, append it to the URL as the value of\na `key` parameter. For example:\n\u003cpre\u003ePOST https://clouderrorreporting.googleapis.com/v1beta1/projects/example-project/events:report?key=123ABC456\u003c/pre\u003e",
	//   "flatPath": "v1beta1/projects/{projectsId}/events:report",
	//   "httpMethod": "POST",
	//   "id": "clouderrorreporting.projects.events.report",
	//   "parameterOrder": [
	//     "projectName"
	//   ],
	//   "parameters": {
	//     "projectName": {
	//       "description": "[Required] The resource name of the Google Cloud Platform project. Written\nas `projects/` plus the\n[Google Cloud Platform project ID](https://support.google.com/cloud/answer/6158840).\nExample: `projects/my-project-123`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+projectName}/events:report",
	//   "request": {
	//     "$ref": "ReportedErrorEvent"
	//   },
	//   "response": {
	//     "$ref": "ReportErrorEventResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "clouderrorreporting.projects.groupStats.list":

type ProjectsGroupStatsListCall struct {
	s            *Service
	projectName  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the specified groups.
func (r *ProjectsGroupStatsService) List(projectName string) *ProjectsGroupStatsListCall {
	c := &ProjectsGroupStatsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectName = projectName
	return c
}

// Alignment sets the optional parameter "alignment": [Optional] The
// alignment of the timed counts to be returned.
// Default is `ALIGNMENT_EQUAL_AT_END`.
//
// Possible values:
//   "ERROR_COUNT_ALIGNMENT_UNSPECIFIED"
//   "ALIGNMENT_EQUAL_ROUNDED"
//   "ALIGNMENT_EQUAL_AT_END"
func (c *ProjectsGroupStatsListCall) Alignment(alignment string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("alignment", alignment)
	return c
}

// AlignmentTime sets the optional parameter "alignmentTime": [Optional]
// Time where the timed counts shall be aligned if rounded
// alignment is chosen. Default is 00:00 UTC.
func (c *ProjectsGroupStatsListCall) AlignmentTime(alignmentTime string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("alignmentTime", alignmentTime)
	return c
}

// GroupId sets the optional parameter "groupId": [Optional] List all
// <code>ErrorGroupStats</code> with these IDs.
func (c *ProjectsGroupStatsListCall) GroupId(groupId ...string) *ProjectsGroupStatsListCall {
	c.urlParams_.SetMulti("groupId", append([]string{}, groupId...))
	return c
}

// Order sets the optional parameter "order": [Optional] The sort order
// in which the results are returned.
// Default is `COUNT_DESC`.
//
// Possible values:
//   "GROUP_ORDER_UNSPECIFIED"
//   "COUNT_DESC"
//   "LAST_SEEN_DESC"
//   "CREATED_DESC"
//   "AFFECTED_USERS_DESC"
func (c *ProjectsGroupStatsListCall) Order(order string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("order", order)
	return c
}

// PageSize sets the optional parameter "pageSize": [Optional] The
// maximum number of results to return per response.
// Default is 20.
func (c *ProjectsGroupStatsListCall) PageSize(pageSize int64) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": [Optional] A
// `next_page_token` provided by a previous response. To view
// additional results, pass this token along with the identical
// query
// parameters as the first request.
func (c *ProjectsGroupStatsListCall) PageToken(pageToken string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ServiceFilterService sets the optional parameter
// "serviceFilter.service": [Optional] The exact value to match
// against
// [`ServiceContext.service`](/error-reporting/reference/rest/v1b
// eta1/ServiceContext#FIELDS.service).
func (c *ProjectsGroupStatsListCall) ServiceFilterService(serviceFilterService string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("serviceFilter.service", serviceFilterService)
	return c
}

// ServiceFilterVersion sets the optional parameter
// "serviceFilter.version": [Optional] The exact value to match
// against
// [`ServiceContext.version`](/error-reporting/reference/rest/v1b
// eta1/ServiceContext#FIELDS.version).
func (c *ProjectsGroupStatsListCall) ServiceFilterVersion(serviceFilterVersion string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("serviceFilter.version", serviceFilterVersion)
	return c
}

// TimeRangePeriod sets the optional parameter "timeRange.period":
// Restricts the query to the specified time range.
//
// Possible values:
//   "PERIOD_UNSPECIFIED"
//   "PERIOD_1_HOUR"
//   "PERIOD_6_HOURS"
//   "PERIOD_1_DAY"
//   "PERIOD_1_WEEK"
//   "PERIOD_30_DAYS"
func (c *ProjectsGroupStatsListCall) TimeRangePeriod(timeRangePeriod string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("timeRange.period", timeRangePeriod)
	return c
}

// TimedCountDuration sets the optional parameter "timedCountDuration":
// [Optional] The preferred duration for a single returned
// `TimedCount`.
// If not set, no timed counts are returned.
func (c *ProjectsGroupStatsListCall) TimedCountDuration(timedCountDuration string) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("timedCountDuration", timedCountDuration)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsGroupStatsListCall) Fields(s ...googleapi.Field) *ProjectsGroupStatsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsGroupStatsListCall) IfNoneMatch(entityTag string) *ProjectsGroupStatsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsGroupStatsListCall) Context(ctx context.Context) *ProjectsGroupStatsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsGroupStatsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsGroupStatsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+projectName}/groupStats")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectName": c.projectName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "clouderrorreporting.projects.groupStats.list" call.
// Exactly one of *ListGroupStatsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListGroupStatsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsGroupStatsListCall) Do(opts ...googleapi.CallOption) (*ListGroupStatsResponse, error) {
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
	ret := &ListGroupStatsResponse{
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
	//   "description": "Lists the specified groups.",
	//   "flatPath": "v1beta1/projects/{projectsId}/groupStats",
	//   "httpMethod": "GET",
	//   "id": "clouderrorreporting.projects.groupStats.list",
	//   "parameterOrder": [
	//     "projectName"
	//   ],
	//   "parameters": {
	//     "alignment": {
	//       "description": "[Optional] The alignment of the timed counts to be returned.\nDefault is `ALIGNMENT_EQUAL_AT_END`.",
	//       "enum": [
	//         "ERROR_COUNT_ALIGNMENT_UNSPECIFIED",
	//         "ALIGNMENT_EQUAL_ROUNDED",
	//         "ALIGNMENT_EQUAL_AT_END"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "alignmentTime": {
	//       "description": "[Optional] Time where the timed counts shall be aligned if rounded\nalignment is chosen. Default is 00:00 UTC.",
	//       "format": "google-datetime",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "groupId": {
	//       "description": "[Optional] List all \u003ccode\u003eErrorGroupStats\u003c/code\u003e with these IDs.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "order": {
	//       "description": "[Optional] The sort order in which the results are returned.\nDefault is `COUNT_DESC`.",
	//       "enum": [
	//         "GROUP_ORDER_UNSPECIFIED",
	//         "COUNT_DESC",
	//         "LAST_SEEN_DESC",
	//         "CREATED_DESC",
	//         "AFFECTED_USERS_DESC"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "[Optional] The maximum number of results to return per response.\nDefault is 20.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "[Optional] A `next_page_token` provided by a previous response. To view\nadditional results, pass this token along with the identical query\nparameters as the first request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectName": {
	//       "description": "[Required] The resource name of the Google Cloud Platform project. Written\nas \u003ccode\u003eprojects/\u003c/code\u003e plus the\n\u003ca href=\"https://support.google.com/cloud/answer/6158840\"\u003eGoogle Cloud\nPlatform project ID\u003c/a\u003e.\n\nExample: \u003ccode\u003eprojects/my-project-123\u003c/code\u003e.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "serviceFilter.service": {
	//       "description": "[Optional] The exact value to match against\n[`ServiceContext.service`](/error-reporting/reference/rest/v1beta1/ServiceContext#FIELDS.service).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "serviceFilter.version": {
	//       "description": "[Optional] The exact value to match against\n[`ServiceContext.version`](/error-reporting/reference/rest/v1beta1/ServiceContext#FIELDS.version).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeRange.period": {
	//       "description": "Restricts the query to the specified time range.",
	//       "enum": [
	//         "PERIOD_UNSPECIFIED",
	//         "PERIOD_1_HOUR",
	//         "PERIOD_6_HOURS",
	//         "PERIOD_1_DAY",
	//         "PERIOD_1_WEEK",
	//         "PERIOD_30_DAYS"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timedCountDuration": {
	//       "description": "[Optional] The preferred duration for a single returned `TimedCount`.\nIf not set, no timed counts are returned.",
	//       "format": "google-duration",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+projectName}/groupStats",
	//   "response": {
	//     "$ref": "ListGroupStatsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsGroupStatsListCall) Pages(ctx context.Context, f func(*ListGroupStatsResponse) error) error {
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

// method id "clouderrorreporting.projects.groups.get":

type ProjectsGroupsGetCall struct {
	s            *Service
	groupName    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Get the specified group.
func (r *ProjectsGroupsService) Get(groupName string) *ProjectsGroupsGetCall {
	c := &ProjectsGroupsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupName = groupName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsGroupsGetCall) Fields(s ...googleapi.Field) *ProjectsGroupsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsGroupsGetCall) IfNoneMatch(entityTag string) *ProjectsGroupsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsGroupsGetCall) Context(ctx context.Context) *ProjectsGroupsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsGroupsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsGroupsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+groupName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupName": c.groupName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "clouderrorreporting.projects.groups.get" call.
// Exactly one of *ErrorGroup or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ErrorGroup.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsGroupsGetCall) Do(opts ...googleapi.CallOption) (*ErrorGroup, error) {
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
	ret := &ErrorGroup{
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
	//   "description": "Get the specified group.",
	//   "flatPath": "v1beta1/projects/{projectsId}/groups/{groupsId}",
	//   "httpMethod": "GET",
	//   "id": "clouderrorreporting.projects.groups.get",
	//   "parameterOrder": [
	//     "groupName"
	//   ],
	//   "parameters": {
	//     "groupName": {
	//       "description": "[Required] The group resource name. Written as\n\u003ccode\u003eprojects/\u003cvar\u003eprojectID\u003c/var\u003e/groups/\u003cvar\u003egroup_name\u003c/var\u003e\u003c/code\u003e.\nCall\n\u003ca href=\"/error-reporting/reference/rest/v1beta1/projects.groupStats/list\"\u003e\n\u003ccode\u003egroupStats.list\u003c/code\u003e\u003c/a\u003e to return a list of groups belonging to\nthis project.\n\nExample: \u003ccode\u003eprojects/my-project-123/groups/my-group\u003c/code\u003e",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/groups/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+groupName}",
	//   "response": {
	//     "$ref": "ErrorGroup"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "clouderrorreporting.projects.groups.update":

type ProjectsGroupsUpdateCall struct {
	s          *Service
	name       string
	errorgroup *ErrorGroup
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Replace the data for the specified group.
// Fails if the group does not exist.
func (r *ProjectsGroupsService) Update(name string, errorgroup *ErrorGroup) *ProjectsGroupsUpdateCall {
	c := &ProjectsGroupsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.errorgroup = errorgroup
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsGroupsUpdateCall) Fields(s ...googleapi.Field) *ProjectsGroupsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsGroupsUpdateCall) Context(ctx context.Context) *ProjectsGroupsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsGroupsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsGroupsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.errorgroup)
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

// Do executes the "clouderrorreporting.projects.groups.update" call.
// Exactly one of *ErrorGroup or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ErrorGroup.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsGroupsUpdateCall) Do(opts ...googleapi.CallOption) (*ErrorGroup, error) {
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
	ret := &ErrorGroup{
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
	//   "description": "Replace the data for the specified group.\nFails if the group does not exist.",
	//   "flatPath": "v1beta1/projects/{projectsId}/groups/{groupsId}",
	//   "httpMethod": "PUT",
	//   "id": "clouderrorreporting.projects.groups.update",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The group resource name.\nExample: \u003ccode\u003eprojects/my-project-123/groups/my-groupid\u003c/code\u003e",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/groups/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "request": {
	//     "$ref": "ErrorGroup"
	//   },
	//   "response": {
	//     "$ref": "ErrorGroup"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

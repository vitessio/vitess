// Package servicecontrol provides access to the Google Service Control API.
//
// See https://cloud.google.com/service-control/
//
// Usage example:
//
//   import "google.golang.org/api/servicecontrol/v1"
//   ...
//   servicecontrolService, err := servicecontrol.New(oauthHttpClient)
package servicecontrol // import "google.golang.org/api/servicecontrol/v1"

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

const apiId = "servicecontrol:v1"
const apiName = "servicecontrol"
const apiVersion = "v1"
const basePath = "https://servicecontrol.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// Manage your Google Service Control data
	ServicecontrolScope = "https://www.googleapis.com/auth/servicecontrol"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Services = NewServicesService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Services *ServicesService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewServicesService(s *Service) *ServicesService {
	rs := &ServicesService{s: s}
	return rs
}

type ServicesService struct {
	s *Service
}

// CheckError: Defines the errors to be returned
// in
// google.api.servicecontrol.v1.CheckResponse.check_errors.
type CheckError struct {
	// Code: The error code.
	//
	// Possible values:
	//   "ERROR_CODE_UNSPECIFIED" - This is never used in `CheckResponse`.
	//   "NOT_FOUND" - The consumer's project id was not found.
	// Same as google.rpc.Code.NOT_FOUND.
	//   "PERMISSION_DENIED" - The consumer doesn't have access to the
	// specified resource.
	// Same as google.rpc.Code.PERMISSION_DENIED.
	//   "RESOURCE_EXHAUSTED" - Quota check failed. Same as
	// google.rpc.Code.RESOURCE_EXHAUSTED.
	//   "SERVICE_NOT_ACTIVATED" - The consumer hasn't activated the
	// service.
	//   "BILLING_DISABLED" - The consumer cannot access the service because
	// billing is disabled.
	//   "PROJECT_DELETED" - The consumer's project has been marked as
	// deleted (soft deletion).
	//   "PROJECT_INVALID" - The consumer's project number or id does not
	// represent a valid project.
	//   "IP_ADDRESS_BLOCKED" - The IP address of the consumer is invalid
	// for the specific consumer
	// project.
	//   "REFERER_BLOCKED" - The referer address of the consumer request is
	// invalid for the specific
	// consumer project.
	//   "CLIENT_APP_BLOCKED" - The client application of the consumer
	// request is invalid for the
	// specific consumer project.
	//   "API_KEY_INVALID" - The consumer's API key is invalid.
	//   "API_KEY_EXPIRED" - The consumer's API Key has expired.
	//   "API_KEY_NOT_FOUND" - The consumer's API Key was not found in
	// config record.
	//   "NAMESPACE_LOOKUP_UNAVAILABLE" - The backend server for looking up
	// project id/number is unavailable.
	//   "SERVICE_STATUS_UNAVAILABLE" - The backend server for checking
	// service status is unavailable.
	//   "BILLING_STATUS_UNAVAILABLE" - The backend server for checking
	// billing status is unavailable.
	Code string `json:"code,omitempty"`

	// Detail: Free-form text providing details on the error cause of the
	// error.
	Detail string `json:"detail,omitempty"`

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

func (s *CheckError) MarshalJSON() ([]byte, error) {
	type noMethod CheckError
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CheckRequest: Request message for the Check method.
type CheckRequest struct {
	// Operation: The operation to be checked.
	Operation *Operation `json:"operation,omitempty"`

	// ServiceConfigId: Specifies which version of service configuration
	// should be used to process
	// the request.
	//
	// If unspecified or no matching version can be found, the
	// latest one will be used.
	ServiceConfigId string `json:"serviceConfigId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Operation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Operation") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CheckRequest) MarshalJSON() ([]byte, error) {
	type noMethod CheckRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CheckResponse: Response message for the Check method.
type CheckResponse struct {
	// CheckErrors: Indicate the decision of the check.
	//
	// If no check errors are present, the service should process the
	// operation.
	// Otherwise the service should use the list of errors to determine
	// the
	// appropriate action.
	CheckErrors []*CheckError `json:"checkErrors,omitempty"`

	// OperationId: The same operation_id value used in the
	// CheckRequest.
	// Used for logging and diagnostics purposes.
	OperationId string `json:"operationId,omitempty"`

	// ServiceConfigId: The actual config id used to process the request.
	ServiceConfigId string `json:"serviceConfigId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CheckErrors") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CheckErrors") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CheckResponse) MarshalJSON() ([]byte, error) {
	type noMethod CheckResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Distribution: Distribution represents a frequency distribution of
// double-valued sample
// points. It contains the size of the population of sample points
// plus
// additional optional information:
//
//   - the arithmetic mean of the samples
//   - the minimum and maximum of the samples
//   - the sum-squared-deviation of the samples, used to compute
// variance
//   - a histogram of the values of the sample points
type Distribution struct {
	// BucketCounts: The number of samples in each histogram bucket.
	// `bucket_counts` are
	// optional. If present, they must sum to the `count` value.
	//
	// The buckets are defined below in `bucket_option`. There are N
	// buckets.
	// `bucket_counts[0]` is the number of samples in the underflow
	// bucket.
	// `bucket_counts[1]` to `bucket_counts[N-1]` are the numbers of
	// samples
	// in each of the finite buckets. And `bucket_counts[N] is the number
	// of samples in the overflow bucket. See the comments of
	// `bucket_option`
	// below for more details.
	//
	// Any suffix of trailing zeros may be omitted.
	BucketCounts googleapi.Int64s `json:"bucketCounts,omitempty"`

	// Count: The total number of samples in the distribution. Must be >= 0.
	Count int64 `json:"count,omitempty,string"`

	// ExplicitBuckets: Buckets with arbitrary user-provided width.
	ExplicitBuckets *ExplicitBuckets `json:"explicitBuckets,omitempty"`

	// ExponentialBuckets: Buckets with exponentially growing width.
	ExponentialBuckets *ExponentialBuckets `json:"exponentialBuckets,omitempty"`

	// LinearBuckets: Buckets with constant width.
	LinearBuckets *LinearBuckets `json:"linearBuckets,omitempty"`

	// Maximum: The maximum of the population of values. Ignored if `count`
	// is zero.
	Maximum float64 `json:"maximum,omitempty"`

	// Mean: The arithmetic mean of the samples in the distribution. If
	// `count` is
	// zero then this field must be zero.
	Mean float64 `json:"mean,omitempty"`

	// Minimum: The minimum of the population of values. Ignored if `count`
	// is zero.
	Minimum float64 `json:"minimum,omitempty"`

	// SumOfSquaredDeviation: The sum of squared deviations from the mean:
	//   Sum[i=1..count]((x_i - mean)^2)
	// where each x_i is a sample values. If `count` is zero then this
	// field
	// must be zero, otherwise validation of the request fails.
	SumOfSquaredDeviation float64 `json:"sumOfSquaredDeviation,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BucketCounts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BucketCounts") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Distribution) MarshalJSON() ([]byte, error) {
	type noMethod Distribution
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ExplicitBuckets: Describing buckets with arbitrary user-provided
// width.
type ExplicitBuckets struct {
	// Bounds: 'bound' is a list of strictly increasing boundaries
	// between
	// buckets. Note that a list of length N-1 defines N buckets because
	// of fenceposting. See comments on `bucket_options` for details.
	//
	// The i'th finite bucket covers the interval
	//   [bound[i-1], bound[i])
	// where i ranges from 1 to bound_size() - 1. Note that there are
	// no
	// finite buckets at all if 'bound' only contains a single element;
	// in
	// that special case the single bound defines the boundary between
	// the
	// underflow and overflow buckets.
	//
	// bucket number                   lower bound    upper bound
	//  i == 0 (underflow)              -inf           bound[i]
	//  0 < i < bound_size()            bound[i-1]     bound[i]
	//  i == bound_size() (overflow)    bound[i-1]     +inf
	Bounds []float64 `json:"bounds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Bounds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Bounds") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ExplicitBuckets) MarshalJSON() ([]byte, error) {
	type noMethod ExplicitBuckets
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ExponentialBuckets: Describing buckets with exponentially growing
// width.
type ExponentialBuckets struct {
	// GrowthFactor: The i'th exponential bucket covers the interval
	//   [scale * growth_factor^(i-1), scale * growth_factor^i)
	// where i ranges from 1 to num_finite_buckets inclusive.
	// Must be larger than 1.0.
	GrowthFactor float64 `json:"growthFactor,omitempty"`

	// NumFiniteBuckets: The number of finite buckets. With the underflow
	// and overflow buckets,
	// the total number of buckets is `num_finite_buckets` + 2.
	// See comments on `bucket_options` for details.
	NumFiniteBuckets int64 `json:"numFiniteBuckets,omitempty"`

	// Scale: The i'th exponential bucket covers the interval
	//   [scale * growth_factor^(i-1), scale * growth_factor^i)
	// where i ranges from 1 to num_finite_buckets inclusive.
	// Must be > 0.
	Scale float64 `json:"scale,omitempty"`

	// ForceSendFields is a list of field names (e.g. "GrowthFactor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GrowthFactor") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ExponentialBuckets) MarshalJSON() ([]byte, error) {
	type noMethod ExponentialBuckets
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LinearBuckets: Describing buckets with constant width.
type LinearBuckets struct {
	// NumFiniteBuckets: The number of finite buckets. With the underflow
	// and overflow buckets,
	// the total number of buckets is `num_finite_buckets` + 2.
	// See comments on `bucket_options` for details.
	NumFiniteBuckets int64 `json:"numFiniteBuckets,omitempty"`

	// Offset: The i'th linear bucket covers the interval
	//   [offset + (i-1) * width, offset + i * width)
	// where i ranges from 1 to num_finite_buckets, inclusive.
	Offset float64 `json:"offset,omitempty"`

	// Width: The i'th linear bucket covers the interval
	//   [offset + (i-1) * width, offset + i * width)
	// where i ranges from 1 to num_finite_buckets, inclusive.
	// Must be strictly positive.
	Width float64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "NumFiniteBuckets") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NumFiniteBuckets") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *LinearBuckets) MarshalJSON() ([]byte, error) {
	type noMethod LinearBuckets
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LogEntry: An individual log entry.
type LogEntry struct {
	// InsertId: A unique ID for the log entry used for deduplication. If
	// omitted,
	// the implementation will generate one based on operation_id.
	InsertId string `json:"insertId,omitempty"`

	// Labels: A set of user-defined (key, value) data that provides
	// additional
	// information about the log entry.
	Labels map[string]string `json:"labels,omitempty"`

	// Name: Required. The log to which this log entry belongs. Examples:
	// "syslog",
	// "book_log".
	Name string `json:"name,omitempty"`

	// ProtoPayload: The log entry payload, represented as a protocol buffer
	// that is
	// expressed as a JSON object. You can only pass `protoPayload`
	// values that belong to a set of approved types.
	ProtoPayload googleapi.RawMessage `json:"protoPayload,omitempty"`

	// Severity: The severity of the log entry. The default value
	// is
	// `LogSeverity.DEFAULT`.
	//
	// Possible values:
	//   "DEFAULT" - (0) The log entry has no assigned severity level.
	//   "DEBUG" - (100) Debug or trace information.
	//   "INFO" - (200) Routine information, such as ongoing status or
	// performance.
	//   "NOTICE" - (300) Normal but significant events, such as start up,
	// shut down, or
	// a configuration change.
	//   "WARNING" - (400) Warning events might cause problems.
	//   "ERROR" - (500) Error events are likely to cause problems.
	//   "CRITICAL" - (600) Critical events cause more severe problems or
	// outages.
	//   "ALERT" - (700) A person must take an action immediately.
	//   "EMERGENCY" - (800) One or more systems are unusable.
	Severity string `json:"severity,omitempty"`

	// StructPayload: The log entry payload, represented as a structure
	// that
	// is expressed as a JSON object.
	StructPayload googleapi.RawMessage `json:"structPayload,omitempty"`

	// TextPayload: The log entry payload, represented as a Unicode string
	// (UTF-8).
	TextPayload string `json:"textPayload,omitempty"`

	// Timestamp: The time the event described by the log entry occurred.
	// If
	// omitted, defaults to operation start time.
	Timestamp string `json:"timestamp,omitempty"`

	// ForceSendFields is a list of field names (e.g. "InsertId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "InsertId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LogEntry) MarshalJSON() ([]byte, error) {
	type noMethod LogEntry
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MetricValue: Represents a single metric value.
type MetricValue struct {
	// BoolValue: A boolean value.
	BoolValue *bool `json:"boolValue,omitempty"`

	// DistributionValue: A distribution value.
	DistributionValue *Distribution `json:"distributionValue,omitempty"`

	// DoubleValue: A double precision floating point value.
	DoubleValue *float64 `json:"doubleValue,omitempty"`

	// EndTime: The end of the time period over which this metric value's
	// measurement
	// applies.
	EndTime string `json:"endTime,omitempty"`

	// Int64Value: A signed 64-bit integer value.
	Int64Value *int64 `json:"int64Value,omitempty,string"`

	// Labels: The labels describing the metric value.
	// See comments on google.api.servicecontrol.v1.Operation.labels for
	// the overriding relationship.
	Labels map[string]string `json:"labels,omitempty"`

	// StartTime: The start of the time period over which this metric
	// value's measurement
	// applies. The time period has different semantics for different
	// metric
	// types (cumulative, delta, and gauge). See the metric
	// definition
	// documentation in the service configuration for details.
	StartTime string `json:"startTime,omitempty"`

	// StringValue: A text string value.
	StringValue *string `json:"stringValue,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BoolValue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BoolValue") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MetricValue) MarshalJSON() ([]byte, error) {
	type noMethod MetricValue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MetricValueSet: Represents a set of metric values in the same
// metric.
// Each metric value in the set should have a unique combination of
// start time,
// end time, and label values.
type MetricValueSet struct {
	// MetricName: The metric name defined in the service configuration.
	MetricName string `json:"metricName,omitempty"`

	// MetricValues: The values in this metric.
	MetricValues []*MetricValue `json:"metricValues,omitempty"`

	// ForceSendFields is a list of field names (e.g. "MetricName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MetricName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MetricValueSet) MarshalJSON() ([]byte, error) {
	type noMethod MetricValueSet
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Operation: Represents information regarding an operation.
type Operation struct {
	// ConsumerId: Identity of the consumer who is using the service.
	// This field should be filled in for the operations initiated by
	// a
	// consumer, but not for service-initiated operations that are
	// not related to a specific consumer.
	//
	// This can be in one of the following formats:
	//   project:<project_id>,
	//   project_number:<project_number>,
	//   api_key:<api_key>.
	ConsumerId string `json:"consumerId,omitempty"`

	// EndTime: End time of the operation.
	// Required when the operation is used in ServiceController.Report,
	// but optional when the operation is used in ServiceController.Check.
	EndTime string `json:"endTime,omitempty"`

	// Importance: DO NOT USE. This is an experimental field.
	//
	// Possible values:
	//   "LOW" - The API implementation may cache and aggregate the
	// data.
	// The data may be lost when rare and unexpected system failures occur.
	//   "HIGH" - The API implementation doesn't cache and aggregate the
	// data.
	// If the method returns successfully, it's guaranteed that the data
	// has
	// been persisted in durable storage.
	Importance string `json:"importance,omitempty"`

	// Labels: Labels describing the operation. Only the following labels
	// are allowed:
	//
	// - Labels describing monitored resources as defined in
	//   the service configuration.
	// - Default labels of metric values. When specified, labels defined in
	// the
	//   metric value override these default.
	// - The following labels defined by Google Cloud Platform:
	//     - `cloud.googleapis.com/location` describing the location where
	// the
	//        operation happened,
	//     - `servicecontrol.googleapis.com/user_agent` describing the user
	// agent
	//        of the API request,
	//     - `servicecontrol.googleapis.com/service_agent` describing the
	// service
	//        used to handle the API request (e.g. ESP),
	//     - `servicecontrol.googleapis.com/platform` describing the
	// platform
	//        where the API is served (e.g. GAE, GCE, GKE).
	Labels map[string]string `json:"labels,omitempty"`

	// LogEntries: Represents information to be logged.
	LogEntries []*LogEntry `json:"logEntries,omitempty"`

	// MetricValueSets: Represents information about this operation. Each
	// MetricValueSet
	// corresponds to a metric defined in the service configuration.
	// The data type used in the MetricValueSet must agree with
	// the data type specified in the metric definition.
	//
	// Within a single operation, it is not allowed to have more than
	// one
	// MetricValue instances that have the same metric names and
	// identical
	// label value combinations. If a request has such duplicated
	// MetricValue
	// instances, the entire request is rejected with
	// an invalid argument error.
	MetricValueSets []*MetricValueSet `json:"metricValueSets,omitempty"`

	// OperationId: Identity of the operation. This must be unique within
	// the scope of the
	// service that generated the operation. If the service calls
	// Check() and Report() on the same operation, the two calls should
	// carry
	// the same id.
	//
	// UUID version 4 is recommended, though not required.
	// In scenarios where an operation is computed from existing
	// information
	// and an idempotent id is desirable for deduplication purpose, UUID
	// version 5
	// is recommended. See RFC 4122 for details.
	OperationId string `json:"operationId,omitempty"`

	// OperationName: Fully qualified name of the operation. Reserved for
	// future use.
	OperationName string `json:"operationName,omitempty"`

	// StartTime: Required. Start time of the operation.
	StartTime string `json:"startTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ConsumerId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ConsumerId") to include in
	// API requests with the JSON null value. By default, fields with empty
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

// ReportError: Represents the processing error of one `Operation` in
// the request.
type ReportError struct {
	// OperationId: The Operation.operation_id value from the request.
	OperationId string `json:"operationId,omitempty"`

	// Status: Details of the error when processing the `Operation`.
	Status *Status `json:"status,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OperationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OperationId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportError) MarshalJSON() ([]byte, error) {
	type noMethod ReportError
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportRequest: Request message for the Report method.
type ReportRequest struct {
	// Operations: Operations to be reported.
	//
	// Typically the service should report one operation per
	// request.
	// Putting multiple operations into a single request is allowed, but
	// should
	// be used only when multiple operations are natually available at the
	// time
	// of the report.
	//
	// If multiple operations are in a single request, the total request
	// size
	// should be no larger than 1MB. See ReportResponse.report_errors
	// for
	// partial failure behavior.
	Operations []*Operation `json:"operations,omitempty"`

	// ServiceConfigId: Specifies which version of service config should be
	// used to process the
	// request.
	//
	// If unspecified or no matching version can be found, the
	// latest one will be used.
	ServiceConfigId string `json:"serviceConfigId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Operations") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Operations") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportRequest) MarshalJSON() ([]byte, error) {
	type noMethod ReportRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportResponse: Response message for the Report method.
type ReportResponse struct {
	// ReportErrors: Partial failures, one for each `Operation` in the
	// request that failed
	// processing. There are three possible combinations of the RPC
	// status:
	//
	// 1. The combination of a successful RPC status and an empty
	// `report_errors`
	//    list indicates a complete success where all `Operations` in the
	//    request are processed successfully.
	// 2. The combination of a successful RPC status and a non-empty
	//    `report_errors` list indicates a partial success where some
	//    `Operations` in the request succeeded. Each
	//    `Operation` that failed processing has a corresponding item
	//    in this list.
	// 3. A failed RPC status indicates a general non-deterministic
	// failure.
	//    When this happens, it's impossible to know which of the
	//    'Operations' in the request succeeded or failed.
	ReportErrors []*ReportError `json:"reportErrors,omitempty"`

	// ServiceConfigId: The actual config id used to process the request.
	ServiceConfigId string `json:"serviceConfigId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ReportErrors") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ReportErrors") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportResponse) MarshalJSON() ([]byte, error) {
	type noMethod ReportResponse
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

// method id "servicecontrol.services.check":

type ServicesCheckCall struct {
	s            *Service
	serviceName  string
	checkrequest *CheckRequest
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Check: Checks an operation with Google Service Control to decide
// whether
// the given operation should proceed. It should be called before
// the
// operation is executed.
//
// If feasible, the client should cache the check results and reuse them
// for
// up to 60s. In case of server errors, the client may rely on the
// cached
// results for longer time.
//
// This method requires the `servicemanagement.services.check`
// permission
// on the specified service. For more information, see
// [Google Cloud IAM](https://cloud.google.com/iam).
func (r *ServicesService) Check(serviceName string, checkrequest *CheckRequest) *ServicesCheckCall {
	c := &ServicesCheckCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.serviceName = serviceName
	c.checkrequest = checkrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ServicesCheckCall) Fields(s ...googleapi.Field) *ServicesCheckCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ServicesCheckCall) Context(ctx context.Context) *ServicesCheckCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ServicesCheckCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ServicesCheckCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.checkrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/services/{serviceName}:check")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"serviceName": c.serviceName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "servicecontrol.services.check" call.
// Exactly one of *CheckResponse or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CheckResponse.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ServicesCheckCall) Do(opts ...googleapi.CallOption) (*CheckResponse, error) {
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
	ret := &CheckResponse{
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
	//   "description": "Checks an operation with Google Service Control to decide whether\nthe given operation should proceed. It should be called before the\noperation is executed.\n\nIf feasible, the client should cache the check results and reuse them for\nup to 60s. In case of server errors, the client may rely on the cached\nresults for longer time.\n\nThis method requires the `servicemanagement.services.check` permission\non the specified service. For more information, see\n[Google Cloud IAM](https://cloud.google.com/iam).",
	//   "flatPath": "v1/services/{serviceName}:check",
	//   "httpMethod": "POST",
	//   "id": "servicecontrol.services.check",
	//   "parameterOrder": [
	//     "serviceName"
	//   ],
	//   "parameters": {
	//     "serviceName": {
	//       "description": "The service name as specified in its service configuration. For example,\n`\"pubsub.googleapis.com\"`.\n\nSee google.api.Service for the definition of a service name.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/services/{serviceName}:check",
	//   "request": {
	//     "$ref": "CheckRequest"
	//   },
	//   "response": {
	//     "$ref": "CheckResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/servicecontrol"
	//   ]
	// }

}

// method id "servicecontrol.services.report":

type ServicesReportCall struct {
	s             *Service
	serviceName   string
	reportrequest *ReportRequest
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// Report: Reports operations to Google Service Control. It should be
// called
// after the operation is completed.
//
// If feasible, the client should aggregate reporting data for up to 5s
// to
// reduce API traffic. Limiting aggregation to 5s is to reduce data
// loss
// during client crashes. Clients should carefully choose the
// aggregation
// window to avoid data loss risk more than 0.01% for business
// and
// compliance reasons.
//
// This method requires the `servicemanagement.services.report`
// permission
// on the specified service. For more information, see
// [Google Cloud IAM](https://cloud.google.com/iam).
func (r *ServicesService) Report(serviceName string, reportrequest *ReportRequest) *ServicesReportCall {
	c := &ServicesReportCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.serviceName = serviceName
	c.reportrequest = reportrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ServicesReportCall) Fields(s ...googleapi.Field) *ServicesReportCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ServicesReportCall) Context(ctx context.Context) *ServicesReportCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ServicesReportCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ServicesReportCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.reportrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/services/{serviceName}:report")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"serviceName": c.serviceName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "servicecontrol.services.report" call.
// Exactly one of *ReportResponse or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ReportResponse.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ServicesReportCall) Do(opts ...googleapi.CallOption) (*ReportResponse, error) {
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
	ret := &ReportResponse{
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
	//   "description": "Reports operations to Google Service Control. It should be called\nafter the operation is completed.\n\nIf feasible, the client should aggregate reporting data for up to 5s to\nreduce API traffic. Limiting aggregation to 5s is to reduce data loss\nduring client crashes. Clients should carefully choose the aggregation\nwindow to avoid data loss risk more than 0.01% for business and\ncompliance reasons.\n\nThis method requires the `servicemanagement.services.report` permission\non the specified service. For more information, see\n[Google Cloud IAM](https://cloud.google.com/iam).",
	//   "flatPath": "v1/services/{serviceName}:report",
	//   "httpMethod": "POST",
	//   "id": "servicecontrol.services.report",
	//   "parameterOrder": [
	//     "serviceName"
	//   ],
	//   "parameters": {
	//     "serviceName": {
	//       "description": "The service name as specified in its service configuration. For example,\n`\"pubsub.googleapis.com\"`.\n\nSee google.api.Service for the definition of a service name.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/services/{serviceName}:report",
	//   "request": {
	//     "$ref": "ReportRequest"
	//   },
	//   "response": {
	//     "$ref": "ReportResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/servicecontrol"
	//   ]
	// }

}

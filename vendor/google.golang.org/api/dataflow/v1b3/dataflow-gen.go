// Package dataflow provides access to the Google Dataflow API.
//
// See https://cloud.google.com/dataflow
//
// Usage example:
//
//   import "google.golang.org/api/dataflow/v1b3"
//   ...
//   dataflowService, err := dataflow.New(oauthHttpClient)
package dataflow // import "google.golang.org/api/dataflow/v1b3"

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

const apiId = "dataflow:v1b3"
const apiName = "dataflow"
const apiVersion = "v1b3"
const basePath = "https://dataflow.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// View your email address
	UserinfoEmailScope = "https://www.googleapis.com/auth/userinfo.email"
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
	rs.Jobs = NewProjectsJobsService(s)
	rs.Locations = NewProjectsLocationsService(s)
	rs.Templates = NewProjectsTemplatesService(s)
	return rs
}

type ProjectsService struct {
	s *Service

	Jobs *ProjectsJobsService

	Locations *ProjectsLocationsService

	Templates *ProjectsTemplatesService
}

func NewProjectsJobsService(s *Service) *ProjectsJobsService {
	rs := &ProjectsJobsService{s: s}
	rs.Debug = NewProjectsJobsDebugService(s)
	rs.Messages = NewProjectsJobsMessagesService(s)
	rs.WorkItems = NewProjectsJobsWorkItemsService(s)
	return rs
}

type ProjectsJobsService struct {
	s *Service

	Debug *ProjectsJobsDebugService

	Messages *ProjectsJobsMessagesService

	WorkItems *ProjectsJobsWorkItemsService
}

func NewProjectsJobsDebugService(s *Service) *ProjectsJobsDebugService {
	rs := &ProjectsJobsDebugService{s: s}
	return rs
}

type ProjectsJobsDebugService struct {
	s *Service
}

func NewProjectsJobsMessagesService(s *Service) *ProjectsJobsMessagesService {
	rs := &ProjectsJobsMessagesService{s: s}
	return rs
}

type ProjectsJobsMessagesService struct {
	s *Service
}

func NewProjectsJobsWorkItemsService(s *Service) *ProjectsJobsWorkItemsService {
	rs := &ProjectsJobsWorkItemsService{s: s}
	return rs
}

type ProjectsJobsWorkItemsService struct {
	s *Service
}

func NewProjectsLocationsService(s *Service) *ProjectsLocationsService {
	rs := &ProjectsLocationsService{s: s}
	rs.Jobs = NewProjectsLocationsJobsService(s)
	return rs
}

type ProjectsLocationsService struct {
	s *Service

	Jobs *ProjectsLocationsJobsService
}

func NewProjectsLocationsJobsService(s *Service) *ProjectsLocationsJobsService {
	rs := &ProjectsLocationsJobsService{s: s}
	rs.Messages = NewProjectsLocationsJobsMessagesService(s)
	rs.WorkItems = NewProjectsLocationsJobsWorkItemsService(s)
	return rs
}

type ProjectsLocationsJobsService struct {
	s *Service

	Messages *ProjectsLocationsJobsMessagesService

	WorkItems *ProjectsLocationsJobsWorkItemsService
}

func NewProjectsLocationsJobsMessagesService(s *Service) *ProjectsLocationsJobsMessagesService {
	rs := &ProjectsLocationsJobsMessagesService{s: s}
	return rs
}

type ProjectsLocationsJobsMessagesService struct {
	s *Service
}

func NewProjectsLocationsJobsWorkItemsService(s *Service) *ProjectsLocationsJobsWorkItemsService {
	rs := &ProjectsLocationsJobsWorkItemsService{s: s}
	return rs
}

type ProjectsLocationsJobsWorkItemsService struct {
	s *Service
}

func NewProjectsTemplatesService(s *Service) *ProjectsTemplatesService {
	rs := &ProjectsTemplatesService{s: s}
	return rs
}

type ProjectsTemplatesService struct {
	s *Service
}

// ApproximateProgress: Obsolete in favor of ApproximateReportedProgress
// and ApproximateSplitRequest.
type ApproximateProgress struct {
	// PercentComplete: Obsolete.
	PercentComplete float64 `json:"percentComplete,omitempty"`

	// Position: Obsolete.
	Position *Position `json:"position,omitempty"`

	// RemainingTime: Obsolete.
	RemainingTime string `json:"remainingTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PercentComplete") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PercentComplete") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ApproximateProgress) MarshalJSON() ([]byte, error) {
	type noMethod ApproximateProgress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ApproximateReportedProgress: A progress measurement of a WorkItem by
// a worker.
type ApproximateReportedProgress struct {
	// ConsumedParallelism: Total amount of parallelism in the portion of
	// input of this task that has already been consumed and is no longer
	// active. In the first two examples above (see remaining_parallelism),
	// the value should be 29 or 2 respectively. The sum of
	// remaining_parallelism and consumed_parallelism should equal the total
	// amount of parallelism in this work item. If specified, must be
	// finite.
	ConsumedParallelism *ReportedParallelism `json:"consumedParallelism,omitempty"`

	// FractionConsumed: Completion as fraction of the input consumed, from
	// 0.0 (beginning, nothing consumed), to 1.0 (end of the input, entire
	// input consumed).
	FractionConsumed float64 `json:"fractionConsumed,omitempty"`

	// Position: A Position within the work to represent a progress.
	Position *Position `json:"position,omitempty"`

	// RemainingParallelism: Total amount of parallelism in the input of
	// this task that remains, (i.e. can be delegated to this task and any
	// new tasks via dynamic splitting). Always at least 1 for non-finished
	// work items and 0 for finished. "Amount of parallelism" refers to how
	// many non-empty parts of the input can be read in parallel. This does
	// not necessarily equal number of records. An input that can be read in
	// parallel down to the individual records is called "perfectly
	// splittable". An example of non-perfectly parallelizable input is a
	// block-compressed file format where a block of records has to be read
	// as a whole, but different blocks can be read in parallel. Examples: *
	// If we are processing record #30 (starting at 1) out of 50 in a
	// perfectly splittable 50-record input, this value should be 21 (20
	// remaining + 1 current). * If we are reading through block 3 in a
	// block-compressed file consisting of 5 blocks, this value should be 3
	// (since blocks 4 and 5 can be processed in parallel by new tasks via
	// dynamic splitting and the current task remains processing block 3). *
	// If we are reading through the last block in a block-compressed file,
	// or reading or processing the last record in a perfectly splittable
	// input, this value should be 1, because apart from the current task,
	// no additional remainder can be split off.
	RemainingParallelism *ReportedParallelism `json:"remainingParallelism,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ConsumedParallelism")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ConsumedParallelism") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ApproximateReportedProgress) MarshalJSON() ([]byte, error) {
	type noMethod ApproximateReportedProgress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ApproximateSplitRequest: A suggestion by the service to the worker to
// dynamically split the WorkItem.
type ApproximateSplitRequest struct {
	// FractionConsumed: A fraction at which to split the work item, from
	// 0.0 (beginning of the input) to 1.0 (end of the input).
	FractionConsumed float64 `json:"fractionConsumed,omitempty"`

	// Position: A Position at which to split the work item.
	Position *Position `json:"position,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FractionConsumed") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FractionConsumed") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ApproximateSplitRequest) MarshalJSON() ([]byte, error) {
	type noMethod ApproximateSplitRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AutoscalingSettings: Settings for WorkerPool autoscaling.
type AutoscalingSettings struct {
	// Algorithm: The algorithm to use for autoscaling.
	//
	// Possible values:
	//   "AUTOSCALING_ALGORITHM_UNKNOWN"
	//   "AUTOSCALING_ALGORITHM_NONE"
	//   "AUTOSCALING_ALGORITHM_BASIC"
	Algorithm string `json:"algorithm,omitempty"`

	// MaxNumWorkers: The maximum number of workers to cap scaling at.
	MaxNumWorkers int64 `json:"maxNumWorkers,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Algorithm") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Algorithm") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AutoscalingSettings) MarshalJSON() ([]byte, error) {
	type noMethod AutoscalingSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ComputationTopology: All configuration data for a particular
// Computation.
type ComputationTopology struct {
	// ComputationId: The ID of the computation.
	ComputationId string `json:"computationId,omitempty"`

	// Inputs: The inputs to the computation.
	Inputs []*StreamLocation `json:"inputs,omitempty"`

	// KeyRanges: The key ranges processed by the computation.
	KeyRanges []*KeyRangeLocation `json:"keyRanges,omitempty"`

	// Outputs: The outputs from the computation.
	Outputs []*StreamLocation `json:"outputs,omitempty"`

	// StateFamilies: The state family values.
	StateFamilies []*StateFamilyConfig `json:"stateFamilies,omitempty"`

	// SystemStageName: The system stage name.
	SystemStageName string `json:"systemStageName,omitempty"`

	// UserStageName: The user stage name.
	UserStageName string `json:"userStageName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComputationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComputationId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ComputationTopology) MarshalJSON() ([]byte, error) {
	type noMethod ComputationTopology
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ConcatPosition: A position that encapsulates an inner position and an
// index for the inner position. A ConcatPosition can be used by a
// reader of a source that encapsulates a set of other sources.
type ConcatPosition struct {
	// Index: Index of the inner source.
	Index int64 `json:"index,omitempty"`

	// Position: Position within the inner source.
	Position *Position `json:"position,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Index") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Index") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ConcatPosition) MarshalJSON() ([]byte, error) {
	type noMethod ConcatPosition
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CounterMetadata: CounterMetadata includes all static non-name
// non-value counter attributes.
type CounterMetadata struct {
	// Description: Human-readable description of the counter semantics.
	Description string `json:"description,omitempty"`

	// Kind: Counter aggregation kind.
	//
	// Possible values:
	//   "INVALID"
	//   "SUM"
	//   "MAX"
	//   "MIN"
	//   "MEAN"
	//   "OR"
	//   "AND"
	//   "SET"
	Kind string `json:"kind,omitempty"`

	// OtherUnits: A string referring to the unit type.
	OtherUnits string `json:"otherUnits,omitempty"`

	// StandardUnits: System defined Units, see above enum.
	//
	// Possible values:
	//   "BYTES"
	//   "BYTES_PER_SEC"
	//   "MILLISECONDS"
	//   "MICROSECONDS"
	//   "NANOSECONDS"
	//   "TIMESTAMP_MSEC"
	//   "TIMESTAMP_USEC"
	//   "TIMESTAMP_NSEC"
	StandardUnits string `json:"standardUnits,omitempty"`

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

func (s *CounterMetadata) MarshalJSON() ([]byte, error) {
	type noMethod CounterMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CounterStructuredName: Identifies a counter within a per-job
// namespace. Counters whose structured names are the same get merged
// into a single value for the job.
type CounterStructuredName struct {
	// ComponentStepName: Name of the optimized step being executed by the
	// workers.
	ComponentStepName string `json:"componentStepName,omitempty"`

	// ExecutionStepName: Name of the stage. An execution step contains
	// multiple component steps.
	ExecutionStepName string `json:"executionStepName,omitempty"`

	// Name: Counter name. Not necessarily globally-unique, but unique
	// within the context of the other fields. Required.
	Name string `json:"name,omitempty"`

	// OriginalStepName: System generated name of the original step in the
	// user's graph, before optimization.
	OriginalStepName string `json:"originalStepName,omitempty"`

	// OtherOrigin: A string containing the origin of the counter.
	OtherOrigin string `json:"otherOrigin,omitempty"`

	// Portion: Portion of this counter, either key or value.
	//
	// Possible values:
	//   "ALL"
	//   "KEY"
	//   "VALUE"
	Portion string `json:"portion,omitempty"`

	// StandardOrigin: One of the standard Origins defined above.
	//
	// Possible values:
	//   "DATAFLOW"
	//   "USER"
	StandardOrigin string `json:"standardOrigin,omitempty"`

	// WorkerId: ID of a particular worker.
	WorkerId string `json:"workerId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComponentStepName")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComponentStepName") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CounterStructuredName) MarshalJSON() ([]byte, error) {
	type noMethod CounterStructuredName
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CounterStructuredNameAndMetadata: A single message which encapsulates
// structured name and metadata for a given counter.
type CounterStructuredNameAndMetadata struct {
	// Metadata: Metadata associated with a counter
	Metadata *CounterMetadata `json:"metadata,omitempty"`

	// Name: Structured name of the counter.
	Name *CounterStructuredName `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Metadata") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Metadata") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CounterStructuredNameAndMetadata) MarshalJSON() ([]byte, error) {
	type noMethod CounterStructuredNameAndMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CounterUpdate: An update to a Counter sent from a worker.
type CounterUpdate struct {
	// Boolean: Boolean value for And, Or.
	Boolean bool `json:"boolean,omitempty"`

	// Cumulative: True if this counter is reported as the total cumulative
	// aggregate value accumulated since the worker started working on this
	// WorkItem. By default this is false, indicating that this counter is
	// reported as a delta.
	Cumulative bool `json:"cumulative,omitempty"`

	// FloatingPoint: Floating point value for Sum, Max, Min.
	FloatingPoint float64 `json:"floatingPoint,omitempty"`

	// FloatingPointList: List of floating point numbers, for Set.
	FloatingPointList *FloatingPointList `json:"floatingPointList,omitempty"`

	// FloatingPointMean: Floating point mean aggregation value for Mean.
	FloatingPointMean *FloatingPointMean `json:"floatingPointMean,omitempty"`

	// Integer: Integer value for Sum, Max, Min.
	Integer *SplitInt64 `json:"integer,omitempty"`

	// IntegerList: List of integers, for Set.
	IntegerList *IntegerList `json:"integerList,omitempty"`

	// IntegerMean: Integer mean aggregation value for Mean.
	IntegerMean *IntegerMean `json:"integerMean,omitempty"`

	// Internal: Value for internally-defined counters used by the Dataflow
	// service.
	Internal interface{} `json:"internal,omitempty"`

	// NameAndKind: Counter name and aggregation type.
	NameAndKind *NameAndKind `json:"nameAndKind,omitempty"`

	// ShortId: The service-generated short identifier for this counter. The
	// short_id -> (name, metadata) mapping is constant for the lifetime of
	// a job.
	ShortId int64 `json:"shortId,omitempty,string"`

	// StringList: List of strings, for Set.
	StringList *StringList `json:"stringList,omitempty"`

	// StructuredNameAndMetadata: Counter structured name and metadata.
	StructuredNameAndMetadata *CounterStructuredNameAndMetadata `json:"structuredNameAndMetadata,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Boolean") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Boolean") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CounterUpdate) MarshalJSON() ([]byte, error) {
	type noMethod CounterUpdate
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateJobFromTemplateRequest: Request to create a Dataflow job.
type CreateJobFromTemplateRequest struct {
	// GcsPath: A path to the serialized JSON representation of the job.
	GcsPath string `json:"gcsPath,omitempty"`

	// JobName: The job name to use for the created job..
	JobName string `json:"jobName,omitempty"`

	// Parameters: Dynamic parameterization of the job's runtime
	// environment.
	Parameters map[string]string `json:"parameters,omitempty"`

	// ForceSendFields is a list of field names (e.g. "GcsPath") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GcsPath") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateJobFromTemplateRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateJobFromTemplateRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CustomSourceLocation: Identifies the location of a custom souce.
type CustomSourceLocation struct {
	// Stateful: Whether this source is stateful.
	Stateful bool `json:"stateful,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Stateful") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Stateful") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CustomSourceLocation) MarshalJSON() ([]byte, error) {
	type noMethod CustomSourceLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DataDiskAssignment: Data disk assignment for a given VM instance.
type DataDiskAssignment struct {
	// DataDisks: Mounted data disks. The order is important a data disk's
	// 0-based index in this list defines which persistent directory the
	// disk is mounted to, for example the list of {
	// "myproject-1014-104817-4c2-harness-0-disk-0" }, {
	// "myproject-1014-104817-4c2-harness-0-disk-1" }.
	DataDisks []string `json:"dataDisks,omitempty"`

	// VmInstance: VM instance name the data disks mounted to, for example
	// "myproject-1014-104817-4c2-harness-0".
	VmInstance string `json:"vmInstance,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DataDisks") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataDisks") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DataDiskAssignment) MarshalJSON() ([]byte, error) {
	type noMethod DataDiskAssignment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DerivedSource: Specification of one of the bundles produced as a
// result of splitting a Source (e.g. when executing a
// SourceSplitRequest, or when splitting an active task using
// WorkItemStatus.dynamic_source_split), relative to the source being
// split.
type DerivedSource struct {
	// DerivationMode: What source to base the produced source on (if any).
	//
	// Possible values:
	//   "SOURCE_DERIVATION_MODE_UNKNOWN"
	//   "SOURCE_DERIVATION_MODE_INDEPENDENT"
	//   "SOURCE_DERIVATION_MODE_CHILD_OF_CURRENT"
	//   "SOURCE_DERIVATION_MODE_SIBLING_OF_CURRENT"
	DerivationMode string `json:"derivationMode,omitempty"`

	// Source: Specification of the source.
	Source *Source `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DerivationMode") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DerivationMode") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DerivedSource) MarshalJSON() ([]byte, error) {
	type noMethod DerivedSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Disk: Describes the data disk used by a workflow job.
type Disk struct {
	// DiskType: Disk storage type, as defined by Google Compute Engine.
	// This must be a disk type appropriate to the project and zone in which
	// the workers will run. If unknown or unspecified, the service will
	// attempt to choose a reasonable default. For example, the standard
	// persistent disk type is a resource name typically ending in
	// "pd-standard". If SSD persistent disks are available, the resource
	// name typically ends with "pd-ssd". The actual valid values are
	// defined the Google Compute Engine API, not by the Dataflow API;
	// consult the Google Compute Engine documentation for more information
	// about determining the set of available disk types for a particular
	// project and zone. Google Compute Engine Disk types are local to a
	// particular project in a particular zone, and so the resource name
	// will typically look something like this:
	// compute.googleapis.com/projects/
	// /zones//diskTypes/pd-standard
	DiskType string `json:"diskType,omitempty"`

	// MountPoint: Directory in a VM where disk is mounted.
	MountPoint string `json:"mountPoint,omitempty"`

	// SizeGb: Size of disk in GB. If zero or unspecified, the service will
	// attempt to choose a reasonable default.
	SizeGb int64 `json:"sizeGb,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DiskType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DiskType") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Disk) MarshalJSON() ([]byte, error) {
	type noMethod Disk
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DynamicSourceSplit: When a task splits using
// WorkItemStatus.dynamic_source_split, this message describes the two
// parts of the split relative to the description of the current task's
// input.
type DynamicSourceSplit struct {
	// Primary: Primary part (continued to be processed by worker).
	// Specified relative to the previously-current source. Becomes current.
	Primary *DerivedSource `json:"primary,omitempty"`

	// Residual: Residual part (returned to the pool of work). Specified
	// relative to the previously-current source.
	Residual *DerivedSource `json:"residual,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Primary") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Primary") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DynamicSourceSplit) MarshalJSON() ([]byte, error) {
	type noMethod DynamicSourceSplit
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Environment: Describes the environment in which a Dataflow Job runs.
type Environment struct {
	// ClusterManagerApiService: The type of cluster manager API to use. If
	// unknown or unspecified, the service will attempt to choose a
	// reasonable default. This should be in the form of the API service
	// name, e.g. "compute.googleapis.com".
	ClusterManagerApiService string `json:"clusterManagerApiService,omitempty"`

	// Dataset: The dataset for the current project where various workflow
	// related tables are stored. The supported resource type is: Google
	// BigQuery: bigquery.googleapis.com/{dataset}
	Dataset string `json:"dataset,omitempty"`

	// Experiments: The list of experiments to enable.
	Experiments []string `json:"experiments,omitempty"`

	// InternalExperiments: Experimental settings.
	InternalExperiments googleapi.RawMessage `json:"internalExperiments,omitempty"`

	// SdkPipelineOptions: The Dataflow SDK pipeline options specified by
	// the user. These options are passed through the service and are used
	// to recreate the SDK pipeline options on the worker in a language
	// agnostic and platform independent way.
	SdkPipelineOptions googleapi.RawMessage `json:"sdkPipelineOptions,omitempty"`

	// ServiceAccountEmail: Identity to run virtual machines as. Defaults to
	// the default account.
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`

	// TempStoragePrefix: The prefix of the resources the system should use
	// for temporary storage. The system will append the suffix
	// "/temp-{JOBNAME} to this resource prefix, where {JOBNAME} is the
	// value of the job_name field. The resulting bucket and object prefix
	// is used as the prefix of the resources used to store temporary data
	// needed during the job execution. NOTE: This will override the value
	// in taskrunner_settings. The supported resource type is: Google Cloud
	// Storage: storage.googleapis.com/{bucket}/{object}
	// bucket.storage.googleapis.com/{object}
	TempStoragePrefix string `json:"tempStoragePrefix,omitempty"`

	// UserAgent: A description of the process that generated the request.
	UserAgent googleapi.RawMessage `json:"userAgent,omitempty"`

	// Version: A structure describing which components and their versions
	// of the service are required in order to run the job.
	Version googleapi.RawMessage `json:"version,omitempty"`

	// WorkerPools: Worker pools. At least one "harness" worker pool must be
	// specified in order for the job to have workers.
	WorkerPools []*WorkerPool `json:"workerPools,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "ClusterManagerApiService") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ClusterManagerApiService")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Environment) MarshalJSON() ([]byte, error) {
	type noMethod Environment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FailedLocation: FailedLocation indicates which location failed to
// respond to a request for data.
type FailedLocation struct {
	// Name: The name of the failed location.
	Name string `json:"name,omitempty"`

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

func (s *FailedLocation) MarshalJSON() ([]byte, error) {
	type noMethod FailedLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FlattenInstruction: An instruction that copies its inputs (zero or
// more) to its (single) output.
type FlattenInstruction struct {
	// Inputs: Describes the inputs to the flatten instruction.
	Inputs []*InstructionInput `json:"inputs,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Inputs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Inputs") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FlattenInstruction) MarshalJSON() ([]byte, error) {
	type noMethod FlattenInstruction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FloatingPointList: A metric value representing a list of floating
// point numbers.
type FloatingPointList struct {
	// Elements: Elements of the list.
	Elements []float64 `json:"elements,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Elements") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Elements") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FloatingPointList) MarshalJSON() ([]byte, error) {
	type noMethod FloatingPointList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FloatingPointMean: A representation of a floating point mean metric
// contribution.
type FloatingPointMean struct {
	// Count: The number of values being aggregated.
	Count *SplitInt64 `json:"count,omitempty"`

	// Sum: The sum of all values being aggregated.
	Sum float64 `json:"sum,omitempty"`

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

func (s *FloatingPointMean) MarshalJSON() ([]byte, error) {
	type noMethod FloatingPointMean
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetDebugConfigRequest: Request to get updated debug configuration for
// component.
type GetDebugConfigRequest struct {
	// ComponentId: The internal component id for which debug configuration
	// is requested.
	ComponentId string `json:"componentId,omitempty"`

	// WorkerId: The worker id, i.e., VM hostname.
	WorkerId string `json:"workerId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComponentId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComponentId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetDebugConfigRequest) MarshalJSON() ([]byte, error) {
	type noMethod GetDebugConfigRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetDebugConfigResponse: Response to a get debug configuration
// request.
type GetDebugConfigResponse struct {
	// Config: The encoded debug configuration for the requested component.
	Config string `json:"config,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Config") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Config") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetDebugConfigResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetDebugConfigResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// InstructionInput: An input of an instruction, as a reference to an
// output of a producer instruction.
type InstructionInput struct {
	// OutputNum: The output index (origin zero) within the producer.
	OutputNum int64 `json:"outputNum,omitempty"`

	// ProducerInstructionIndex: The index (origin zero) of the parallel
	// instruction that produces the output to be consumed by this input.
	// This index is relative to the list of instructions in this input's
	// instruction's containing MapTask.
	ProducerInstructionIndex int64 `json:"producerInstructionIndex,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OutputNum") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OutputNum") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *InstructionInput) MarshalJSON() ([]byte, error) {
	type noMethod InstructionInput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// InstructionOutput: An output of an instruction.
type InstructionOutput struct {
	// Codec: The codec to use to encode data being written via this output.
	Codec googleapi.RawMessage `json:"codec,omitempty"`

	// Name: The user-provided name of this output.
	Name string `json:"name,omitempty"`

	// OnlyCountKeyBytes: For system-generated byte and mean byte metrics,
	// certain instructions should only report the key size.
	OnlyCountKeyBytes bool `json:"onlyCountKeyBytes,omitempty"`

	// OnlyCountValueBytes: For system-generated byte and mean byte metrics,
	// certain instructions should only report the value size.
	OnlyCountValueBytes bool `json:"onlyCountValueBytes,omitempty"`

	// OriginalName: System-defined name for this output in the original
	// workflow graph. Outputs that do not contribute to an original
	// instruction do not set this.
	OriginalName string `json:"originalName,omitempty"`

	// SystemName: System-defined name of this output. Unique across the
	// workflow.
	SystemName string `json:"systemName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Codec") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Codec") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *InstructionOutput) MarshalJSON() ([]byte, error) {
	type noMethod InstructionOutput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IntegerList: A metric value representing a list of integers.
type IntegerList struct {
	// Elements: Elements of the list.
	Elements []*SplitInt64 `json:"elements,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Elements") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Elements") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IntegerList) MarshalJSON() ([]byte, error) {
	type noMethod IntegerList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IntegerMean: A representation of an integer mean metric contribution.
type IntegerMean struct {
	// Count: The number of values being aggregated.
	Count *SplitInt64 `json:"count,omitempty"`

	// Sum: The sum of all values being aggregated.
	Sum *SplitInt64 `json:"sum,omitempty"`

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

func (s *IntegerMean) MarshalJSON() ([]byte, error) {
	type noMethod IntegerMean
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Job: Defines a job to be run by the Dataflow service.
type Job struct {
	// ClientRequestId: Client's unique identifier of the job, re-used by
	// SDK across retried attempts. If this field is set, the service will
	// ensure its uniqueness. That is, the request to create a job will fail
	// if the service has knowledge of a previously submitted job with the
	// same client's id and job name. The caller may, for example, use this
	// field to ensure idempotence of job creation across retried attempts
	// to create a job. By default, the field is empty and, in that case,
	// the service ignores it.
	ClientRequestId string `json:"clientRequestId,omitempty"`

	// CreateTime: Timestamp when job was initially created. Immutable, set
	// by the Dataflow service.
	CreateTime string `json:"createTime,omitempty"`

	// CurrentState: The current state of the job. Jobs are created in the
	// JOB_STATE_STOPPED state unless otherwise specified. A job in the
	// JOB_STATE_RUNNING state may asynchronously enter a terminal state.
	// Once a job has reached a terminal state, no further state updates may
	// be made. This field may be mutated by the Dataflow service; callers
	// cannot mutate it.
	//
	// Possible values:
	//   "JOB_STATE_UNKNOWN"
	//   "JOB_STATE_STOPPED"
	//   "JOB_STATE_RUNNING"
	//   "JOB_STATE_DONE"
	//   "JOB_STATE_FAILED"
	//   "JOB_STATE_CANCELLED"
	//   "JOB_STATE_UPDATED"
	//   "JOB_STATE_DRAINING"
	//   "JOB_STATE_DRAINED"
	CurrentState string `json:"currentState,omitempty"`

	// CurrentStateTime: The timestamp associated with the current state.
	CurrentStateTime string `json:"currentStateTime,omitempty"`

	// Environment: Environment for the job.
	Environment *Environment `json:"environment,omitempty"`

	// ExecutionInfo: Information about how the Dataflow service will
	// actually run the job.
	ExecutionInfo *JobExecutionInfo `json:"executionInfo,omitempty"`

	// Id: The unique ID of this job. This field is set by the Dataflow
	// service when the Job is created, and is immutable for the life of the
	// Job.
	Id string `json:"id,omitempty"`

	// Labels: User-defined labels for this job. The labels map can contain
	// no more than 64 entries. Entries of the labels map are UTF8 strings
	// that comply with the following restrictions: * Keys must conform to
	// regexp: \p{Ll}\p{Lo}{0,62} * Values must conform to regexp:
	// [\p{Ll}\p{Lo}\p{N}_-]{0,63} * Both keys and values are additionally
	// constrained to be <= 128 bytes in size.
	Labels map[string]string `json:"labels,omitempty"`

	// Location: The location which contains this job.
	Location string `json:"location,omitempty"`

	// Name: The user-specified Dataflow job name. Only one Job with a given
	// name may exist in a project at any given time. If a caller attempts
	// to create a Job with the same name as an already-existing Job, the
	// attempt will return the existing Job. The name must match the regular
	// expression [a-z]([-a-z0-9]{0,38}[a-z0-9])?
	Name string `json:"name,omitempty"`

	// ProjectId: The project which owns the job.
	ProjectId string `json:"projectId,omitempty"`

	// ReplaceJobId: If this job is an update of an existing job, this field
	// will be the ID of the job it replaced. When sending a
	// CreateJobRequest, you can update a job by specifying it here. The job
	// named here will be stopped, and its intermediate state transferred to
	// this job.
	ReplaceJobId string `json:"replaceJobId,omitempty"`

	// ReplacedByJobId: If another job is an update of this job (and thus,
	// this job is in JOB_STATE_UPDATED), this field will contain the ID of
	// that job.
	ReplacedByJobId string `json:"replacedByJobId,omitempty"`

	// RequestedState: The job's requested state. UpdateJob may be used to
	// switch between the JOB_STATE_STOPPED and JOB_STATE_RUNNING states, by
	// setting requested_state. UpdateJob may also be used to directly set a
	// job's requested state to JOB_STATE_CANCELLED or JOB_STATE_DONE,
	// irrevocably terminating the job if it has not already reached a
	// terminal state.
	//
	// Possible values:
	//   "JOB_STATE_UNKNOWN"
	//   "JOB_STATE_STOPPED"
	//   "JOB_STATE_RUNNING"
	//   "JOB_STATE_DONE"
	//   "JOB_STATE_FAILED"
	//   "JOB_STATE_CANCELLED"
	//   "JOB_STATE_UPDATED"
	//   "JOB_STATE_DRAINING"
	//   "JOB_STATE_DRAINED"
	RequestedState string `json:"requestedState,omitempty"`

	// Steps: The top-level steps that constitute the entire job.
	Steps []*Step `json:"steps,omitempty"`

	// TempFiles: A set of files the system should be aware of that are used
	// for temporary storage. These temporary files will be removed on job
	// completion. No duplicates are allowed. No file patterns are
	// supported. The supported files are: Google Cloud Storage:
	// storage.googleapis.com/{bucket}/{object}
	// bucket.storage.googleapis.com/{object}
	TempFiles []string `json:"tempFiles,omitempty"`

	// TransformNameMapping: Map of transform name prefixes of the job to be
	// replaced to the corresponding name prefixes of the new job.
	TransformNameMapping map[string]string `json:"transformNameMapping,omitempty"`

	// Type: The type of dataflow job.
	//
	// Possible values:
	//   "JOB_TYPE_UNKNOWN"
	//   "JOB_TYPE_BATCH"
	//   "JOB_TYPE_STREAMING"
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ClientRequestId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ClientRequestId") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Job) MarshalJSON() ([]byte, error) {
	type noMethod Job
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// JobExecutionInfo: Additional information about how a Dataflow job
// will be executed which isn’t contained in the submitted job.
type JobExecutionInfo struct {
	// Stages: A mapping from each stage to the information about that
	// stage.
	Stages map[string]JobExecutionStageInfo `json:"stages,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Stages") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Stages") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JobExecutionInfo) MarshalJSON() ([]byte, error) {
	type noMethod JobExecutionInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// JobExecutionStageInfo: Contains information about how a particular
// google.dataflow.v1beta3.Step will be executed.
type JobExecutionStageInfo struct {
	// StepName: The steps associated with the execution stage. Note that
	// stages may have several steps, and that a given step might be run by
	// more than one stage.
	StepName []string `json:"stepName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "StepName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "StepName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JobExecutionStageInfo) MarshalJSON() ([]byte, error) {
	type noMethod JobExecutionStageInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// JobMessage: A particular message pertaining to a Dataflow job.
type JobMessage struct {
	// Id: Identifies the message. This is automatically generated by the
	// service; the caller should treat it as an opaque string.
	Id string `json:"id,omitempty"`

	// MessageImportance: Importance level of the message.
	//
	// Possible values:
	//   "JOB_MESSAGE_IMPORTANCE_UNKNOWN"
	//   "JOB_MESSAGE_DEBUG"
	//   "JOB_MESSAGE_DETAILED"
	//   "JOB_MESSAGE_BASIC"
	//   "JOB_MESSAGE_WARNING"
	//   "JOB_MESSAGE_ERROR"
	MessageImportance string `json:"messageImportance,omitempty"`

	// MessageText: The text of the message.
	MessageText string `json:"messageText,omitempty"`

	// Time: The timestamp of the message.
	Time string `json:"time,omitempty"`

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

func (s *JobMessage) MarshalJSON() ([]byte, error) {
	type noMethod JobMessage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// JobMetrics: JobMetrics contains a collection of metrics descibing the
// detailed progress of a Dataflow job. Metrics correspond to
// user-defined and system-defined metrics in the job. This resource
// captures only the most recent values of each metric; time-series data
// can be queried for them (under the same metric names) from Cloud
// Monitoring.
type JobMetrics struct {
	// MetricTime: Timestamp as of which metric values are current.
	MetricTime string `json:"metricTime,omitempty"`

	// Metrics: All metrics for this job.
	Metrics []*MetricUpdate `json:"metrics,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "MetricTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MetricTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JobMetrics) MarshalJSON() ([]byte, error) {
	type noMethod JobMetrics
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// KeyRangeDataDiskAssignment: Data disk assignment information for a
// specific key-range of a sharded computation. Currently we only
// support UTF-8 character splits to simplify encoding into JSON.
type KeyRangeDataDiskAssignment struct {
	// DataDisk: The name of the data disk where data for this range is
	// stored. This name is local to the Google Cloud Platform project and
	// uniquely identifies the disk within that project, for example
	// "myproject-1014-104817-4c2-harness-0-disk-1".
	DataDisk string `json:"dataDisk,omitempty"`

	// End: The end (exclusive) of the key range.
	End string `json:"end,omitempty"`

	// Start: The start (inclusive) of the key range.
	Start string `json:"start,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DataDisk") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataDisk") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *KeyRangeDataDiskAssignment) MarshalJSON() ([]byte, error) {
	type noMethod KeyRangeDataDiskAssignment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// KeyRangeLocation: Location information for a specific key-range of a
// sharded computation. Currently we only support UTF-8 character splits
// to simplify encoding into JSON.
type KeyRangeLocation struct {
	// DataDisk: The name of the data disk where data for this range is
	// stored. This name is local to the Google Cloud Platform project and
	// uniquely identifies the disk within that project, for example
	// "myproject-1014-104817-4c2-harness-0-disk-1".
	DataDisk string `json:"dataDisk,omitempty"`

	// DeliveryEndpoint: The physical location of this range assignment to
	// be used for streaming computation cross-worker message delivery.
	DeliveryEndpoint string `json:"deliveryEndpoint,omitempty"`

	// End: The end (exclusive) of the key range.
	End string `json:"end,omitempty"`

	// PersistentDirectory: The location of the persistent state for this
	// range, as a persistent directory in the worker local filesystem.
	PersistentDirectory string `json:"persistentDirectory,omitempty"`

	// Start: The start (inclusive) of the key range.
	Start string `json:"start,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DataDisk") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataDisk") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *KeyRangeLocation) MarshalJSON() ([]byte, error) {
	type noMethod KeyRangeLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LeaseWorkItemRequest: Request to lease WorkItems.
type LeaseWorkItemRequest struct {
	// CurrentWorkerTime: The current timestamp at the worker.
	CurrentWorkerTime string `json:"currentWorkerTime,omitempty"`

	// Location: The location which contains the WorkItem's job.
	Location string `json:"location,omitempty"`

	// RequestedLeaseDuration: The initial lease period.
	RequestedLeaseDuration string `json:"requestedLeaseDuration,omitempty"`

	// WorkItemTypes: Filter for WorkItem type.
	WorkItemTypes []string `json:"workItemTypes,omitempty"`

	// WorkerCapabilities: Worker capabilities. WorkItems might be limited
	// to workers with specific capabilities.
	WorkerCapabilities []string `json:"workerCapabilities,omitempty"`

	// WorkerId: Identifies the worker leasing work -- typically the ID of
	// the virtual machine running the worker.
	WorkerId string `json:"workerId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CurrentWorkerTime")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CurrentWorkerTime") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *LeaseWorkItemRequest) MarshalJSON() ([]byte, error) {
	type noMethod LeaseWorkItemRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LeaseWorkItemResponse: Response to a request to lease WorkItems.
type LeaseWorkItemResponse struct {
	// WorkItems: A list of the leased WorkItems.
	WorkItems []*WorkItem `json:"workItems,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "WorkItems") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "WorkItems") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LeaseWorkItemResponse) MarshalJSON() ([]byte, error) {
	type noMethod LeaseWorkItemResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListJobMessagesResponse: Response to a request to list job messages.
type ListJobMessagesResponse struct {
	// JobMessages: Messages in ascending timestamp order.
	JobMessages []*JobMessage `json:"jobMessages,omitempty"`

	// NextPageToken: The token to obtain the next page of results if there
	// are more.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "JobMessages") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "JobMessages") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListJobMessagesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListJobMessagesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListJobsResponse: Response to a request to list Dataflow jobs. This
// may be a partial response, depending on the page size in the
// ListJobsRequest.
type ListJobsResponse struct {
	// FailedLocation: Zero or more messages describing locations that
	// failed to respond.
	FailedLocation []*FailedLocation `json:"failedLocation,omitempty"`

	// Jobs: A subset of the requested job information.
	Jobs []*Job `json:"jobs,omitempty"`

	// NextPageToken: Set if there may be more results than fit in this
	// response.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "FailedLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FailedLocation") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ListJobsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListJobsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MapTask: MapTask consists of an ordered set of instructions, each of
// which describes one particular low-level operation for the worker to
// perform in order to accomplish the MapTask's WorkItem. Each
// instruction must appear in the list before any instructions which
// depends on its output.
type MapTask struct {
	// Instructions: The instructions in the MapTask.
	Instructions []*ParallelInstruction `json:"instructions,omitempty"`

	// StageName: System-defined name of the stage containing this MapTask.
	// Unique across the workflow.
	StageName string `json:"stageName,omitempty"`

	// SystemName: System-defined name of this MapTask. Unique across the
	// workflow.
	SystemName string `json:"systemName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Instructions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Instructions") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MapTask) MarshalJSON() ([]byte, error) {
	type noMethod MapTask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MetricShortId: The metric short id is returned to the user alongside
// an offset into ReportWorkItemStatusRequest
type MetricShortId struct {
	// MetricIndex: The index of the corresponding metric in the
	// ReportWorkItemStatusRequest. Required.
	MetricIndex int64 `json:"metricIndex,omitempty"`

	// ShortId: The service-generated short identifier for the metric.
	ShortId int64 `json:"shortId,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "MetricIndex") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MetricIndex") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MetricShortId) MarshalJSON() ([]byte, error) {
	type noMethod MetricShortId
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MetricStructuredName: Identifies a metric, by describing the source
// which generated the metric.
type MetricStructuredName struct {
	// Context: Zero or more labeled fields which identify the part of the
	// job this metric is associated with, such as the name of a step or
	// collection. For example, built-in counters associated with steps will
	// have context['step'] = . Counters associated with PCollections in the
	// SDK will have context['pcollection'] =
	// .
	Context map[string]string `json:"context,omitempty"`

	// Name: Worker-defined metric name.
	Name string `json:"name,omitempty"`

	// Origin: Origin (namespace) of metric name. May be blank for
	// user-define metrics; will be "dataflow" for metrics defined by the
	// Dataflow service or SDK.
	Origin string `json:"origin,omitempty"`

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

func (s *MetricStructuredName) MarshalJSON() ([]byte, error) {
	type noMethod MetricStructuredName
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MetricUpdate: Describes the state of a metric.
type MetricUpdate struct {
	// Cumulative: True if this metric is reported as the total cumulative
	// aggregate value accumulated since the worker started working on this
	// WorkItem. By default this is false, indicating that this metric is
	// reported as a delta that is not associated with any WorkItem.
	Cumulative bool `json:"cumulative,omitempty"`

	// Internal: Worker-computed aggregate value for internal use by the
	// Dataflow service.
	Internal interface{} `json:"internal,omitempty"`

	// Kind: Metric aggregation kind. The possible metric aggregation kinds
	// are "Sum", "Max", "Min", "Mean", "Set", "And", and "Or". The
	// specified aggregation kind is case-insensitive. If omitted, this is
	// not an aggregated value but instead a single metric sample value.
	Kind string `json:"kind,omitempty"`

	// MeanCount: Worker-computed aggregate value for the "Mean" aggregation
	// kind. This holds the count of the aggregated values and is used in
	// combination with mean_sum above to obtain the actual mean aggregate
	// value. The only possible value type is Long.
	MeanCount interface{} `json:"meanCount,omitempty"`

	// MeanSum: Worker-computed aggregate value for the "Mean" aggregation
	// kind. This holds the sum of the aggregated values and is used in
	// combination with mean_count below to obtain the actual mean aggregate
	// value. The only possible value types are Long and Double.
	MeanSum interface{} `json:"meanSum,omitempty"`

	// Name: Name of the metric.
	Name *MetricStructuredName `json:"name,omitempty"`

	// Scalar: Worker-computed aggregate value for aggregation kinds "Sum",
	// "Max", "Min", "And", and "Or". The possible value types are Long,
	// Double, and Boolean.
	Scalar interface{} `json:"scalar,omitempty"`

	// Set: Worker-computed aggregate value for the "Set" aggregation kind.
	// The only possible value type is a list of Values whose type can be
	// Long, Double, or String, according to the metric's type. All Values
	// in the list must be of the same type.
	Set interface{} `json:"set,omitempty"`

	// UpdateTime: Timestamp associated with the metric value. Optional when
	// workers are reporting work progress; it will be filled in responses
	// from the metrics API.
	UpdateTime string `json:"updateTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Cumulative") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Cumulative") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MetricUpdate) MarshalJSON() ([]byte, error) {
	type noMethod MetricUpdate
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MountedDataDisk: Describes mounted data disk.
type MountedDataDisk struct {
	// DataDisk: The name of the data disk. This name is local to the Google
	// Cloud Platform project and uniquely identifies the disk within that
	// project, for example "myproject-1014-104817-4c2-harness-0-disk-1".
	DataDisk string `json:"dataDisk,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DataDisk") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataDisk") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MountedDataDisk) MarshalJSON() ([]byte, error) {
	type noMethod MountedDataDisk
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MultiOutputInfo: Information about an output of a multi-output DoFn.
type MultiOutputInfo struct {
	// Tag: The id of the tag the user code will emit to this output by;
	// this should correspond to the tag of some SideInputInfo.
	Tag string `json:"tag,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Tag") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Tag") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MultiOutputInfo) MarshalJSON() ([]byte, error) {
	type noMethod MultiOutputInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// NameAndKind: Basic metadata about a counter.
type NameAndKind struct {
	// Kind: Counter aggregation kind.
	//
	// Possible values:
	//   "INVALID"
	//   "SUM"
	//   "MAX"
	//   "MIN"
	//   "MEAN"
	//   "OR"
	//   "AND"
	//   "SET"
	Kind string `json:"kind,omitempty"`

	// Name: Name of the counter.
	Name string `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Kind") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Kind") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NameAndKind) MarshalJSON() ([]byte, error) {
	type noMethod NameAndKind
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Package: Packages that need to be installed in order for a worker to
// run the steps of the Dataflow job which will be assigned to its
// worker pool. This is the mechanism by which the SDK causes code to be
// loaded onto the workers. For example, the Dataflow Java SDK might use
// this to install jars containing the user's code and all of the
// various dependencies (libraries, data files, etc) required in order
// for that code to run.
type Package struct {
	// Location: The resource to read the package from. The supported
	// resource type is: Google Cloud Storage:
	// storage.googleapis.com/{bucket} bucket.storage.googleapis.com/
	Location string `json:"location,omitempty"`

	// Name: The name of the package.
	Name string `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Location") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Location") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Package) MarshalJSON() ([]byte, error) {
	type noMethod Package
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ParDoInstruction: An instruction that does a ParDo operation. Takes
// one main input and zero or more side inputs, and produces zero or
// more outputs. Runs user code.
type ParDoInstruction struct {
	// Input: The input.
	Input *InstructionInput `json:"input,omitempty"`

	// MultiOutputInfos: Information about each of the outputs, if user_fn
	// is a MultiDoFn.
	MultiOutputInfos []*MultiOutputInfo `json:"multiOutputInfos,omitempty"`

	// NumOutputs: The number of outputs.
	NumOutputs int64 `json:"numOutputs,omitempty"`

	// SideInputs: Zero or more side inputs.
	SideInputs []*SideInputInfo `json:"sideInputs,omitempty"`

	// UserFn: The user function to invoke.
	UserFn googleapi.RawMessage `json:"userFn,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Input") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Input") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ParDoInstruction) MarshalJSON() ([]byte, error) {
	type noMethod ParDoInstruction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ParallelInstruction: Describes a particular operation comprising a
// MapTask.
type ParallelInstruction struct {
	// Flatten: Additional information for Flatten instructions.
	Flatten *FlattenInstruction `json:"flatten,omitempty"`

	// Name: User-provided name of this operation.
	Name string `json:"name,omitempty"`

	// OriginalName: System-defined name for the operation in the original
	// workflow graph.
	OriginalName string `json:"originalName,omitempty"`

	// Outputs: Describes the outputs of the instruction.
	Outputs []*InstructionOutput `json:"outputs,omitempty"`

	// ParDo: Additional information for ParDo instructions.
	ParDo *ParDoInstruction `json:"parDo,omitempty"`

	// PartialGroupByKey: Additional information for PartialGroupByKey
	// instructions.
	PartialGroupByKey *PartialGroupByKeyInstruction `json:"partialGroupByKey,omitempty"`

	// Read: Additional information for Read instructions.
	Read *ReadInstruction `json:"read,omitempty"`

	// SystemName: System-defined name of this operation. Unique across the
	// workflow.
	SystemName string `json:"systemName,omitempty"`

	// Write: Additional information for Write instructions.
	Write *WriteInstruction `json:"write,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Flatten") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Flatten") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ParallelInstruction) MarshalJSON() ([]byte, error) {
	type noMethod ParallelInstruction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PartialGroupByKeyInstruction: An instruction that does a partial
// group-by-key. One input and one output.
type PartialGroupByKeyInstruction struct {
	// Input: Describes the input to the partial group-by-key instruction.
	Input *InstructionInput `json:"input,omitempty"`

	// InputElementCodec: The codec to use for interpreting an element in
	// the input PTable.
	InputElementCodec googleapi.RawMessage `json:"inputElementCodec,omitempty"`

	// OriginalCombineValuesInputStoreName: If this instruction includes a
	// combining function this is the name of the intermediate store between
	// the GBK and the CombineValues.
	OriginalCombineValuesInputStoreName string `json:"originalCombineValuesInputStoreName,omitempty"`

	// OriginalCombineValuesStepName: If this instruction includes a
	// combining function, this is the name of the CombineValues instruction
	// lifted into this instruction.
	OriginalCombineValuesStepName string `json:"originalCombineValuesStepName,omitempty"`

	// SideInputs: Zero or more side inputs.
	SideInputs []*SideInputInfo `json:"sideInputs,omitempty"`

	// ValueCombiningFn: The value combining function to invoke.
	ValueCombiningFn googleapi.RawMessage `json:"valueCombiningFn,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Input") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Input") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PartialGroupByKeyInstruction) MarshalJSON() ([]byte, error) {
	type noMethod PartialGroupByKeyInstruction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Position: Position defines a position within a collection of data.
// The value can be either the end position, a key (used with ordered
// collections), a byte offset, or a record index.
type Position struct {
	// ByteOffset: Position is a byte offset.
	ByteOffset int64 `json:"byteOffset,omitempty,string"`

	// ConcatPosition: CloudPosition is a concat position.
	ConcatPosition *ConcatPosition `json:"concatPosition,omitempty"`

	// End: Position is past all other positions. Also useful for the end
	// position of an unbounded range.
	End bool `json:"end,omitempty"`

	// Key: Position is a string key, ordered lexicographically.
	Key string `json:"key,omitempty"`

	// RecordIndex: Position is a record index.
	RecordIndex int64 `json:"recordIndex,omitempty,string"`

	// ShufflePosition: CloudPosition is a base64 encoded
	// BatchShufflePosition (with FIXED sharding).
	ShufflePosition string `json:"shufflePosition,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ByteOffset") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ByteOffset") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Position) MarshalJSON() ([]byte, error) {
	type noMethod Position
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PubsubLocation: Identifies a pubsub location to use for transferring
// data into or out of a streaming Dataflow job.
type PubsubLocation struct {
	// DropLateData: Indicates whether the pipeline allows late-arriving
	// data.
	DropLateData bool `json:"dropLateData,omitempty"`

	// IdLabel: If set, contains a pubsub label from which to extract record
	// ids. If left empty, record deduplication will be strictly best
	// effort.
	IdLabel string `json:"idLabel,omitempty"`

	// Subscription: A pubsub subscription, in the form of
	// "pubsub.googleapis.com/subscriptions/
	// /"
	Subscription string `json:"subscription,omitempty"`

	// TimestampLabel: If set, contains a pubsub label from which to extract
	// record timestamps. If left empty, record timestamps will be generated
	// upon arrival.
	TimestampLabel string `json:"timestampLabel,omitempty"`

	// Topic: A pubsub topic, in the form of
	// "pubsub.googleapis.com/topics/
	// /"
	Topic string `json:"topic,omitempty"`

	// TrackingSubscription: If set, specifies the pubsub subscription that
	// will be used for tracking custom time timestamps for watermark
	// estimation.
	TrackingSubscription string `json:"trackingSubscription,omitempty"`

	// WithAttributes: If true, then the client has requested to get pubsub
	// attributes.
	WithAttributes bool `json:"withAttributes,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DropLateData") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DropLateData") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PubsubLocation) MarshalJSON() ([]byte, error) {
	type noMethod PubsubLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReadInstruction: An instruction that reads records. Takes no inputs,
// produces one output.
type ReadInstruction struct {
	// Source: The source to read from.
	Source *Source `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReadInstruction) MarshalJSON() ([]byte, error) {
	type noMethod ReadInstruction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportWorkItemStatusRequest: Request to report the status of
// WorkItems.
type ReportWorkItemStatusRequest struct {
	// CurrentWorkerTime: The current timestamp at the worker.
	CurrentWorkerTime string `json:"currentWorkerTime,omitempty"`

	// Location: The location which contains the WorkItem's job.
	Location string `json:"location,omitempty"`

	// WorkItemStatuses: The order is unimportant, except that the order of
	// the WorkItemServiceState messages in the ReportWorkItemStatusResponse
	// corresponds to the order of WorkItemStatus messages here.
	WorkItemStatuses []*WorkItemStatus `json:"workItemStatuses,omitempty"`

	// WorkerId: The ID of the worker reporting the WorkItem status. If this
	// does not match the ID of the worker which the Dataflow service
	// believes currently has the lease on the WorkItem, the report will be
	// dropped (with an error response).
	WorkerId string `json:"workerId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CurrentWorkerTime")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CurrentWorkerTime") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ReportWorkItemStatusRequest) MarshalJSON() ([]byte, error) {
	type noMethod ReportWorkItemStatusRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportWorkItemStatusResponse: Response from a request to report the
// status of WorkItems.
type ReportWorkItemStatusResponse struct {
	// WorkItemServiceStates: A set of messages indicating the service-side
	// state for each WorkItem whose status was reported, in the same order
	// as the WorkItemStatus messages in the ReportWorkItemStatusRequest
	// which resulting in this response.
	WorkItemServiceStates []*WorkItemServiceState `json:"workItemServiceStates,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g.
	// "WorkItemServiceStates") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "WorkItemServiceStates") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ReportWorkItemStatusResponse) MarshalJSON() ([]byte, error) {
	type noMethod ReportWorkItemStatusResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportedParallelism: Represents the level of parallelism in a
// WorkItem's input, reported by the worker.
type ReportedParallelism struct {
	// IsInfinite: Specifies whether the parallelism is infinite. If true,
	// "value" is ignored. Infinite parallelism means the service will
	// assume that the work item can always be split into more non-empty
	// work items by dynamic splitting. This is a work-around for lack of
	// support for infinity by the current JSON-based Java RPC stack.
	IsInfinite bool `json:"isInfinite,omitempty"`

	// Value: Specifies the level of parallelism in case it is finite.
	Value float64 `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IsInfinite") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IsInfinite") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportedParallelism) MarshalJSON() ([]byte, error) {
	type noMethod ReportedParallelism
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SendDebugCaptureRequest: Request to send encoded debug information.
type SendDebugCaptureRequest struct {
	// ComponentId: The internal component id for which debug information is
	// sent.
	ComponentId string `json:"componentId,omitempty"`

	// Data: The encoded debug information.
	Data string `json:"data,omitempty"`

	// WorkerId: The worker id, i.e., VM hostname.
	WorkerId string `json:"workerId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComponentId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComponentId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SendDebugCaptureRequest) MarshalJSON() ([]byte, error) {
	type noMethod SendDebugCaptureRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SendDebugCaptureResponse: Response to a send capture request. nothing
type SendDebugCaptureResponse struct {
	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`
}

// SendWorkerMessagesRequest: A request for sending worker messages to
// the service.
type SendWorkerMessagesRequest struct {
	// WorkerMessages: The WorkerMessages to send.
	WorkerMessages []*WorkerMessage `json:"workerMessages,omitempty"`

	// ForceSendFields is a list of field names (e.g. "WorkerMessages") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "WorkerMessages") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SendWorkerMessagesRequest) MarshalJSON() ([]byte, error) {
	type noMethod SendWorkerMessagesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SendWorkerMessagesResponse: The response to the worker messages.
type SendWorkerMessagesResponse struct {
	// WorkerMessageResponses: The servers response to the worker messages.
	WorkerMessageResponses []*WorkerMessageResponse `json:"workerMessageResponses,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g.
	// "WorkerMessageResponses") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "WorkerMessageResponses")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SendWorkerMessagesResponse) MarshalJSON() ([]byte, error) {
	type noMethod SendWorkerMessagesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SeqMapTask: Describes a particular function to invoke.
type SeqMapTask struct {
	// Inputs: Information about each of the inputs.
	Inputs []*SideInputInfo `json:"inputs,omitempty"`

	// Name: The user-provided name of the SeqDo operation.
	Name string `json:"name,omitempty"`

	// OutputInfos: Information about each of the outputs.
	OutputInfos []*SeqMapTaskOutputInfo `json:"outputInfos,omitempty"`

	// StageName: System-defined name of the stage containing the SeqDo
	// operation. Unique across the workflow.
	StageName string `json:"stageName,omitempty"`

	// SystemName: System-defined name of the SeqDo operation. Unique across
	// the workflow.
	SystemName string `json:"systemName,omitempty"`

	// UserFn: The user function to invoke.
	UserFn googleapi.RawMessage `json:"userFn,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Inputs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Inputs") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SeqMapTask) MarshalJSON() ([]byte, error) {
	type noMethod SeqMapTask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SeqMapTaskOutputInfo: Information about an output of a SeqMapTask.
type SeqMapTaskOutputInfo struct {
	// Sink: The sink to write the output value to.
	Sink *Sink `json:"sink,omitempty"`

	// Tag: The id of the TupleTag the user code will tag the output value
	// by.
	Tag string `json:"tag,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Sink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Sink") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SeqMapTaskOutputInfo) MarshalJSON() ([]byte, error) {
	type noMethod SeqMapTaskOutputInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ShellTask: A task which consists of a shell command for the worker to
// execute.
type ShellTask struct {
	// Command: The shell command to run.
	Command string `json:"command,omitempty"`

	// ExitCode: Exit code for the task.
	ExitCode int64 `json:"exitCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Command") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Command") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ShellTask) MarshalJSON() ([]byte, error) {
	type noMethod ShellTask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SideInputInfo: Information about a side input of a DoFn or an input
// of a SeqDoFn.
type SideInputInfo struct {
	// Kind: How to interpret the source element(s) as a side input value.
	Kind googleapi.RawMessage `json:"kind,omitempty"`

	// Sources: The source(s) to read element(s) from to get the value of
	// this side input. If more than one source, then the elements are taken
	// from the sources, in the specified order if order matters. At least
	// one source is required.
	Sources []*Source `json:"sources,omitempty"`

	// Tag: The id of the tag the user code will access this side input by;
	// this should correspond to the tag of some MultiOutputInfo.
	Tag string `json:"tag,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Kind") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Kind") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SideInputInfo) MarshalJSON() ([]byte, error) {
	type noMethod SideInputInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Sink: A sink that records can be encoded and written to.
type Sink struct {
	// Codec: The codec to use to encode data written to the sink.
	Codec googleapi.RawMessage `json:"codec,omitempty"`

	// Spec: The sink to write to, plus its parameters.
	Spec googleapi.RawMessage `json:"spec,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Codec") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Codec") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Sink) MarshalJSON() ([]byte, error) {
	type noMethod Sink
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Source: A source that records can be read and decoded from.
type Source struct {
	// BaseSpecs: While splitting, sources may specify the produced bundles
	// as differences against another source, in order to save backend-side
	// memory and allow bigger jobs. For details, see SourceSplitRequest. To
	// support this use case, the full set of parameters of the source is
	// logically obtained by taking the latest explicitly specified value of
	// each parameter in the order: base_specs (later items win), spec
	// (overrides anything in base_specs).
	BaseSpecs []googleapi.RawMessage `json:"baseSpecs,omitempty"`

	// Codec: The codec to use to decode data read from the source.
	Codec googleapi.RawMessage `json:"codec,omitempty"`

	// DoesNotNeedSplitting: Setting this value to true hints to the
	// framework that the source doesn't need splitting, and using
	// SourceSplitRequest on it would yield
	// SOURCE_SPLIT_OUTCOME_USE_CURRENT. E.g. a file splitter may set this
	// to true when splitting a single file into a set of byte ranges of
	// appropriate size, and set this to false when splitting a filepattern
	// into individual files. However, for efficiency, a file splitter may
	// decide to produce file subranges directly from the filepattern to
	// avoid a splitting round-trip. See SourceSplitRequest for an overview
	// of the splitting process. This field is meaningful only in the Source
	// objects populated by the user (e.g. when filling in a DerivedSource).
	// Source objects supplied by the framework to the user don't have this
	// field populated.
	DoesNotNeedSplitting bool `json:"doesNotNeedSplitting,omitempty"`

	// Metadata: Optionally, metadata for this source can be supplied right
	// away, avoiding a SourceGetMetadataOperation roundtrip (see
	// SourceOperationRequest). This field is meaningful only in the Source
	// objects populated by the user (e.g. when filling in a DerivedSource).
	// Source objects supplied by the framework to the user don't have this
	// field populated.
	Metadata *SourceMetadata `json:"metadata,omitempty"`

	// Spec: The source to read from, plus its parameters.
	Spec googleapi.RawMessage `json:"spec,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BaseSpecs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BaseSpecs") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Source) MarshalJSON() ([]byte, error) {
	type noMethod Source
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceFork: DEPRECATED in favor of DynamicSourceSplit.
type SourceFork struct {
	// Primary: DEPRECATED
	Primary *SourceSplitShard `json:"primary,omitempty"`

	// PrimarySource: DEPRECATED
	PrimarySource *DerivedSource `json:"primarySource,omitempty"`

	// Residual: DEPRECATED
	Residual *SourceSplitShard `json:"residual,omitempty"`

	// ResidualSource: DEPRECATED
	ResidualSource *DerivedSource `json:"residualSource,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Primary") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Primary") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceFork) MarshalJSON() ([]byte, error) {
	type noMethod SourceFork
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceGetMetadataRequest: A request to compute the SourceMetadata of
// a Source.
type SourceGetMetadataRequest struct {
	// Source: Specification of the source whose metadata should be
	// computed.
	Source *Source `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceGetMetadataRequest) MarshalJSON() ([]byte, error) {
	type noMethod SourceGetMetadataRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceGetMetadataResponse: The result of a
// SourceGetMetadataOperation.
type SourceGetMetadataResponse struct {
	// Metadata: The computed metadata.
	Metadata *SourceMetadata `json:"metadata,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Metadata") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Metadata") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceGetMetadataResponse) MarshalJSON() ([]byte, error) {
	type noMethod SourceGetMetadataResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceMetadata: Metadata about a Source useful for automatically
// optimizing and tuning the pipeline, etc.
type SourceMetadata struct {
	// EstimatedSizeBytes: An estimate of the total size (in bytes) of the
	// data that would be read from this source. This estimate is in terms
	// of external storage size, before any decompression or other
	// processing done by the reader.
	EstimatedSizeBytes int64 `json:"estimatedSizeBytes,omitempty,string"`

	// Infinite: Specifies that the size of this source is known to be
	// infinite (this is a streaming source).
	Infinite bool `json:"infinite,omitempty"`

	// ProducesSortedKeys: Whether this source is known to produce key/value
	// pairs with the (encoded) keys in lexicographically sorted order.
	ProducesSortedKeys bool `json:"producesSortedKeys,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EstimatedSizeBytes")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EstimatedSizeBytes") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SourceMetadata) MarshalJSON() ([]byte, error) {
	type noMethod SourceMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceOperationRequest: A work item that represents the different
// operations that can be performed on a user-defined Source
// specification.
type SourceOperationRequest struct {
	// GetMetadata: Information about a request to get metadata about a
	// source.
	GetMetadata *SourceGetMetadataRequest `json:"getMetadata,omitempty"`

	// Split: Information about a request to split a source.
	Split *SourceSplitRequest `json:"split,omitempty"`

	// ForceSendFields is a list of field names (e.g. "GetMetadata") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GetMetadata") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceOperationRequest) MarshalJSON() ([]byte, error) {
	type noMethod SourceOperationRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceOperationResponse: The result of a SourceOperationRequest,
// specified in ReportWorkItemStatusRequest.source_operation when the
// work item is completed.
type SourceOperationResponse struct {
	// GetMetadata: A response to a request to get metadata about a source.
	GetMetadata *SourceGetMetadataResponse `json:"getMetadata,omitempty"`

	// Split: A response to a request to split a source.
	Split *SourceSplitResponse `json:"split,omitempty"`

	// ForceSendFields is a list of field names (e.g. "GetMetadata") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GetMetadata") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceOperationResponse) MarshalJSON() ([]byte, error) {
	type noMethod SourceOperationResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceSplitOptions: Hints for splitting a Source into bundles (parts
// for parallel processing) using SourceSplitRequest.
type SourceSplitOptions struct {
	// DesiredBundleSizeBytes: The source should be split into a set of
	// bundles where the estimated size of each is approximately this many
	// bytes.
	DesiredBundleSizeBytes int64 `json:"desiredBundleSizeBytes,omitempty,string"`

	// DesiredShardSizeBytes: DEPRECATED in favor of
	// desired_bundle_size_bytes.
	DesiredShardSizeBytes int64 `json:"desiredShardSizeBytes,omitempty,string"`

	// ForceSendFields is a list of field names (e.g.
	// "DesiredBundleSizeBytes") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DesiredBundleSizeBytes")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SourceSplitOptions) MarshalJSON() ([]byte, error) {
	type noMethod SourceSplitOptions
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceSplitRequest: Represents the operation to split a high-level
// Source specification into bundles (parts for parallel processing). At
// a high level, splitting of a source into bundles happens as follows:
// SourceSplitRequest is applied to the source. If it returns
// SOURCE_SPLIT_OUTCOME_USE_CURRENT, no further splitting happens and
// the source is used "as is". Otherwise, splitting is applied
// recursively to each produced DerivedSource. As an optimization, for
// any Source, if its does_not_need_splitting is true, the framework
// assumes that splitting this source would return
// SOURCE_SPLIT_OUTCOME_USE_CURRENT, and doesn't initiate a
// SourceSplitRequest. This applies both to the initial source being
// split and to bundles produced from it.
type SourceSplitRequest struct {
	// Options: Hints for tuning the splitting process.
	Options *SourceSplitOptions `json:"options,omitempty"`

	// Source: Specification of the source to be split.
	Source *Source `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Options") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Options") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceSplitRequest) MarshalJSON() ([]byte, error) {
	type noMethod SourceSplitRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceSplitResponse: The response to a SourceSplitRequest.
type SourceSplitResponse struct {
	// Bundles: If outcome is SPLITTING_HAPPENED, then this is a list of
	// bundles into which the source was split. Otherwise this field is
	// ignored. This list can be empty, which means the source represents an
	// empty input.
	Bundles []*DerivedSource `json:"bundles,omitempty"`

	// Outcome: Indicates whether splitting happened and produced a list of
	// bundles. If this is USE_CURRENT_SOURCE_AS_IS, the current source
	// should be processed "as is" without splitting. "bundles" is ignored
	// in this case. If this is SPLITTING_HAPPENED, then "bundles" contains
	// a list of bundles into which the source was split.
	//
	// Possible values:
	//   "SOURCE_SPLIT_OUTCOME_UNKNOWN"
	//   "SOURCE_SPLIT_OUTCOME_USE_CURRENT"
	//   "SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED"
	Outcome string `json:"outcome,omitempty"`

	// Shards: DEPRECATED in favor of bundles.
	Shards []*SourceSplitShard `json:"shards,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Bundles") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Bundles") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SourceSplitResponse) MarshalJSON() ([]byte, error) {
	type noMethod SourceSplitResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SourceSplitShard: DEPRECATED in favor of DerivedSource.
type SourceSplitShard struct {
	// DerivationMode: DEPRECATED
	//
	// Possible values:
	//   "SOURCE_DERIVATION_MODE_UNKNOWN"
	//   "SOURCE_DERIVATION_MODE_INDEPENDENT"
	//   "SOURCE_DERIVATION_MODE_CHILD_OF_CURRENT"
	//   "SOURCE_DERIVATION_MODE_SIBLING_OF_CURRENT"
	DerivationMode string `json:"derivationMode,omitempty"`

	// Source: DEPRECATED
	Source *Source `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DerivationMode") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DerivationMode") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SourceSplitShard) MarshalJSON() ([]byte, error) {
	type noMethod SourceSplitShard
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SplitInt64: A representation of an int64, n, that is immune to
// precision loss when encoded in JSON.
type SplitInt64 struct {
	// HighBits: The high order bits, including the sign: n >> 32.
	HighBits int64 `json:"highBits,omitempty"`

	// LowBits: The low order bits: n & 0xffffffff.
	LowBits int64 `json:"lowBits,omitempty"`

	// ForceSendFields is a list of field names (e.g. "HighBits") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HighBits") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SplitInt64) MarshalJSON() ([]byte, error) {
	type noMethod SplitInt64
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StateFamilyConfig: State family configuration.
type StateFamilyConfig struct {
	// IsRead: If true, this family corresponds to a read operation.
	IsRead bool `json:"isRead,omitempty"`

	// StateFamily: The state family value.
	StateFamily string `json:"stateFamily,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IsRead") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IsRead") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StateFamilyConfig) MarshalJSON() ([]byte, error) {
	type noMethod StateFamilyConfig
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

// Step: Defines a particular step within a Dataflow job. A job consists
// of multiple steps, each of which performs some specific operation as
// part of the overall job. Data is typically passed from one step to
// another as part of the job. Here's an example of a sequence of steps
// which together implement a Map-Reduce job: * Read a collection of
// data from some source, parsing the collection's elements. * Validate
// the elements. * Apply a user-defined function to map each element to
// some value and extract an element-specific key value. * Group
// elements with the same key into a single element with that key,
// transforming a multiply-keyed collection into a uniquely-keyed
// collection. * Write the elements out to some data sink. (Note that
// the Dataflow service may be used to run many different types of jobs,
// not just Map-Reduce).
type Step struct {
	// Kind: The kind of step in the dataflow Job.
	Kind string `json:"kind,omitempty"`

	// Name: Name identifying the step. This must be unique for each step
	// with respect to all other steps in the dataflow Job.
	Name string `json:"name,omitempty"`

	// Properties: Named properties associated with the step. Each kind of
	// predefined step has its own required set of properties.
	Properties googleapi.RawMessage `json:"properties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Kind") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Kind") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Step) MarshalJSON() ([]byte, error) {
	type noMethod Step
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamLocation: Describes a stream of data, either as input to be
// processed or as output of a streaming Dataflow job.
type StreamLocation struct {
	// CustomSourceLocation: The stream is a custom source.
	CustomSourceLocation *CustomSourceLocation `json:"customSourceLocation,omitempty"`

	// PubsubLocation: The stream is a pubsub stream.
	PubsubLocation *PubsubLocation `json:"pubsubLocation,omitempty"`

	// SideInputLocation: The stream is a streaming side input.
	SideInputLocation *StreamingSideInputLocation `json:"sideInputLocation,omitempty"`

	// StreamingStageLocation: The stream is part of another computation
	// within the current streaming Dataflow job.
	StreamingStageLocation *StreamingStageLocation `json:"streamingStageLocation,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "CustomSourceLocation") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CustomSourceLocation") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *StreamLocation) MarshalJSON() ([]byte, error) {
	type noMethod StreamLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingComputationConfig: Configuration information for a single
// streaming computation.
type StreamingComputationConfig struct {
	// ComputationId: Unique identifier for this computation.
	ComputationId string `json:"computationId,omitempty"`

	// Instructions: Instructions that comprise the computation.
	Instructions []*ParallelInstruction `json:"instructions,omitempty"`

	// StageName: Stage name of this computation.
	StageName string `json:"stageName,omitempty"`

	// SystemName: System defined name for this computation.
	SystemName string `json:"systemName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComputationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComputationId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StreamingComputationConfig) MarshalJSON() ([]byte, error) {
	type noMethod StreamingComputationConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingComputationRanges: Describes full or partial data disk
// assignment information of the computation ranges.
type StreamingComputationRanges struct {
	// ComputationId: The ID of the computation.
	ComputationId string `json:"computationId,omitempty"`

	// RangeAssignments: Data disk assignments for ranges from this
	// computation.
	RangeAssignments []*KeyRangeDataDiskAssignment `json:"rangeAssignments,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComputationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComputationId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StreamingComputationRanges) MarshalJSON() ([]byte, error) {
	type noMethod StreamingComputationRanges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingComputationTask: A task which describes what action should
// be performed for the specified streaming computation ranges.
type StreamingComputationTask struct {
	// ComputationRanges: Contains ranges of a streaming computation this
	// task should apply to.
	ComputationRanges []*StreamingComputationRanges `json:"computationRanges,omitempty"`

	// DataDisks: Describes the set of data disks this task should apply to.
	DataDisks []*MountedDataDisk `json:"dataDisks,omitempty"`

	// TaskType: A type of streaming computation task.
	//
	// Possible values:
	//   "STREAMING_COMPUTATION_TASK_UNKNOWN"
	//   "STREAMING_COMPUTATION_TASK_STOP"
	//   "STREAMING_COMPUTATION_TASK_START"
	TaskType string `json:"taskType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ComputationRanges")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ComputationRanges") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *StreamingComputationTask) MarshalJSON() ([]byte, error) {
	type noMethod StreamingComputationTask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingConfigTask: A task that carries configuration information
// for streaming computations.
type StreamingConfigTask struct {
	// StreamingComputationConfigs: Set of computation configuration
	// information.
	StreamingComputationConfigs []*StreamingComputationConfig `json:"streamingComputationConfigs,omitempty"`

	// UserStepToStateFamilyNameMap: Map from user step names to state
	// families.
	UserStepToStateFamilyNameMap map[string]string `json:"userStepToStateFamilyNameMap,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "StreamingComputationConfigs") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "StreamingComputationConfigs") to include in API requests with the
	// JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StreamingConfigTask) MarshalJSON() ([]byte, error) {
	type noMethod StreamingConfigTask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingSetupTask: A task which initializes part of a streaming
// Dataflow job.
type StreamingSetupTask struct {
	// Drain: The user has requested drain.
	Drain bool `json:"drain,omitempty"`

	// ReceiveWorkPort: The TCP port on which the worker should listen for
	// messages from other streaming computation workers.
	ReceiveWorkPort int64 `json:"receiveWorkPort,omitempty"`

	// StreamingComputationTopology: The global topology of the streaming
	// Dataflow job.
	StreamingComputationTopology *TopologyConfig `json:"streamingComputationTopology,omitempty"`

	// WorkerHarnessPort: The TCP port used by the worker to communicate
	// with the Dataflow worker harness.
	WorkerHarnessPort int64 `json:"workerHarnessPort,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Drain") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Drain") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StreamingSetupTask) MarshalJSON() ([]byte, error) {
	type noMethod StreamingSetupTask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingSideInputLocation: Identifies the location of a streaming
// side input.
type StreamingSideInputLocation struct {
	// StateFamily: Identifies the state family where this side input is
	// stored.
	StateFamily string `json:"stateFamily,omitempty"`

	// Tag: Identifies the particular side input within the streaming
	// Dataflow job.
	Tag string `json:"tag,omitempty"`

	// ForceSendFields is a list of field names (e.g. "StateFamily") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "StateFamily") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StreamingSideInputLocation) MarshalJSON() ([]byte, error) {
	type noMethod StreamingSideInputLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StreamingStageLocation: Identifies the location of a streaming
// computation stage, for stage-to-stage communication.
type StreamingStageLocation struct {
	// StreamId: Identifies the particular stream within the streaming
	// Dataflow job.
	StreamId string `json:"streamId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "StreamId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "StreamId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StreamingStageLocation) MarshalJSON() ([]byte, error) {
	type noMethod StreamingStageLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StringList: A metric value representing a list of strings.
type StringList struct {
	// Elements: Elements of the list.
	Elements []string `json:"elements,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Elements") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Elements") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StringList) MarshalJSON() ([]byte, error) {
	type noMethod StringList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TaskRunnerSettings: Taskrunner configuration settings.
type TaskRunnerSettings struct {
	// Alsologtostderr: Also send taskrunner log info to stderr?
	Alsologtostderr bool `json:"alsologtostderr,omitempty"`

	// BaseTaskDir: Location on the worker for task-specific subdirectories.
	BaseTaskDir string `json:"baseTaskDir,omitempty"`

	// BaseUrl: The base URL for the taskrunner to use when accessing Google
	// Cloud APIs. When workers access Google Cloud APIs, they logically do
	// so via relative URLs. If this field is specified, it supplies the
	// base URL to use for resolving these relative URLs. The normative
	// algorithm used is defined by RFC 1808, "Relative Uniform Resource
	// Locators". If not specified, the default value is
	// "http://www.googleapis.com/"
	BaseUrl string `json:"baseUrl,omitempty"`

	// CommandlinesFileName: Store preprocessing commands in this file.
	CommandlinesFileName string `json:"commandlinesFileName,omitempty"`

	// ContinueOnException: Do we continue taskrunner if an exception is
	// hit?
	ContinueOnException bool `json:"continueOnException,omitempty"`

	// DataflowApiVersion: API version of endpoint, e.g. "v1b3"
	DataflowApiVersion string `json:"dataflowApiVersion,omitempty"`

	// HarnessCommand: Command to launch the worker harness.
	HarnessCommand string `json:"harnessCommand,omitempty"`

	// LanguageHint: Suggested backend language.
	LanguageHint string `json:"languageHint,omitempty"`

	// LogDir: Directory on the VM to store logs.
	LogDir string `json:"logDir,omitempty"`

	// LogToSerialconsole: Send taskrunner log into to Google Compute Engine
	// VM serial console?
	LogToSerialconsole bool `json:"logToSerialconsole,omitempty"`

	// LogUploadLocation: Indicates where to put logs. If this is not
	// specified, the logs will not be uploaded. The supported resource type
	// is: Google Cloud Storage: storage.googleapis.com/{bucket}/{object}
	// bucket.storage.googleapis.com/{object}
	LogUploadLocation string `json:"logUploadLocation,omitempty"`

	// OauthScopes: OAuth2 scopes to be requested by the taskrunner in order
	// to access the dataflow API.
	OauthScopes []string `json:"oauthScopes,omitempty"`

	// ParallelWorkerSettings: Settings to pass to the parallel worker
	// harness.
	ParallelWorkerSettings *WorkerSettings `json:"parallelWorkerSettings,omitempty"`

	// StreamingWorkerMainClass: Streaming worker main class name.
	StreamingWorkerMainClass string `json:"streamingWorkerMainClass,omitempty"`

	// TaskGroup: The UNIX group ID on the worker VM to use for tasks
	// launched by taskrunner; e.g. "wheel".
	TaskGroup string `json:"taskGroup,omitempty"`

	// TaskUser: The UNIX user ID on the worker VM to use for tasks launched
	// by taskrunner; e.g. "root".
	TaskUser string `json:"taskUser,omitempty"`

	// TempStoragePrefix: The prefix of the resources the taskrunner should
	// use for temporary storage. The supported resource type is: Google
	// Cloud Storage: storage.googleapis.com/{bucket}/{object}
	// bucket.storage.googleapis.com/{object}
	TempStoragePrefix string `json:"tempStoragePrefix,omitempty"`

	// VmId: ID string of VM.
	VmId string `json:"vmId,omitempty"`

	// WorkflowFileName: Store the workflow in this file.
	WorkflowFileName string `json:"workflowFileName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alsologtostderr") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alsologtostderr") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *TaskRunnerSettings) MarshalJSON() ([]byte, error) {
	type noMethod TaskRunnerSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TopologyConfig: Global topology of the streaming Dataflow job,
// including all computations and their sharded locations.
type TopologyConfig struct {
	// Computations: The computations associated with a streaming Dataflow
	// job.
	Computations []*ComputationTopology `json:"computations,omitempty"`

	// DataDiskAssignments: The disks assigned to a streaming Dataflow job.
	DataDiskAssignments []*DataDiskAssignment `json:"dataDiskAssignments,omitempty"`

	// ForwardingKeyBits: The size (in bits) of keys that will be assigned
	// to source messages.
	ForwardingKeyBits int64 `json:"forwardingKeyBits,omitempty"`

	// PersistentStateVersion: Version number for persistent state.
	PersistentStateVersion int64 `json:"persistentStateVersion,omitempty"`

	// UserStageToComputationNameMap: Maps user stage names to stable
	// computation names.
	UserStageToComputationNameMap map[string]string `json:"userStageToComputationNameMap,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Computations") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Computations") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TopologyConfig) MarshalJSON() ([]byte, error) {
	type noMethod TopologyConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkItem: WorkItem represents basic information about a WorkItem to
// be executed in the cloud.
type WorkItem struct {
	// Configuration: Work item-specific configuration as an opaque blob.
	Configuration string `json:"configuration,omitempty"`

	// Id: Identifies this WorkItem.
	Id int64 `json:"id,omitempty,string"`

	// InitialReportIndex: The initial index to use when reporting the
	// status of the WorkItem.
	InitialReportIndex int64 `json:"initialReportIndex,omitempty,string"`

	// JobId: Identifies the workflow job this WorkItem belongs to.
	JobId string `json:"jobId,omitempty"`

	// LeaseExpireTime: Time when the lease on this Work will expire.
	LeaseExpireTime string `json:"leaseExpireTime,omitempty"`

	// MapTask: Additional information for MapTask WorkItems.
	MapTask *MapTask `json:"mapTask,omitempty"`

	// Packages: Any required packages that need to be fetched in order to
	// execute this WorkItem.
	Packages []*Package `json:"packages,omitempty"`

	// ProjectId: Identifies the cloud project this WorkItem belongs to.
	ProjectId string `json:"projectId,omitempty"`

	// ReportStatusInterval: Recommended reporting interval.
	ReportStatusInterval string `json:"reportStatusInterval,omitempty"`

	// SeqMapTask: Additional information for SeqMapTask WorkItems.
	SeqMapTask *SeqMapTask `json:"seqMapTask,omitempty"`

	// ShellTask: Additional information for ShellTask WorkItems.
	ShellTask *ShellTask `json:"shellTask,omitempty"`

	// SourceOperationTask: Additional information for source operation
	// WorkItems.
	SourceOperationTask *SourceOperationRequest `json:"sourceOperationTask,omitempty"`

	// StreamingComputationTask: Additional information for
	// StreamingComputationTask WorkItems.
	StreamingComputationTask *StreamingComputationTask `json:"streamingComputationTask,omitempty"`

	// StreamingConfigTask: Additional information for StreamingConfigTask
	// WorkItems.
	StreamingConfigTask *StreamingConfigTask `json:"streamingConfigTask,omitempty"`

	// StreamingSetupTask: Additional information for StreamingSetupTask
	// WorkItems.
	StreamingSetupTask *StreamingSetupTask `json:"streamingSetupTask,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Configuration") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Configuration") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WorkItem) MarshalJSON() ([]byte, error) {
	type noMethod WorkItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkItemServiceState: The Dataflow service's idea of the current
// state of a WorkItem being processed by a worker.
type WorkItemServiceState struct {
	// HarnessData: Other data returned by the service, specific to the
	// particular worker harness.
	HarnessData googleapi.RawMessage `json:"harnessData,omitempty"`

	// LeaseExpireTime: Time at which the current lease will expire.
	LeaseExpireTime string `json:"leaseExpireTime,omitempty"`

	// MetricShortId: The short ids that workers should use in subsequent
	// metric updates. Workers should strive to use short ids whenever
	// possible, but it is ok to request the short_id again if a worker lost
	// track of it (e.g. if the worker is recovering from a crash). NOTE: it
	// is possible that the response may have short ids for a subset of the
	// metrics.
	MetricShortId []*MetricShortId `json:"metricShortId,omitempty"`

	// NextReportIndex: The index value to use for the next report sent by
	// the worker. Note: If the report call fails for whatever reason, the
	// worker should reuse this index for subsequent report attempts.
	NextReportIndex int64 `json:"nextReportIndex,omitempty,string"`

	// ReportStatusInterval: New recommended reporting interval.
	ReportStatusInterval string `json:"reportStatusInterval,omitempty"`

	// SplitRequest: The progress point in the WorkItem where the Dataflow
	// service suggests that the worker truncate the task.
	SplitRequest *ApproximateSplitRequest `json:"splitRequest,omitempty"`

	// SuggestedStopPoint: DEPRECATED in favor of split_request.
	SuggestedStopPoint *ApproximateProgress `json:"suggestedStopPoint,omitempty"`

	// SuggestedStopPosition: Obsolete, always empty.
	SuggestedStopPosition *Position `json:"suggestedStopPosition,omitempty"`

	// ForceSendFields is a list of field names (e.g. "HarnessData") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HarnessData") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WorkItemServiceState) MarshalJSON() ([]byte, error) {
	type noMethod WorkItemServiceState
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkItemStatus: Conveys a worker's progress through the work
// described by a WorkItem.
type WorkItemStatus struct {
	// Completed: True if the WorkItem was completed (successfully or
	// unsuccessfully).
	Completed bool `json:"completed,omitempty"`

	// CounterUpdates: Worker output counters for this WorkItem.
	CounterUpdates []*CounterUpdate `json:"counterUpdates,omitempty"`

	// DynamicSourceSplit: See documentation of stop_position.
	DynamicSourceSplit *DynamicSourceSplit `json:"dynamicSourceSplit,omitempty"`

	// Errors: Specifies errors which occurred during processing. If errors
	// are provided, and completed = true, then the WorkItem is considered
	// to have failed.
	Errors []*Status `json:"errors,omitempty"`

	// MetricUpdates: DEPRECATED in favor of counter_updates.
	MetricUpdates []*MetricUpdate `json:"metricUpdates,omitempty"`

	// Progress: DEPRECATED in favor of reported_progress.
	Progress *ApproximateProgress `json:"progress,omitempty"`

	// ReportIndex: The report index. When a WorkItem is leased, the lease
	// will contain an initial report index. When a WorkItem's status is
	// reported to the system, the report should be sent with that report
	// index, and the response will contain the index the worker should use
	// for the next report. Reports received with unexpected index values
	// will be rejected by the service. In order to preserve idempotency,
	// the worker should not alter the contents of a report, even if the
	// worker must submit the same report multiple times before getting back
	// a response. The worker should not submit a subsequent report until
	// the response for the previous report had been received from the
	// service.
	ReportIndex int64 `json:"reportIndex,omitempty,string"`

	// ReportedProgress: The worker's progress through this WorkItem.
	ReportedProgress *ApproximateReportedProgress `json:"reportedProgress,omitempty"`

	// RequestedLeaseDuration: Amount of time the worker requests for its
	// lease.
	RequestedLeaseDuration string `json:"requestedLeaseDuration,omitempty"`

	// SourceFork: DEPRECATED in favor of dynamic_source_split.
	SourceFork *SourceFork `json:"sourceFork,omitempty"`

	// SourceOperationResponse: If the work item represented a
	// SourceOperationRequest, and the work is completed, contains the
	// result of the operation.
	SourceOperationResponse *SourceOperationResponse `json:"sourceOperationResponse,omitempty"`

	// StopPosition: A worker may split an active map task in two parts,
	// "primary" and "residual", continuing to process the primary part and
	// returning the residual part into the pool of available work. This
	// event is called a "dynamic split" and is critical to the dynamic work
	// rebalancing feature. The two obtained sub-tasks are called "parts" of
	// the split. The parts, if concatenated, must represent the same input
	// as would be read by the current task if the split did not happen. The
	// exact way in which the original task is decomposed into the two parts
	// is specified either as a position demarcating them (stop_position),
	// or explicitly as two DerivedSources, if this task consumes a
	// user-defined source type (dynamic_source_split). The "current" task
	// is adjusted as a result of the split: after a task with range [A, B)
	// sends a stop_position update at C, its range is considered to be [A,
	// C), e.g.: * Progress should be interpreted relative to the new range,
	// e.g. "75% completed" means "75% of [A, C) completed" * The worker
	// should interpret proposed_stop_position relative to the new range,
	// e.g. "split at 68%" should be interpreted as "split at 68% of [A,
	// C)". * If the worker chooses to split again using stop_position, only
	// stop_positions in [A, C) will be accepted. * Etc.
	// dynamic_source_split has similar semantics: e.g., if a task with
	// source S splits using dynamic_source_split into {P, R} (where P and R
	// must be together equivalent to S), then subsequent progress and
	// proposed_stop_position should be interpreted relative to P, and in a
	// potential subsequent dynamic_source_split into {P', R'}, P' and R'
	// must be together equivalent to P, etc.
	StopPosition *Position `json:"stopPosition,omitempty"`

	// WorkItemId: Identifies the WorkItem.
	WorkItemId string `json:"workItemId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Completed") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Completed") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WorkItemStatus) MarshalJSON() ([]byte, error) {
	type noMethod WorkItemStatus
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerHealthReport: WorkerHealthReport contains information about the
// health of a worker. The VM should be identified by the labels
// attached to the WorkerMessage that this health ping belongs to.
type WorkerHealthReport struct {
	// Pods: The pods running on the worker. See:
	// http://kubernetes.io/v1.1/docs/api-reference/v1/definitions.html#_v1_pod This field is used by the worker to send the status of the indvidual containers running on each
	// worker.
	Pods []googleapi.RawMessage `json:"pods,omitempty"`

	// ReportInterval: The interval at which the worker is sending health
	// reports. The default value of 0 should be interpreted as the field is
	// not being explicitly set by the worker.
	ReportInterval string `json:"reportInterval,omitempty"`

	// VmIsHealthy: Whether the VM is healthy.
	VmIsHealthy bool `json:"vmIsHealthy,omitempty"`

	// VmStartupTime: The time the VM was booted.
	VmStartupTime string `json:"vmStartupTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Pods") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Pods") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WorkerHealthReport) MarshalJSON() ([]byte, error) {
	type noMethod WorkerHealthReport
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerHealthReportResponse: WorkerHealthReportResponse contains
// information returned to the worker in response to a health ping.
type WorkerHealthReportResponse struct {
	// ReportInterval: A positive value indicates the worker should change
	// its reporting interval to the specified value. The default value of
	// zero means no change in report rate is requested by the server.
	ReportInterval string `json:"reportInterval,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ReportInterval") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ReportInterval") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *WorkerHealthReportResponse) MarshalJSON() ([]byte, error) {
	type noMethod WorkerHealthReportResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerMessage: WorkerMessage provides information to the backend
// about a worker.
type WorkerMessage struct {
	// Labels: Labels are used to group WorkerMessages. For example, a
	// worker_message about a particular container might have the labels: {
	// "JOB_ID": "2015-04-22", "WORKER_ID": "wordcount-vm-2015…"
	// "CONTAINER_TYPE": "worker", "CONTAINER_ID": "ac1234def"} Label tags
	// typically correspond to Label enum values. However, for ease of
	// development other strings can be used as tags. LABEL_UNSPECIFIED
	// should not be used here.
	Labels map[string]string `json:"labels,omitempty"`

	// Time: The timestamp of the worker_message.
	Time string `json:"time,omitempty"`

	// WorkerHealthReport: The health of a worker.
	WorkerHealthReport *WorkerHealthReport `json:"workerHealthReport,omitempty"`

	// WorkerMessageCode: A worker message code.
	WorkerMessageCode *WorkerMessageCode `json:"workerMessageCode,omitempty"`

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

func (s *WorkerMessage) MarshalJSON() ([]byte, error) {
	type noMethod WorkerMessage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerMessageCode: A message code is used to report status and error
// messages to the service. The message codes are intended to be machine
// readable. The service will take care of translating these into user
// understandable messages if necessary. Example use cases: 1. Worker
// processes reporting successful startup. 2. Worker processes reporting
// specific errors (e.g. package staging failure).
type WorkerMessageCode struct {
	// Code: The code is a string intended for consumption by a machine that
	// identifies the type of message being sent. Examples: 1.
	// "HARNESS_STARTED" might be used to indicate the worker harness has
	// started. 2. "GCS_DOWNLOAD_ERROR" might be used to indicate an error
	// downloading a GCS file as part of the boot process of one of the
	// worker containers. This is a string and not an enum to make it easy
	// to add new codes without waiting for an API change.
	Code string `json:"code,omitempty"`

	// Parameters: Parameters contains specific information about the code.
	// This is a struct to allow parameters of different types. Examples: 1.
	// For a "HARNESS_STARTED" message parameters might provide the name of
	// the worker and additional data like timing information. 2. For a
	// "GCS_DOWNLOAD_ERROR" parameters might contain fields listing the GCS
	// objects being downloaded and fields containing errors. In general
	// complex data structures should be avoided. If a worker needs to send
	// a specific and complicated data structure then please consider
	// defining a new proto and adding it to the data oneof in
	// WorkerMessageResponse. Conventions: Parameters should only be used
	// for information that isn't typically passed as a label. hostname and
	// other worker identifiers should almost always be passed as labels
	// since they will be included on most messages.
	Parameters googleapi.RawMessage `json:"parameters,omitempty"`

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

func (s *WorkerMessageCode) MarshalJSON() ([]byte, error) {
	type noMethod WorkerMessageCode
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerMessageResponse: A worker_message response allows the server to
// pass information to the sender.
type WorkerMessageResponse struct {
	// WorkerHealthReportResponse: The service's response to a worker's
	// health report.
	WorkerHealthReportResponse *WorkerHealthReportResponse `json:"workerHealthReportResponse,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "WorkerHealthReportResponse") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "WorkerHealthReportResponse") to include in API requests with the
	// JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WorkerMessageResponse) MarshalJSON() ([]byte, error) {
	type noMethod WorkerMessageResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerPool: Describes one particular pool of Dataflow workers to be
// instantiated by the Dataflow service in order to perform the
// computations required by a job. Note that a workflow job may use
// multiple pools, in order to match the various computational
// requirements of the various stages of the job.
type WorkerPool struct {
	// AutoscalingSettings: Settings for autoscaling of this WorkerPool.
	AutoscalingSettings *AutoscalingSettings `json:"autoscalingSettings,omitempty"`

	// DataDisks: Data disks that are used by a VM in this workflow.
	DataDisks []*Disk `json:"dataDisks,omitempty"`

	// DefaultPackageSet: The default package set to install. This allows
	// the service to select a default set of packages which are useful to
	// worker harnesses written in a particular language.
	//
	// Possible values:
	//   "DEFAULT_PACKAGE_SET_UNKNOWN"
	//   "DEFAULT_PACKAGE_SET_NONE"
	//   "DEFAULT_PACKAGE_SET_JAVA"
	//   "DEFAULT_PACKAGE_SET_PYTHON"
	DefaultPackageSet string `json:"defaultPackageSet,omitempty"`

	// DiskSizeGb: Size of root disk for VMs, in GB. If zero or unspecified,
	// the service will attempt to choose a reasonable default.
	DiskSizeGb int64 `json:"diskSizeGb,omitempty"`

	// DiskSourceImage: Fully qualified source image for disks.
	DiskSourceImage string `json:"diskSourceImage,omitempty"`

	// DiskType: Type of root disk for VMs. If empty or unspecified, the
	// service will attempt to choose a reasonable default.
	DiskType string `json:"diskType,omitempty"`

	// IpConfiguration: Configuration for VM IPs.
	//
	// Possible values:
	//   "WORKER_IP_UNSPECIFIED"
	//   "WORKER_IP_PUBLIC"
	//   "WORKER_IP_PRIVATE"
	IpConfiguration string `json:"ipConfiguration,omitempty"`

	// Kind: The kind of the worker pool; currently only 'harness' and
	// 'shuffle' are supported.
	Kind string `json:"kind,omitempty"`

	// MachineType: Machine type (e.g. "n1-standard-1"). If empty or
	// unspecified, the service will attempt to choose a reasonable default.
	MachineType string `json:"machineType,omitempty"`

	// Metadata: Metadata to set on the Google Compute Engine VMs.
	Metadata map[string]string `json:"metadata,omitempty"`

	// Network: Network to which VMs will be assigned. If empty or
	// unspecified, the service will use the network "default".
	Network string `json:"network,omitempty"`

	// NumThreadsPerWorker: The number of threads per worker harness. If
	// empty or unspecified, the service will choose a number of threads
	// (according to the number of cores on the selected machine type for
	// batch, or 1 by convention for streaming).
	NumThreadsPerWorker int64 `json:"numThreadsPerWorker,omitempty"`

	// NumWorkers: Number of Google Compute Engine workers in this pool
	// needed to execute the job. If zero or unspecified, the service will
	// attempt to choose a reasonable default.
	NumWorkers int64 `json:"numWorkers,omitempty"`

	// OnHostMaintenance: The action to take on host maintenance, as defined
	// by the Google Compute Engine API.
	OnHostMaintenance string `json:"onHostMaintenance,omitempty"`

	// Packages: Packages to be installed on workers.
	Packages []*Package `json:"packages,omitempty"`

	// PoolArgs: Extra arguments for this worker pool.
	PoolArgs googleapi.RawMessage `json:"poolArgs,omitempty"`

	// Subnetwork: Subnetwork to which VMs will be assigned, if desired.
	// Expected to be of the form "regions/REGION/subnetworks/SUBNETWORK".
	Subnetwork string `json:"subnetwork,omitempty"`

	// TaskrunnerSettings: Settings passed through to Google Compute Engine
	// workers when using the standard Dataflow task runner. Users should
	// ignore this field.
	TaskrunnerSettings *TaskRunnerSettings `json:"taskrunnerSettings,omitempty"`

	// TeardownPolicy: Sets the policy for determining when to turndown
	// worker pool. Allowed values are: TEARDOWN_ALWAYS,
	// TEARDOWN_ON_SUCCESS, and TEARDOWN_NEVER. TEARDOWN_ALWAYS means
	// workers are always torn down regardless of whether the job succeeds.
	// TEARDOWN_ON_SUCCESS means workers are torn down if the job succeeds.
	// TEARDOWN_NEVER means the workers are never torn down. If the workers
	// are not torn down by the service, they will continue to run and use
	// Google Compute Engine VM resources in the user's project until they
	// are explicitly terminated by the user. Because of this, Google
	// recommends using the TEARDOWN_ALWAYS policy except for small,
	// manually supervised test jobs. If unknown or unspecified, the service
	// will attempt to choose a reasonable default.
	//
	// Possible values:
	//   "TEARDOWN_POLICY_UNKNOWN"
	//   "TEARDOWN_ALWAYS"
	//   "TEARDOWN_ON_SUCCESS"
	//   "TEARDOWN_NEVER"
	TeardownPolicy string `json:"teardownPolicy,omitempty"`

	// WorkerHarnessContainerImage: Docker container image that executes
	// Dataflow worker harness, residing in Google Container Registry.
	// Required.
	WorkerHarnessContainerImage string `json:"workerHarnessContainerImage,omitempty"`

	// Zone: Zone to run the worker pools in. If empty or unspecified, the
	// service will attempt to choose a reasonable default.
	Zone string `json:"zone,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AutoscalingSettings")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AutoscalingSettings") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *WorkerPool) MarshalJSON() ([]byte, error) {
	type noMethod WorkerPool
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WorkerSettings: Provides data to pass through to the worker harness.
type WorkerSettings struct {
	// BaseUrl: The base URL for accessing Google Cloud APIs. When workers
	// access Google Cloud APIs, they logically do so via relative URLs. If
	// this field is specified, it supplies the base URL to use for
	// resolving these relative URLs. The normative algorithm used is
	// defined by RFC 1808, "Relative Uniform Resource Locators". If not
	// specified, the default value is "http://www.googleapis.com/"
	BaseUrl string `json:"baseUrl,omitempty"`

	// ReportingEnabled: Send work progress updates to service.
	ReportingEnabled bool `json:"reportingEnabled,omitempty"`

	// ServicePath: The Dataflow service path relative to the root URL, for
	// example, "dataflow/v1b3/projects".
	ServicePath string `json:"servicePath,omitempty"`

	// ShuffleServicePath: The Shuffle service path relative to the root
	// URL, for example, "shuffle/v1beta1".
	ShuffleServicePath string `json:"shuffleServicePath,omitempty"`

	// TempStoragePrefix: The prefix of the resources the system should use
	// for temporary storage. The supported resource type is: Google Cloud
	// Storage: storage.googleapis.com/{bucket}/{object}
	// bucket.storage.googleapis.com/{object}
	TempStoragePrefix string `json:"tempStoragePrefix,omitempty"`

	// WorkerId: ID of the worker running this pipeline.
	WorkerId string `json:"workerId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BaseUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BaseUrl") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WorkerSettings) MarshalJSON() ([]byte, error) {
	type noMethod WorkerSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WriteInstruction: An instruction that writes records. Takes one
// input, produces no outputs.
type WriteInstruction struct {
	// Input: The input.
	Input *InstructionInput `json:"input,omitempty"`

	// Sink: The sink to write to.
	Sink *Sink `json:"sink,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Input") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Input") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WriteInstruction) MarshalJSON() ([]byte, error) {
	type noMethod WriteInstruction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "dataflow.projects.workerMessages":

type ProjectsWorkerMessagesCall struct {
	s                         *Service
	projectId                 string
	sendworkermessagesrequest *SendWorkerMessagesRequest
	urlParams_                gensupport.URLParams
	ctx_                      context.Context
	header_                   http.Header
}

// WorkerMessages: Send a worker_message to the service.
func (r *ProjectsService) WorkerMessages(projectId string, sendworkermessagesrequest *SendWorkerMessagesRequest) *ProjectsWorkerMessagesCall {
	c := &ProjectsWorkerMessagesCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.sendworkermessagesrequest = sendworkermessagesrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsWorkerMessagesCall) Fields(s ...googleapi.Field) *ProjectsWorkerMessagesCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsWorkerMessagesCall) Context(ctx context.Context) *ProjectsWorkerMessagesCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsWorkerMessagesCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsWorkerMessagesCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.sendworkermessagesrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/WorkerMessages")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.workerMessages" call.
// Exactly one of *SendWorkerMessagesResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *SendWorkerMessagesResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsWorkerMessagesCall) Do(opts ...googleapi.CallOption) (*SendWorkerMessagesResponse, error) {
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
	ret := &SendWorkerMessagesResponse{
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
	//   "description": "Send a worker_message to the service.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.workerMessages",
	//   "parameterOrder": [
	//     "projectId"
	//   ],
	//   "parameters": {
	//     "projectId": {
	//       "description": "The project to send the WorkerMessages to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/WorkerMessages",
	//   "request": {
	//     "$ref": "SendWorkerMessagesRequest"
	//   },
	//   "response": {
	//     "$ref": "SendWorkerMessagesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.create":

type ProjectsJobsCreateCall struct {
	s          *Service
	projectId  string
	job        *Job
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a dataflow job.
func (r *ProjectsJobsService) Create(projectId string, job *Job) *ProjectsJobsCreateCall {
	c := &ProjectsJobsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.job = job
	return c
}

// Location sets the optional parameter "location": The location which
// contains this job.
func (c *ProjectsJobsCreateCall) Location(location string) *ProjectsJobsCreateCall {
	c.urlParams_.Set("location", location)
	return c
}

// ReplaceJobId sets the optional parameter "replaceJobId": DEPRECATED.
// This field is now on the Job message.
func (c *ProjectsJobsCreateCall) ReplaceJobId(replaceJobId string) *ProjectsJobsCreateCall {
	c.urlParams_.Set("replaceJobId", replaceJobId)
	return c
}

// View sets the optional parameter "view": Level of information
// requested in response.
//
// Possible values:
//   "JOB_VIEW_UNKNOWN"
//   "JOB_VIEW_SUMMARY"
//   "JOB_VIEW_ALL"
func (c *ProjectsJobsCreateCall) View(view string) *ProjectsJobsCreateCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsCreateCall) Fields(s ...googleapi.Field) *ProjectsJobsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsCreateCall) Context(ctx context.Context) *ProjectsJobsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.job)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.create" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsJobsCreateCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Creates a dataflow job.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.jobs.create",
	//   "parameterOrder": [
	//     "projectId"
	//   ],
	//   "parameters": {
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "replaceJobId": {
	//       "description": "DEPRECATED. This field is now on the Job message.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Level of information requested in response.",
	//       "enum": [
	//         "JOB_VIEW_UNKNOWN",
	//         "JOB_VIEW_SUMMARY",
	//         "JOB_VIEW_ALL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs",
	//   "request": {
	//     "$ref": "Job"
	//   },
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.get":

type ProjectsJobsGetCall struct {
	s            *Service
	projectId    string
	jobId        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the state of the specified dataflow job.
func (r *ProjectsJobsService) Get(projectId string, jobId string) *ProjectsJobsGetCall {
	c := &ProjectsJobsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	return c
}

// Location sets the optional parameter "location": The location which
// contains this job.
func (c *ProjectsJobsGetCall) Location(location string) *ProjectsJobsGetCall {
	c.urlParams_.Set("location", location)
	return c
}

// View sets the optional parameter "view": Level of information
// requested in response.
//
// Possible values:
//   "JOB_VIEW_UNKNOWN"
//   "JOB_VIEW_SUMMARY"
//   "JOB_VIEW_ALL"
func (c *ProjectsJobsGetCall) View(view string) *ProjectsJobsGetCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsGetCall) Fields(s ...googleapi.Field) *ProjectsJobsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsJobsGetCall) IfNoneMatch(entityTag string) *ProjectsJobsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsGetCall) Context(ctx context.Context) *ProjectsJobsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.get" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsJobsGetCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Gets the state of the specified dataflow job.",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.jobs.get",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "Identifies a single job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Level of information requested in response.",
	//       "enum": [
	//         "JOB_VIEW_UNKNOWN",
	//         "JOB_VIEW_SUMMARY",
	//         "JOB_VIEW_ALL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}",
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.getMetrics":

type ProjectsJobsGetMetricsCall struct {
	s            *Service
	projectId    string
	jobId        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetMetrics: Request the job status.
func (r *ProjectsJobsService) GetMetrics(projectId string, jobId string) *ProjectsJobsGetMetricsCall {
	c := &ProjectsJobsGetMetricsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	return c
}

// Location sets the optional parameter "location": The location which
// contains the job specified by job_id.
func (c *ProjectsJobsGetMetricsCall) Location(location string) *ProjectsJobsGetMetricsCall {
	c.urlParams_.Set("location", location)
	return c
}

// StartTime sets the optional parameter "startTime": Return only metric
// data that has changed since this time. Default is to return all
// information about all metrics for the job.
func (c *ProjectsJobsGetMetricsCall) StartTime(startTime string) *ProjectsJobsGetMetricsCall {
	c.urlParams_.Set("startTime", startTime)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsGetMetricsCall) Fields(s ...googleapi.Field) *ProjectsJobsGetMetricsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsJobsGetMetricsCall) IfNoneMatch(entityTag string) *ProjectsJobsGetMetricsCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsGetMetricsCall) Context(ctx context.Context) *ProjectsJobsGetMetricsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsGetMetricsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsGetMetricsCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}/metrics")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.getMetrics" call.
// Exactly one of *JobMetrics or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *JobMetrics.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsJobsGetMetricsCall) Do(opts ...googleapi.CallOption) (*JobMetrics, error) {
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
	ret := &JobMetrics{
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
	//   "description": "Request the job status.",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.jobs.getMetrics",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "The job to get messages for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains the job specified by job_id.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "A project id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "startTime": {
	//       "description": "Return only metric data that has changed since this time. Default is to return all information about all metrics for the job.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}/metrics",
	//   "response": {
	//     "$ref": "JobMetrics"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.list":

type ProjectsJobsListCall struct {
	s            *Service
	projectId    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List the jobs of a project
func (r *ProjectsJobsService) List(projectId string) *ProjectsJobsListCall {
	c := &ProjectsJobsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	return c
}

// Filter sets the optional parameter "filter": The kind of filter to
// use.
//
// Possible values:
//   "UNKNOWN"
//   "ALL"
//   "TERMINATED"
//   "ACTIVE"
func (c *ProjectsJobsListCall) Filter(filter string) *ProjectsJobsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// Location sets the optional parameter "location": The location which
// contains this job.
func (c *ProjectsJobsListCall) Location(location string) *ProjectsJobsListCall {
	c.urlParams_.Set("location", location)
	return c
}

// PageSize sets the optional parameter "pageSize": If there are many
// jobs, limit response to at most this many. The actual number of jobs
// returned will be the lesser of max_responses and an unspecified
// server-defined limit.
func (c *ProjectsJobsListCall) PageSize(pageSize int64) *ProjectsJobsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Set this to the
// 'next_page_token' field of a previous response to request additional
// results in a long list.
func (c *ProjectsJobsListCall) PageToken(pageToken string) *ProjectsJobsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// View sets the optional parameter "view": Level of information
// requested in response. Default is SUMMARY.
//
// Possible values:
//   "JOB_VIEW_UNKNOWN"
//   "JOB_VIEW_SUMMARY"
//   "JOB_VIEW_ALL"
func (c *ProjectsJobsListCall) View(view string) *ProjectsJobsListCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsListCall) Fields(s ...googleapi.Field) *ProjectsJobsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsJobsListCall) IfNoneMatch(entityTag string) *ProjectsJobsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsListCall) Context(ctx context.Context) *ProjectsJobsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.list" call.
// Exactly one of *ListJobsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListJobsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsListCall) Do(opts ...googleapi.CallOption) (*ListJobsResponse, error) {
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
	ret := &ListJobsResponse{
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
	//   "description": "List the jobs of a project",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.jobs.list",
	//   "parameterOrder": [
	//     "projectId"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "The kind of filter to use.",
	//       "enum": [
	//         "UNKNOWN",
	//         "ALL",
	//         "TERMINATED",
	//         "ACTIVE"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "If there are many jobs, limit response to at most this many. The actual number of jobs returned will be the lesser of max_responses and an unspecified server-defined limit.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Set this to the 'next_page_token' field of a previous response to request additional results in a long list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the jobs.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Level of information requested in response. Default is SUMMARY.",
	//       "enum": [
	//         "JOB_VIEW_UNKNOWN",
	//         "JOB_VIEW_SUMMARY",
	//         "JOB_VIEW_ALL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs",
	//   "response": {
	//     "$ref": "ListJobsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsJobsListCall) Pages(ctx context.Context, f func(*ListJobsResponse) error) error {
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

// method id "dataflow.projects.jobs.update":

type ProjectsJobsUpdateCall struct {
	s          *Service
	projectId  string
	jobId      string
	job        *Job
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates the state of an existing dataflow job.
func (r *ProjectsJobsService) Update(projectId string, jobId string, job *Job) *ProjectsJobsUpdateCall {
	c := &ProjectsJobsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	c.job = job
	return c
}

// Location sets the optional parameter "location": The location which
// contains this job.
func (c *ProjectsJobsUpdateCall) Location(location string) *ProjectsJobsUpdateCall {
	c.urlParams_.Set("location", location)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsUpdateCall) Fields(s ...googleapi.Field) *ProjectsJobsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsUpdateCall) Context(ctx context.Context) *ProjectsJobsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.job)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.update" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsJobsUpdateCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Updates the state of an existing dataflow job.",
	//   "httpMethod": "PUT",
	//   "id": "dataflow.projects.jobs.update",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "Identifies a single job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}",
	//   "request": {
	//     "$ref": "Job"
	//   },
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.debug.getConfig":

type ProjectsJobsDebugGetConfigCall struct {
	s                     *Service
	projectId             string
	jobId                 string
	getdebugconfigrequest *GetDebugConfigRequest
	urlParams_            gensupport.URLParams
	ctx_                  context.Context
	header_               http.Header
}

// GetConfig: Get encoded debug configuration for component. Not
// cacheable.
func (r *ProjectsJobsDebugService) GetConfig(projectId string, jobId string, getdebugconfigrequest *GetDebugConfigRequest) *ProjectsJobsDebugGetConfigCall {
	c := &ProjectsJobsDebugGetConfigCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	c.getdebugconfigrequest = getdebugconfigrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsDebugGetConfigCall) Fields(s ...googleapi.Field) *ProjectsJobsDebugGetConfigCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsDebugGetConfigCall) Context(ctx context.Context) *ProjectsJobsDebugGetConfigCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsDebugGetConfigCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsDebugGetConfigCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.getdebugconfigrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}/debug/getConfig")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.debug.getConfig" call.
// Exactly one of *GetDebugConfigResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GetDebugConfigResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsDebugGetConfigCall) Do(opts ...googleapi.CallOption) (*GetDebugConfigResponse, error) {
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
	ret := &GetDebugConfigResponse{
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
	//   "description": "Get encoded debug configuration for component. Not cacheable.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.jobs.debug.getConfig",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "The job id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}/debug/getConfig",
	//   "request": {
	//     "$ref": "GetDebugConfigRequest"
	//   },
	//   "response": {
	//     "$ref": "GetDebugConfigResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.debug.sendCapture":

type ProjectsJobsDebugSendCaptureCall struct {
	s                       *Service
	projectId               string
	jobId                   string
	senddebugcapturerequest *SendDebugCaptureRequest
	urlParams_              gensupport.URLParams
	ctx_                    context.Context
	header_                 http.Header
}

// SendCapture: Send encoded debug capture data for component.
func (r *ProjectsJobsDebugService) SendCapture(projectId string, jobId string, senddebugcapturerequest *SendDebugCaptureRequest) *ProjectsJobsDebugSendCaptureCall {
	c := &ProjectsJobsDebugSendCaptureCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	c.senddebugcapturerequest = senddebugcapturerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsDebugSendCaptureCall) Fields(s ...googleapi.Field) *ProjectsJobsDebugSendCaptureCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsDebugSendCaptureCall) Context(ctx context.Context) *ProjectsJobsDebugSendCaptureCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsDebugSendCaptureCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsDebugSendCaptureCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.senddebugcapturerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}/debug/sendCapture")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.debug.sendCapture" call.
// Exactly one of *SendDebugCaptureResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *SendDebugCaptureResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsDebugSendCaptureCall) Do(opts ...googleapi.CallOption) (*SendDebugCaptureResponse, error) {
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
	ret := &SendDebugCaptureResponse{
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
	//   "description": "Send encoded debug capture data for component.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.jobs.debug.sendCapture",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "The job id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}/debug/sendCapture",
	//   "request": {
	//     "$ref": "SendDebugCaptureRequest"
	//   },
	//   "response": {
	//     "$ref": "SendDebugCaptureResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.messages.list":

type ProjectsJobsMessagesListCall struct {
	s            *Service
	projectId    string
	jobId        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Request the job status.
func (r *ProjectsJobsMessagesService) List(projectId string, jobId string) *ProjectsJobsMessagesListCall {
	c := &ProjectsJobsMessagesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	return c
}

// EndTime sets the optional parameter "endTime": Return only messages
// with timestamps < end_time. The default is now (i.e. return up to the
// latest messages available).
func (c *ProjectsJobsMessagesListCall) EndTime(endTime string) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("endTime", endTime)
	return c
}

// Location sets the optional parameter "location": The location which
// contains the job specified by job_id.
func (c *ProjectsJobsMessagesListCall) Location(location string) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("location", location)
	return c
}

// MinimumImportance sets the optional parameter "minimumImportance":
// Filter to only get messages with importance >= level
//
// Possible values:
//   "JOB_MESSAGE_IMPORTANCE_UNKNOWN"
//   "JOB_MESSAGE_DEBUG"
//   "JOB_MESSAGE_DETAILED"
//   "JOB_MESSAGE_BASIC"
//   "JOB_MESSAGE_WARNING"
//   "JOB_MESSAGE_ERROR"
func (c *ProjectsJobsMessagesListCall) MinimumImportance(minimumImportance string) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("minimumImportance", minimumImportance)
	return c
}

// PageSize sets the optional parameter "pageSize": If specified,
// determines the maximum number of messages to return. If unspecified,
// the service may choose an appropriate default, or may return an
// arbitrarily large number of results.
func (c *ProjectsJobsMessagesListCall) PageSize(pageSize int64) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": If supplied, this
// should be the value of next_page_token returned by an earlier call.
// This will cause the next page of results to be returned.
func (c *ProjectsJobsMessagesListCall) PageToken(pageToken string) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// StartTime sets the optional parameter "startTime": If specified,
// return only messages with timestamps >= start_time. The default is
// the job creation time (i.e. beginning of messages).
func (c *ProjectsJobsMessagesListCall) StartTime(startTime string) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("startTime", startTime)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsMessagesListCall) Fields(s ...googleapi.Field) *ProjectsJobsMessagesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsJobsMessagesListCall) IfNoneMatch(entityTag string) *ProjectsJobsMessagesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsMessagesListCall) Context(ctx context.Context) *ProjectsJobsMessagesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsMessagesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsMessagesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}/messages")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.messages.list" call.
// Exactly one of *ListJobMessagesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListJobMessagesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsMessagesListCall) Do(opts ...googleapi.CallOption) (*ListJobMessagesResponse, error) {
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
	ret := &ListJobMessagesResponse{
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
	//   "description": "Request the job status.",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.jobs.messages.list",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "endTime": {
	//       "description": "Return only messages with timestamps \u003c end_time. The default is now (i.e. return up to the latest messages available).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "jobId": {
	//       "description": "The job to get messages about.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains the job specified by job_id.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "minimumImportance": {
	//       "description": "Filter to only get messages with importance \u003e= level",
	//       "enum": [
	//         "JOB_MESSAGE_IMPORTANCE_UNKNOWN",
	//         "JOB_MESSAGE_DEBUG",
	//         "JOB_MESSAGE_DETAILED",
	//         "JOB_MESSAGE_BASIC",
	//         "JOB_MESSAGE_WARNING",
	//         "JOB_MESSAGE_ERROR"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "If specified, determines the maximum number of messages to return. If unspecified, the service may choose an appropriate default, or may return an arbitrarily large number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "If supplied, this should be the value of next_page_token returned by an earlier call. This will cause the next page of results to be returned.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "A project id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "startTime": {
	//       "description": "If specified, return only messages with timestamps \u003e= start_time. The default is the job creation time (i.e. beginning of messages).",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}/messages",
	//   "response": {
	//     "$ref": "ListJobMessagesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsJobsMessagesListCall) Pages(ctx context.Context, f func(*ListJobMessagesResponse) error) error {
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

// method id "dataflow.projects.jobs.workItems.lease":

type ProjectsJobsWorkItemsLeaseCall struct {
	s                    *Service
	projectId            string
	jobId                string
	leaseworkitemrequest *LeaseWorkItemRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Lease: Leases a dataflow WorkItem to run.
func (r *ProjectsJobsWorkItemsService) Lease(projectId string, jobId string, leaseworkitemrequest *LeaseWorkItemRequest) *ProjectsJobsWorkItemsLeaseCall {
	c := &ProjectsJobsWorkItemsLeaseCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	c.leaseworkitemrequest = leaseworkitemrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsWorkItemsLeaseCall) Fields(s ...googleapi.Field) *ProjectsJobsWorkItemsLeaseCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsWorkItemsLeaseCall) Context(ctx context.Context) *ProjectsJobsWorkItemsLeaseCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsWorkItemsLeaseCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsWorkItemsLeaseCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.leaseworkitemrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}/workItems:lease")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.workItems.lease" call.
// Exactly one of *LeaseWorkItemResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *LeaseWorkItemResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsWorkItemsLeaseCall) Do(opts ...googleapi.CallOption) (*LeaseWorkItemResponse, error) {
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
	ret := &LeaseWorkItemResponse{
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
	//   "description": "Leases a dataflow WorkItem to run.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.jobs.workItems.lease",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "Identifies the workflow job this worker belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "Identifies the project this worker belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}/workItems:lease",
	//   "request": {
	//     "$ref": "LeaseWorkItemRequest"
	//   },
	//   "response": {
	//     "$ref": "LeaseWorkItemResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.jobs.workItems.reportStatus":

type ProjectsJobsWorkItemsReportStatusCall struct {
	s                           *Service
	projectId                   string
	jobId                       string
	reportworkitemstatusrequest *ReportWorkItemStatusRequest
	urlParams_                  gensupport.URLParams
	ctx_                        context.Context
	header_                     http.Header
}

// ReportStatus: Reports the status of dataflow WorkItems leased by a
// worker.
func (r *ProjectsJobsWorkItemsService) ReportStatus(projectId string, jobId string, reportworkitemstatusrequest *ReportWorkItemStatusRequest) *ProjectsJobsWorkItemsReportStatusCall {
	c := &ProjectsJobsWorkItemsReportStatusCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.jobId = jobId
	c.reportworkitemstatusrequest = reportworkitemstatusrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsWorkItemsReportStatusCall) Fields(s ...googleapi.Field) *ProjectsJobsWorkItemsReportStatusCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsWorkItemsReportStatusCall) Context(ctx context.Context) *ProjectsJobsWorkItemsReportStatusCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsWorkItemsReportStatusCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsWorkItemsReportStatusCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.reportworkitemstatusrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/jobs/{jobId}/workItems:reportStatus")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.jobs.workItems.reportStatus" call.
// Exactly one of *ReportWorkItemStatusResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ReportWorkItemStatusResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsWorkItemsReportStatusCall) Do(opts ...googleapi.CallOption) (*ReportWorkItemStatusResponse, error) {
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
	ret := &ReportWorkItemStatusResponse{
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
	//   "description": "Reports the status of dataflow WorkItems leased by a worker.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.jobs.workItems.reportStatus",
	//   "parameterOrder": [
	//     "projectId",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "The job which the WorkItem is part of.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the WorkItem's job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/jobs/{jobId}/workItems:reportStatus",
	//   "request": {
	//     "$ref": "ReportWorkItemStatusRequest"
	//   },
	//   "response": {
	//     "$ref": "ReportWorkItemStatusResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.locations.jobs.create":

type ProjectsLocationsJobsCreateCall struct {
	s          *Service
	projectId  string
	location   string
	job        *Job
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a dataflow job.
func (r *ProjectsLocationsJobsService) Create(projectId string, location string, job *Job) *ProjectsLocationsJobsCreateCall {
	c := &ProjectsLocationsJobsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.job = job
	return c
}

// ReplaceJobId sets the optional parameter "replaceJobId": DEPRECATED.
// This field is now on the Job message.
func (c *ProjectsLocationsJobsCreateCall) ReplaceJobId(replaceJobId string) *ProjectsLocationsJobsCreateCall {
	c.urlParams_.Set("replaceJobId", replaceJobId)
	return c
}

// View sets the optional parameter "view": Level of information
// requested in response.
//
// Possible values:
//   "JOB_VIEW_UNKNOWN"
//   "JOB_VIEW_SUMMARY"
//   "JOB_VIEW_ALL"
func (c *ProjectsLocationsJobsCreateCall) View(view string) *ProjectsLocationsJobsCreateCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsCreateCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsCreateCall) Context(ctx context.Context) *ProjectsLocationsJobsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.job)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.create" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsLocationsJobsCreateCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Creates a dataflow job.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.locations.jobs.create",
	//   "parameterOrder": [
	//     "projectId",
	//     "location"
	//   ],
	//   "parameters": {
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "replaceJobId": {
	//       "description": "DEPRECATED. This field is now on the Job message.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Level of information requested in response.",
	//       "enum": [
	//         "JOB_VIEW_UNKNOWN",
	//         "JOB_VIEW_SUMMARY",
	//         "JOB_VIEW_ALL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs",
	//   "request": {
	//     "$ref": "Job"
	//   },
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.locations.jobs.get":

type ProjectsLocationsJobsGetCall struct {
	s            *Service
	projectId    string
	location     string
	jobId        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the state of the specified dataflow job.
func (r *ProjectsLocationsJobsService) Get(projectId string, location string, jobId string) *ProjectsLocationsJobsGetCall {
	c := &ProjectsLocationsJobsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.jobId = jobId
	return c
}

// View sets the optional parameter "view": Level of information
// requested in response.
//
// Possible values:
//   "JOB_VIEW_UNKNOWN"
//   "JOB_VIEW_SUMMARY"
//   "JOB_VIEW_ALL"
func (c *ProjectsLocationsJobsGetCall) View(view string) *ProjectsLocationsJobsGetCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsGetCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsJobsGetCall) IfNoneMatch(entityTag string) *ProjectsLocationsJobsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsGetCall) Context(ctx context.Context) *ProjectsLocationsJobsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.get" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsLocationsJobsGetCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Gets the state of the specified dataflow job.",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.locations.jobs.get",
	//   "parameterOrder": [
	//     "projectId",
	//     "location",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "Identifies a single job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Level of information requested in response.",
	//       "enum": [
	//         "JOB_VIEW_UNKNOWN",
	//         "JOB_VIEW_SUMMARY",
	//         "JOB_VIEW_ALL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}",
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.locations.jobs.getMetrics":

type ProjectsLocationsJobsGetMetricsCall struct {
	s            *Service
	projectId    string
	location     string
	jobId        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetMetrics: Request the job status.
func (r *ProjectsLocationsJobsService) GetMetrics(projectId string, location string, jobId string) *ProjectsLocationsJobsGetMetricsCall {
	c := &ProjectsLocationsJobsGetMetricsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.jobId = jobId
	return c
}

// StartTime sets the optional parameter "startTime": Return only metric
// data that has changed since this time. Default is to return all
// information about all metrics for the job.
func (c *ProjectsLocationsJobsGetMetricsCall) StartTime(startTime string) *ProjectsLocationsJobsGetMetricsCall {
	c.urlParams_.Set("startTime", startTime)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsGetMetricsCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsGetMetricsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsJobsGetMetricsCall) IfNoneMatch(entityTag string) *ProjectsLocationsJobsGetMetricsCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsGetMetricsCall) Context(ctx context.Context) *ProjectsLocationsJobsGetMetricsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsGetMetricsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsGetMetricsCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/metrics")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.getMetrics" call.
// Exactly one of *JobMetrics or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *JobMetrics.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsLocationsJobsGetMetricsCall) Do(opts ...googleapi.CallOption) (*JobMetrics, error) {
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
	ret := &JobMetrics{
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
	//   "description": "Request the job status.",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.locations.jobs.getMetrics",
	//   "parameterOrder": [
	//     "projectId",
	//     "location",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "The job to get messages for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains the job specified by job_id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "A project id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "startTime": {
	//       "description": "Return only metric data that has changed since this time. Default is to return all information about all metrics for the job.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/metrics",
	//   "response": {
	//     "$ref": "JobMetrics"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.locations.jobs.list":

type ProjectsLocationsJobsListCall struct {
	s            *Service
	projectId    string
	location     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List the jobs of a project
func (r *ProjectsLocationsJobsService) List(projectId string, location string) *ProjectsLocationsJobsListCall {
	c := &ProjectsLocationsJobsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	return c
}

// Filter sets the optional parameter "filter": The kind of filter to
// use.
//
// Possible values:
//   "UNKNOWN"
//   "ALL"
//   "TERMINATED"
//   "ACTIVE"
func (c *ProjectsLocationsJobsListCall) Filter(filter string) *ProjectsLocationsJobsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": If there are many
// jobs, limit response to at most this many. The actual number of jobs
// returned will be the lesser of max_responses and an unspecified
// server-defined limit.
func (c *ProjectsLocationsJobsListCall) PageSize(pageSize int64) *ProjectsLocationsJobsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Set this to the
// 'next_page_token' field of a previous response to request additional
// results in a long list.
func (c *ProjectsLocationsJobsListCall) PageToken(pageToken string) *ProjectsLocationsJobsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// View sets the optional parameter "view": Level of information
// requested in response. Default is SUMMARY.
//
// Possible values:
//   "JOB_VIEW_UNKNOWN"
//   "JOB_VIEW_SUMMARY"
//   "JOB_VIEW_ALL"
func (c *ProjectsLocationsJobsListCall) View(view string) *ProjectsLocationsJobsListCall {
	c.urlParams_.Set("view", view)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsListCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsJobsListCall) IfNoneMatch(entityTag string) *ProjectsLocationsJobsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsListCall) Context(ctx context.Context) *ProjectsLocationsJobsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.list" call.
// Exactly one of *ListJobsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListJobsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsJobsListCall) Do(opts ...googleapi.CallOption) (*ListJobsResponse, error) {
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
	ret := &ListJobsResponse{
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
	//   "description": "List the jobs of a project",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.locations.jobs.list",
	//   "parameterOrder": [
	//     "projectId",
	//     "location"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "The kind of filter to use.",
	//       "enum": [
	//         "UNKNOWN",
	//         "ALL",
	//         "TERMINATED",
	//         "ACTIVE"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "If there are many jobs, limit response to at most this many. The actual number of jobs returned will be the lesser of max_responses and an unspecified server-defined limit.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Set this to the 'next_page_token' field of a previous response to request additional results in a long list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the jobs.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "view": {
	//       "description": "Level of information requested in response. Default is SUMMARY.",
	//       "enum": [
	//         "JOB_VIEW_UNKNOWN",
	//         "JOB_VIEW_SUMMARY",
	//         "JOB_VIEW_ALL"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs",
	//   "response": {
	//     "$ref": "ListJobsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsLocationsJobsListCall) Pages(ctx context.Context, f func(*ListJobsResponse) error) error {
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

// method id "dataflow.projects.locations.jobs.update":

type ProjectsLocationsJobsUpdateCall struct {
	s          *Service
	projectId  string
	location   string
	jobId      string
	job        *Job
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates the state of an existing dataflow job.
func (r *ProjectsLocationsJobsService) Update(projectId string, location string, jobId string, job *Job) *ProjectsLocationsJobsUpdateCall {
	c := &ProjectsLocationsJobsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.jobId = jobId
	c.job = job
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsUpdateCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsUpdateCall) Context(ctx context.Context) *ProjectsLocationsJobsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.job)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.update" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsLocationsJobsUpdateCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Updates the state of an existing dataflow job.",
	//   "httpMethod": "PUT",
	//   "id": "dataflow.projects.locations.jobs.update",
	//   "parameterOrder": [
	//     "projectId",
	//     "location",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "Identifies a single job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains this job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}",
	//   "request": {
	//     "$ref": "Job"
	//   },
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.locations.jobs.messages.list":

type ProjectsLocationsJobsMessagesListCall struct {
	s            *Service
	projectId    string
	location     string
	jobId        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Request the job status.
func (r *ProjectsLocationsJobsMessagesService) List(projectId string, location string, jobId string) *ProjectsLocationsJobsMessagesListCall {
	c := &ProjectsLocationsJobsMessagesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.jobId = jobId
	return c
}

// EndTime sets the optional parameter "endTime": Return only messages
// with timestamps < end_time. The default is now (i.e. return up to the
// latest messages available).
func (c *ProjectsLocationsJobsMessagesListCall) EndTime(endTime string) *ProjectsLocationsJobsMessagesListCall {
	c.urlParams_.Set("endTime", endTime)
	return c
}

// MinimumImportance sets the optional parameter "minimumImportance":
// Filter to only get messages with importance >= level
//
// Possible values:
//   "JOB_MESSAGE_IMPORTANCE_UNKNOWN"
//   "JOB_MESSAGE_DEBUG"
//   "JOB_MESSAGE_DETAILED"
//   "JOB_MESSAGE_BASIC"
//   "JOB_MESSAGE_WARNING"
//   "JOB_MESSAGE_ERROR"
func (c *ProjectsLocationsJobsMessagesListCall) MinimumImportance(minimumImportance string) *ProjectsLocationsJobsMessagesListCall {
	c.urlParams_.Set("minimumImportance", minimumImportance)
	return c
}

// PageSize sets the optional parameter "pageSize": If specified,
// determines the maximum number of messages to return. If unspecified,
// the service may choose an appropriate default, or may return an
// arbitrarily large number of results.
func (c *ProjectsLocationsJobsMessagesListCall) PageSize(pageSize int64) *ProjectsLocationsJobsMessagesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": If supplied, this
// should be the value of next_page_token returned by an earlier call.
// This will cause the next page of results to be returned.
func (c *ProjectsLocationsJobsMessagesListCall) PageToken(pageToken string) *ProjectsLocationsJobsMessagesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// StartTime sets the optional parameter "startTime": If specified,
// return only messages with timestamps >= start_time. The default is
// the job creation time (i.e. beginning of messages).
func (c *ProjectsLocationsJobsMessagesListCall) StartTime(startTime string) *ProjectsLocationsJobsMessagesListCall {
	c.urlParams_.Set("startTime", startTime)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsMessagesListCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsMessagesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsJobsMessagesListCall) IfNoneMatch(entityTag string) *ProjectsLocationsJobsMessagesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsMessagesListCall) Context(ctx context.Context) *ProjectsLocationsJobsMessagesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsMessagesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsMessagesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/messages")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.messages.list" call.
// Exactly one of *ListJobMessagesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListJobMessagesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsJobsMessagesListCall) Do(opts ...googleapi.CallOption) (*ListJobMessagesResponse, error) {
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
	ret := &ListJobMessagesResponse{
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
	//   "description": "Request the job status.",
	//   "httpMethod": "GET",
	//   "id": "dataflow.projects.locations.jobs.messages.list",
	//   "parameterOrder": [
	//     "projectId",
	//     "location",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "endTime": {
	//       "description": "Return only messages with timestamps \u003c end_time. The default is now (i.e. return up to the latest messages available).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "jobId": {
	//       "description": "The job to get messages about.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains the job specified by job_id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "minimumImportance": {
	//       "description": "Filter to only get messages with importance \u003e= level",
	//       "enum": [
	//         "JOB_MESSAGE_IMPORTANCE_UNKNOWN",
	//         "JOB_MESSAGE_DEBUG",
	//         "JOB_MESSAGE_DETAILED",
	//         "JOB_MESSAGE_BASIC",
	//         "JOB_MESSAGE_WARNING",
	//         "JOB_MESSAGE_ERROR"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "If specified, determines the maximum number of messages to return. If unspecified, the service may choose an appropriate default, or may return an arbitrarily large number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "If supplied, this should be the value of next_page_token returned by an earlier call. This will cause the next page of results to be returned.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "A project id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "startTime": {
	//       "description": "If specified, return only messages with timestamps \u003e= start_time. The default is the job creation time (i.e. beginning of messages).",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/messages",
	//   "response": {
	//     "$ref": "ListJobMessagesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsLocationsJobsMessagesListCall) Pages(ctx context.Context, f func(*ListJobMessagesResponse) error) error {
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

// method id "dataflow.projects.locations.jobs.workItems.lease":

type ProjectsLocationsJobsWorkItemsLeaseCall struct {
	s                    *Service
	projectId            string
	location             string
	jobId                string
	leaseworkitemrequest *LeaseWorkItemRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Lease: Leases a dataflow WorkItem to run.
func (r *ProjectsLocationsJobsWorkItemsService) Lease(projectId string, location string, jobId string, leaseworkitemrequest *LeaseWorkItemRequest) *ProjectsLocationsJobsWorkItemsLeaseCall {
	c := &ProjectsLocationsJobsWorkItemsLeaseCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.jobId = jobId
	c.leaseworkitemrequest = leaseworkitemrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsWorkItemsLeaseCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsWorkItemsLeaseCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsWorkItemsLeaseCall) Context(ctx context.Context) *ProjectsLocationsJobsWorkItemsLeaseCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsWorkItemsLeaseCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsWorkItemsLeaseCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.leaseworkitemrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/workItems:lease")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.workItems.lease" call.
// Exactly one of *LeaseWorkItemResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *LeaseWorkItemResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsJobsWorkItemsLeaseCall) Do(opts ...googleapi.CallOption) (*LeaseWorkItemResponse, error) {
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
	ret := &LeaseWorkItemResponse{
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
	//   "description": "Leases a dataflow WorkItem to run.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.locations.jobs.workItems.lease",
	//   "parameterOrder": [
	//     "projectId",
	//     "location",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "Identifies the workflow job this worker belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains the WorkItem's job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "Identifies the project this worker belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/workItems:lease",
	//   "request": {
	//     "$ref": "LeaseWorkItemRequest"
	//   },
	//   "response": {
	//     "$ref": "LeaseWorkItemResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.locations.jobs.workItems.reportStatus":

type ProjectsLocationsJobsWorkItemsReportStatusCall struct {
	s                           *Service
	projectId                   string
	location                    string
	jobId                       string
	reportworkitemstatusrequest *ReportWorkItemStatusRequest
	urlParams_                  gensupport.URLParams
	ctx_                        context.Context
	header_                     http.Header
}

// ReportStatus: Reports the status of dataflow WorkItems leased by a
// worker.
func (r *ProjectsLocationsJobsWorkItemsService) ReportStatus(projectId string, location string, jobId string, reportworkitemstatusrequest *ReportWorkItemStatusRequest) *ProjectsLocationsJobsWorkItemsReportStatusCall {
	c := &ProjectsLocationsJobsWorkItemsReportStatusCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.location = location
	c.jobId = jobId
	c.reportworkitemstatusrequest = reportworkitemstatusrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsJobsWorkItemsReportStatusCall) Fields(s ...googleapi.Field) *ProjectsLocationsJobsWorkItemsReportStatusCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsJobsWorkItemsReportStatusCall) Context(ctx context.Context) *ProjectsLocationsJobsWorkItemsReportStatusCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsJobsWorkItemsReportStatusCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsJobsWorkItemsReportStatusCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.reportworkitemstatusrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/workItems:reportStatus")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
		"location":  c.location,
		"jobId":     c.jobId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.locations.jobs.workItems.reportStatus" call.
// Exactly one of *ReportWorkItemStatusResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ReportWorkItemStatusResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsJobsWorkItemsReportStatusCall) Do(opts ...googleapi.CallOption) (*ReportWorkItemStatusResponse, error) {
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
	ret := &ReportWorkItemStatusResponse{
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
	//   "description": "Reports the status of dataflow WorkItems leased by a worker.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.locations.jobs.workItems.reportStatus",
	//   "parameterOrder": [
	//     "projectId",
	//     "location",
	//     "jobId"
	//   ],
	//   "parameters": {
	//     "jobId": {
	//       "description": "The job which the WorkItem is part of.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "location": {
	//       "description": "The location which contains the WorkItem's job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project which owns the WorkItem's job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/locations/{location}/jobs/{jobId}/workItems:reportStatus",
	//   "request": {
	//     "$ref": "ReportWorkItemStatusRequest"
	//   },
	//   "response": {
	//     "$ref": "ReportWorkItemStatusResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "dataflow.projects.templates.create":

type ProjectsTemplatesCreateCall struct {
	s                            *Service
	projectId                    string
	createjobfromtemplaterequest *CreateJobFromTemplateRequest
	urlParams_                   gensupport.URLParams
	ctx_                         context.Context
	header_                      http.Header
}

// Create: Creates a dataflow job from a template.
func (r *ProjectsTemplatesService) Create(projectId string, createjobfromtemplaterequest *CreateJobFromTemplateRequest) *ProjectsTemplatesCreateCall {
	c := &ProjectsTemplatesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.projectId = projectId
	c.createjobfromtemplaterequest = createjobfromtemplaterequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsTemplatesCreateCall) Fields(s ...googleapi.Field) *ProjectsTemplatesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsTemplatesCreateCall) Context(ctx context.Context) *ProjectsTemplatesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsTemplatesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsTemplatesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.createjobfromtemplaterequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1b3/projects/{projectId}/templates")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.projectId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dataflow.projects.templates.create" call.
// Exactly one of *Job or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Job.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsTemplatesCreateCall) Do(opts ...googleapi.CallOption) (*Job, error) {
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
	ret := &Job{
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
	//   "description": "Creates a dataflow job from a template.",
	//   "httpMethod": "POST",
	//   "id": "dataflow.projects.templates.create",
	//   "parameterOrder": [
	//     "projectId"
	//   ],
	//   "parameters": {
	//     "projectId": {
	//       "description": "The project which owns the job.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1b3/projects/{projectId}/templates",
	//   "request": {
	//     "$ref": "CreateJobFromTemplateRequest"
	//   },
	//   "response": {
	//     "$ref": "Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

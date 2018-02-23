// Package ml provides access to the Google Cloud Machine Learning.
//
// See https://cloud.google.com/ml/
//
// Usage example:
//
//   import "google.golang.org/api/ml/v1beta1"
//   ...
//   mlService, err := ml.New(oauthHttpClient)
package ml // import "google.golang.org/api/ml/v1beta1"

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

const apiId = "ml:v1beta1"
const apiName = "ml"
const apiVersion = "v1beta1"
const basePath = "https://ml.googleapis.com/"

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
	rs.Jobs = NewProjectsJobsService(s)
	rs.Models = NewProjectsModelsService(s)
	rs.Operations = NewProjectsOperationsService(s)
	return rs
}

type ProjectsService struct {
	s *Service

	Jobs *ProjectsJobsService

	Models *ProjectsModelsService

	Operations *ProjectsOperationsService
}

func NewProjectsJobsService(s *Service) *ProjectsJobsService {
	rs := &ProjectsJobsService{s: s}
	return rs
}

type ProjectsJobsService struct {
	s *Service
}

func NewProjectsModelsService(s *Service) *ProjectsModelsService {
	rs := &ProjectsModelsService{s: s}
	rs.Versions = NewProjectsModelsVersionsService(s)
	return rs
}

type ProjectsModelsService struct {
	s *Service

	Versions *ProjectsModelsVersionsService
}

func NewProjectsModelsVersionsService(s *Service) *ProjectsModelsVersionsService {
	rs := &ProjectsModelsVersionsService{s: s}
	return rs
}

type ProjectsModelsVersionsService struct {
	s *Service
}

func NewProjectsOperationsService(s *Service) *ProjectsOperationsService {
	rs := &ProjectsOperationsService{s: s}
	return rs
}

type ProjectsOperationsService struct {
	s *Service
}

// GoogleApi__HttpBody: Message that represents an arbitrary HTTP body.
// It should only be used for
// payload formats that can't be represented as JSON, such as raw binary
// or
// an HTML page.
//
//
// This message can be used both in streaming and non-streaming API
// methods in
// the request as well as the response.
//
// It can be used as a top-level request field, which is convenient if
// one
// wants to extract parameters from either the URL or HTTP template into
// the
// request fields and also want access to the raw HTTP body.
//
// Example:
//
//     message GetResourceRequest {
//       // A unique request id.
//       string request_id = 1;
//
//       // The raw HTTP body is bound to this field.
//       google.api.HttpBody http_body = 2;
//     }
//
//     service ResourceService {
//       rpc GetResource(GetResourceRequest) returns
// (google.api.HttpBody);
//       rpc UpdateResource(google.api.HttpBody) returns
// (google.protobuf.Empty);
//     }
//
// Example with streaming methods:
//
//     service CaldavService {
//       rpc GetCalendar(stream google.api.HttpBody)
//         returns (stream google.api.HttpBody);
//       rpc UpdateCalendar(stream google.api.HttpBody)
//         returns (stream google.api.HttpBody);
//     }
//
// Use of this type only changes how the request and response bodies
// are
// handled, all other features will continue to work unchanged.
type GoogleApi__HttpBody struct {
	// ContentType: The HTTP Content-Type string representing the content
	// type of the body.
	ContentType string `json:"contentType,omitempty"`

	// Data: HTTP body binary data.
	Data string `json:"data,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ContentType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContentType") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleApi__HttpBody) MarshalJSON() ([]byte, error) {
	type noMethod GoogleApi__HttpBody
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1HyperparameterOutputHyperparameterMetric: An
// observed value of a metric.
type GoogleCloudMlV1beta1HyperparameterOutputHyperparameterMetric struct {
	// ObjectiveValue: The objective value at this training step.
	ObjectiveValue float64 `json:"objectiveValue,omitempty"`

	// TrainingStep: The global training step for this metric.
	TrainingStep int64 `json:"trainingStep,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "ObjectiveValue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectiveValue") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1HyperparameterOutputHyperparameterMetric) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1HyperparameterOutputHyperparameterMetric
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__CancelJobRequest: Request message for the
// CancelJob method.
type GoogleCloudMlV1beta1__CancelJobRequest struct {
}

// GoogleCloudMlV1beta1__GetConfigResponse: Returns service account
// information associated with a project.
type GoogleCloudMlV1beta1__GetConfigResponse struct {
	// ServiceAccount: The service account Cloud ML uses to access resources
	// in the project.
	ServiceAccount string `json:"serviceAccount,omitempty"`

	// ServiceAccountProject: The project number for `service_account`.
	ServiceAccountProject int64 `json:"serviceAccountProject,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ServiceAccount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ServiceAccount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__GetConfigResponse) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__GetConfigResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__HyperparameterOutput: Represents the result of
// a single hyperparameter tuning trial from a
// training job. The TrainingOutput object that is returned on
// successful
// completion of a training job with hyperparameter tuning includes a
// list
// of HyperparameterOutput objects, one for each successful trial.
type GoogleCloudMlV1beta1__HyperparameterOutput struct {
	// AllMetrics: All recorded object metrics for this trial.
	AllMetrics []*GoogleCloudMlV1beta1HyperparameterOutputHyperparameterMetric `json:"allMetrics,omitempty"`

	// FinalMetric: The final objective metric seen for this trial.
	FinalMetric *GoogleCloudMlV1beta1HyperparameterOutputHyperparameterMetric `json:"finalMetric,omitempty"`

	// Hyperparameters: The hyperparameters given to this trial.
	Hyperparameters map[string]string `json:"hyperparameters,omitempty"`

	// TrialId: The trial id for these results.
	TrialId string `json:"trialId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AllMetrics") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllMetrics") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__HyperparameterOutput) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__HyperparameterOutput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__HyperparameterSpec: Represents a set of
// hyperparameters to optimize.
type GoogleCloudMlV1beta1__HyperparameterSpec struct {
	// Goal: Required. The type of goal to use for tuning. Available types
	// are
	// `MAXIMIZE` and `MINIMIZE`.
	//
	// Defaults to `MAXIMIZE`.
	//
	// Possible values:
	//   "GOAL_TYPE_UNSPECIFIED" - Goal Type will default to maximize.
	//   "MAXIMIZE" - Maximize the goal metric.
	//   "MINIMIZE" - Minimize the goal metric.
	Goal string `json:"goal,omitempty"`

	// MaxParallelTrials: Optional. The number of training trials to run
	// concurrently.
	// You can reduce the time it takes to perform hyperparameter tuning by
	// adding
	// trials in parallel. However, each trail only benefits from the
	// information
	// gained in completed trials. That means that a trial does not get
	// access to
	// the results of trials running at the same time, which could reduce
	// the
	// quality of the overall optimization.
	//
	// Each trial will use the same scale tier and machine types.
	//
	// Defaults to one.
	MaxParallelTrials int64 `json:"maxParallelTrials,omitempty"`

	// MaxTrials: Optional. How many training trials should be attempted to
	// optimize
	// the specified hyperparameters.
	//
	// Defaults to one.
	MaxTrials int64 `json:"maxTrials,omitempty"`

	// Params: Required. The set of parameters to tune.
	Params []*GoogleCloudMlV1beta1__ParameterSpec `json:"params,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Goal") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Goal") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__HyperparameterSpec) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__HyperparameterSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__Job: Represents a training or prediction job.
type GoogleCloudMlV1beta1__Job struct {
	// CreateTime: Output only. When the job was created.
	CreateTime string `json:"createTime,omitempty"`

	// EndTime: Output only. When the job processing was completed.
	EndTime string `json:"endTime,omitempty"`

	// ErrorMessage: Output only. The details of a failure or a
	// cancellation.
	ErrorMessage string `json:"errorMessage,omitempty"`

	// JobId: Required. The user-specified id of the job.
	JobId string `json:"jobId,omitempty"`

	// PredictionInput: Input parameters to create a prediction job.
	PredictionInput *GoogleCloudMlV1beta1__PredictionInput `json:"predictionInput,omitempty"`

	// PredictionOutput: The current prediction job result.
	PredictionOutput *GoogleCloudMlV1beta1__PredictionOutput `json:"predictionOutput,omitempty"`

	// StartTime: Output only. When the job processing was started.
	StartTime string `json:"startTime,omitempty"`

	// State: Output only. The detailed state of a job.
	//
	// Possible values:
	//   "STATE_UNSPECIFIED" - The job state is unspecified.
	//   "QUEUED" - The job has been just created and processing has not yet
	// begun.
	//   "PREPARING" - The service is preparing to run the job.
	//   "RUNNING" - The job is in progress.
	//   "SUCCEEDED" - The job completed successfully.
	//   "FAILED" - The job failed.
	// `error_message` should contain the details of the failure.
	//   "CANCELLING" - The job is being cancelled.
	// `error_message` should describe the reason for the cancellation.
	//   "CANCELLED" - The job has been cancelled.
	// `error_message` should describe the reason for the cancellation.
	State string `json:"state,omitempty"`

	// TrainingInput: Input parameters to create a training job.
	TrainingInput *GoogleCloudMlV1beta1__TrainingInput `json:"trainingInput,omitempty"`

	// TrainingOutput: The current training job result.
	TrainingOutput *GoogleCloudMlV1beta1__TrainingOutput `json:"trainingOutput,omitempty"`

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

func (s *GoogleCloudMlV1beta1__Job) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__Job
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__ListJobsResponse: Response message for the
// ListJobs method.
type GoogleCloudMlV1beta1__ListJobsResponse struct {
	// Jobs: The list of jobs.
	Jobs []*GoogleCloudMlV1beta1__Job `json:"jobs,omitempty"`

	// NextPageToken: Optional. Pass this token as the `page_token` field of
	// the request for a
	// subsequent call.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Jobs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Jobs") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__ListJobsResponse) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__ListJobsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__ListModelsResponse: Response message for the
// ListModels method.
type GoogleCloudMlV1beta1__ListModelsResponse struct {
	// Models: The list of models.
	Models []*GoogleCloudMlV1beta1__Model `json:"models,omitempty"`

	// NextPageToken: Optional. Pass this token as the `page_token` field of
	// the request for a
	// subsequent call.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Models") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Models") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__ListModelsResponse) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__ListModelsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__ListVersionsResponse: Response message for the
// ListVersions method.
type GoogleCloudMlV1beta1__ListVersionsResponse struct {
	// NextPageToken: Optional. Pass this token as the `page_token` field of
	// the request for a
	// subsequent call.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Versions: The list of versions.
	Versions []*GoogleCloudMlV1beta1__Version `json:"versions,omitempty"`

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

func (s *GoogleCloudMlV1beta1__ListVersionsResponse) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__ListVersionsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__Model: Represents a machine learning
// solution.
//
// A model can have multiple versions, each of which is a deployed,
// trained
// model ready to receive prediction requests. The model itself is just
// a
// container.
type GoogleCloudMlV1beta1__Model struct {
	// DefaultVersion: Output only. The default version of the model. This
	// version will be used to
	// handle prediction requests that do not specify a version.
	//
	// You can change the default version by
	// calling
	// [projects.methods.versions.setDefault](/ml/reference/rest/v1be
	// ta1/projects.models.versions/setDefault).
	DefaultVersion *GoogleCloudMlV1beta1__Version `json:"defaultVersion,omitempty"`

	// Description: Optional. The description specified for the model when
	// it was created.
	Description string `json:"description,omitempty"`

	// Name: Required. The name specified for the model when it was
	// created.
	//
	// The model name must be unique within the project it is created in.
	Name string `json:"name,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DefaultVersion") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DefaultVersion") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__Model) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__Model
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__OperationMetadata: Represents the metadata of
// the long-running operation.
type GoogleCloudMlV1beta1__OperationMetadata struct {
	// CreateTime: The time the operation was submitted.
	CreateTime string `json:"createTime,omitempty"`

	// EndTime: The time operation processing completed.
	EndTime string `json:"endTime,omitempty"`

	// IsCancellationRequested: Indicates whether a request to cancel this
	// operation has been made.
	IsCancellationRequested bool `json:"isCancellationRequested,omitempty"`

	// ModelName: Contains the name of the model associated with the
	// operation.
	ModelName string `json:"modelName,omitempty"`

	// OperationType: The operation type.
	//
	// Possible values:
	//   "OPERATION_TYPE_UNSPECIFIED" - Unspecified operation type.
	//   "CREATE_VERSION" - An operation to create a new version.
	//   "DELETE_VERSION" - An operation to delete an existing version.
	//   "DELETE_MODEL" - An operation to delete an existing model.
	OperationType string `json:"operationType,omitempty"`

	// StartTime: The time operation processing started.
	StartTime string `json:"startTime,omitempty"`

	// Version: Contains the version associated with the operation.
	Version *GoogleCloudMlV1beta1__Version `json:"version,omitempty"`

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

func (s *GoogleCloudMlV1beta1__OperationMetadata) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__OperationMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__ParameterSpec: Represents a single
// hyperparameter to optimize.
type GoogleCloudMlV1beta1__ParameterSpec struct {
	// CategoricalValues: Required if type is `CATEGORICAL`. The list of
	// possible categories.
	CategoricalValues []string `json:"categoricalValues,omitempty"`

	// DiscreteValues: Required if type is `DISCRETE`.
	// A list of feasible points.
	// The list should be in strictly increasing order. For instance,
	// this
	// parameter might have possible settings of 1.5, 2.5, and 4.0. This
	// list
	// should not contain more than 1,000 values.
	DiscreteValues []float64 `json:"discreteValues,omitempty"`

	// MaxValue: Required if typeis `DOUBLE` or `INTEGER`. This field
	// should be unset if type is `CATEGORICAL`. This value should be
	// integers if
	// type is `INTEGER`.
	MaxValue float64 `json:"maxValue,omitempty"`

	// MinValue: Required if type is `DOUBLE` or `INTEGER`. This
	// field
	// should be unset if type is `CATEGORICAL`. This value should be
	// integers if
	// type is INTEGER.
	MinValue float64 `json:"minValue,omitempty"`

	// ParameterName: Required. The parameter name must be unique amongst
	// all ParameterConfigs in
	// a HyperparameterSpec message. E.g., "learning_rate".
	ParameterName string `json:"parameterName,omitempty"`

	// ScaleType: Optional. How the parameter should be scaled to the
	// hypercube.
	// Leave unset for categorical parameters.
	// Some kind of scaling is strongly recommended for real or
	// integral
	// parameters (e.g., `UNIT_LINEAR_SCALE`).
	//
	// Possible values:
	//   "NONE" - By default, no scaling is applied.
	//   "UNIT_LINEAR_SCALE" - Scales the feasible space to (0, 1) linearly.
	//   "UNIT_LOG_SCALE" - Scales the feasible space logarithmically to (0,
	// 1). The entire feasible
	// space must be strictly positive.
	//   "UNIT_REVERSE_LOG_SCALE" - Scales the feasible space "reverse"
	// logarithmically to (0, 1). The result
	// is that values close to the top of the feasible space are spread out
	// more
	// than points near the bottom. The entire feasible space must be
	// strictly
	// positive.
	ScaleType string `json:"scaleType,omitempty"`

	// Type: Required. The type of the parameter.
	//
	// Possible values:
	//   "PARAMETER_TYPE_UNSPECIFIED" - You must specify a valid type. Using
	// this unspecified type will result in
	// an error.
	//   "DOUBLE" - Type for real-valued parameters.
	//   "INTEGER" - Type for integral parameters.
	//   "CATEGORICAL" - The parameter is categorical, with a value chosen
	// from the categories
	// field.
	//   "DISCRETE" - The parameter is real valued, with a fixed set of
	// feasible points. If
	// `type==DISCRETE`, feasible_points must be provided, and
	// {`min_value`, `max_value`} will be ignored.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CategoricalValues")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CategoricalValues") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__ParameterSpec) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__ParameterSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__PredictRequest: Request for predictions to be
// issued against a trained model.
//
// The body of the request is a single JSON object with a single
// top-level
// field:
//
// <dl>
//   <dt>instances</dt>
//   <dd>A JSON array containing values representing the instances to
// use for
//       prediction.</dd>
// </dl>
//
// The structure of each element of the instances list is determined by
// your
// model's input definition. Instances can include named inputs or can
// contain
// only unlabeled values.
//
// Most data does not include named inputs. Some instances will be
// simple
// JSON values (boolean, number, or string). However, instances are
// often lists
// of simple values, or complex nested lists. Here are some examples of
// request
// bodies:
//
// CSV data with each row encoded as a string value:
// <pre>
// {"instances": ["1.0,true,\\"x\\"",
// "-2.0,false,\\"y\\""]}
// </pre>
// Plain text:
// <pre>
// {"instances": ["the quick brown fox", "la bruja le
// dio"]}
// </pre>
// Sentences encoded as lists of words (vectors of
// strings):
// <pre>
// {"instances": [["the","quick","brown"],
// ["la","bruja","le"]]}
// </pre>
// Floating point scalar values:
// <pre>
// {"instances": [0.0, 1.1, 2.2]}
// </pre>
// Vectors of integers:
// <pre>
// {"instances": [[0, 1, 2], [3, 4, 5],...]}
// </pre>
// Tensors (in this case, two-dimensional tensors):
// <pre>
// {"instances": [[[0, 1, 2], [3, 4, 5]], ...]}
// </pre>
// Images represented as a three-dimensional list. In this encoding
// scheme the
// first two dimensions represent the rows and columns of the image, and
// the
// third contains the R, G, and B values for each
// pixel.
// <pre>
// {"instances": [[[[138, 30, 66], [130, 20, 56], ...]]]]}
// </pre>
// Data must be encoded as UTF-8. If your data uses another character
// encoding,
// you must base64 encode the data and mark it as binary. To mark a JSON
// string
// as binary, replace it with an object with a single attribute named
// `b`:
// <pre>{"b": "..."} </pre>
// For example:
//
// Two Serialized tf.Examples (fake data, for illustrative purposes
// only):
// <pre>
// {"instances": [{"b64": "X5ad6u"}, {"b64": "IA9j4nx"}]}
// </pre>
// Two JPEG image byte strings (fake data, for illustrative purposes
// only):
// <pre>
// {"instances": [{"b64": "ASa8asdf"}, {"b64": "JLK7ljk3"}]}
// </pre>
// If your data includes named references, format each instance as a
// JSON object
// with the named references as the keys:
//
// JSON input data to be preprocessed:
// <pre>
// {"instances": [{"a": 1.0,  "b": true,  "c": "x"},
//                {"a": -2.0, "b": false, "c": "y"}]}
// </pre>
// Some models have an underlying TensorFlow graph that accepts multiple
// input
// tensors. In this case, you should use the names of JSON name/value
// pairs to
// identify the input tensors, as shown in the following exmaples:
//
// For a graph with input tensor aliases "tag" (string) and
// "image"
// (base64-encoded string):
// <pre>
// {"instances": [{"tag": "beach", "image": {"b64": "ASa8asdf"}},
//                {"tag": "car", "image": {"b64":
// "JLK7ljk3"}}]}
// </pre>
// For a graph with input tensor aliases "tag" (string) and
// "image"
// (3-dimensional array of 8-bit ints):
// <pre>
// {"instances": [{"tag": "beach", "image": [[[263, 1, 10], [262, 2,
// 11], ...]]},
//                {"tag": "car", "image": [[[10, 11, 24], [23, 10, 15],
// ...]]}]}
// </pre>
// If the call is successful, the response body will contain one
// prediction
// entry per instance in the request body. If prediction fails for
// any
// instance, the response body will contain no predictions and will
// contian
// a single error entry instead.
type GoogleCloudMlV1beta1__PredictRequest struct {
	// HttpBody:
	// Required. The prediction request body.
	HttpBody *GoogleApi__HttpBody `json:"httpBody,omitempty"`

	// ForceSendFields is a list of field names (e.g. "HttpBody") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HttpBody") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__PredictRequest) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__PredictRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__PredictionInput: Represents input parameters
// for a prediction job.
type GoogleCloudMlV1beta1__PredictionInput struct {
	// DataFormat: Required. The format of the input data files.
	//
	// Possible values:
	//   "DATA_FORMAT_UNSPECIFIED" - Unspecified format.
	//   "TEXT" - The source file is a text file with instances separated by
	// the
	// new-line character.
	//   "TF_RECORD" - The source file is a TFRecord file.
	//   "TF_RECORD_GZIP" - The source file is a GZIP-compressed TFRecord
	// file.
	DataFormat string `json:"dataFormat,omitempty"`

	// InputPaths: Required. The Google Cloud Storage location of the input
	// data files.
	// May contain wildcards.
	InputPaths []string `json:"inputPaths,omitempty"`

	// MaxWorkerCount: Optional. The maximum number of workers to be used
	// for parallel processing.
	// Defaults to 10 if not specified.
	MaxWorkerCount int64 `json:"maxWorkerCount,omitempty,string"`

	// ModelName: Use this field if you want to use the default version for
	// the specified
	// model. The string must use the following
	// format:
	//
	// "projects/<var>[YOUR_PROJECT]</var>/models/<var>[YOUR_MODEL]
	// </var>"
	ModelName string `json:"modelName,omitempty"`

	// OutputPath: Required. The output Google Cloud Storage location.
	OutputPath string `json:"outputPath,omitempty"`

	// Region: Required. The Google Compute Engine region to run the
	// prediction job in.
	Region string `json:"region,omitempty"`

	// VersionName: Use this field if you want to specify a version of the
	// model to use. The
	// string is formatted the same way as `model_version`, with the
	// addition
	// of the version
	// information:
	//
	// "projects/<var>[YOUR_PROJECT]</var>/models/<var>YOUR_MO
	// DEL/versions/<var>[YOUR_VERSION]</var>"
	VersionName string `json:"versionName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DataFormat") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataFormat") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__PredictionInput) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__PredictionInput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__PredictionOutput: Represents results of a
// prediction job.
type GoogleCloudMlV1beta1__PredictionOutput struct {
	// ErrorCount: The number of data instances which resulted in errors.
	ErrorCount int64 `json:"errorCount,omitempty,string"`

	// OutputPath: The output Google Cloud Storage location provided at the
	// job creation time.
	OutputPath string `json:"outputPath,omitempty"`

	// PredictionCount: The number of generated predictions.
	PredictionCount int64 `json:"predictionCount,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "ErrorCount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ErrorCount") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__PredictionOutput) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__PredictionOutput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__SetDefaultVersionRequest: Request message for
// the SetDefaultVersion request.
type GoogleCloudMlV1beta1__SetDefaultVersionRequest struct {
}

// GoogleCloudMlV1beta1__TrainingInput: Represents input parameters for
// a training job.
type GoogleCloudMlV1beta1__TrainingInput struct {
	// Args: Optional. Command line arguments to pass to the program.
	Args []string `json:"args,omitempty"`

	// Hyperparameters: Optional. The set of Hyperparameters to tune.
	Hyperparameters *GoogleCloudMlV1beta1__HyperparameterSpec `json:"hyperparameters,omitempty"`

	// MasterType: Optional. Specifies the type of virtual machine to use
	// for your training
	// job's master worker.
	//
	// The following types are supported:
	//
	// <dl>
	//   <dt>standard</dt>
	//   <dd>
	//   A basic machine configuration suitable for training simple models
	// with
	//   small to moderate datasets.
	//   </dd>
	//   <dt>large_model</dt>
	//   <dd>
	//   A machine with a lot of memory, specially suited for parameter
	// servers
	//   when your model is large (having many hidden layers or layers with
	// very
	//   large numbers of nodes).
	//   </dd>
	//   <dt>complex_model_s</dt>
	//   <dd>
	//   A machine suitable for the master and workers of the cluster when
	// your
	//   model requires more computation than the standard machine can
	// handle
	//   satisfactorily.
	//   </dd>
	//   <dt>complex_model_m</dt>
	//   <dd>
	//   A machine with roughly twice the number of cores and roughly double
	// the
	//   memory of <code suppresswarning="true">complex_model_s</code>.
	//   </dd>
	//   <dt>complex_model_l</dt>
	//   <dd>
	//   A machine with roughly twice the number of cores and roughly double
	// the
	//   memory of <code suppresswarning="true">complex_model_m</code>.
	//   </dd>
	// </dl>
	//
	// You must set this value when `scaleTier` is set to `CUSTOM`.
	MasterType string `json:"masterType,omitempty"`

	// PackageUris: Required. The Google Cloud Storage location of the
	// packages with
	// the training program and any additional dependencies.
	PackageUris []string `json:"packageUris,omitempty"`

	// ParameterServerCount: Optional. The number of parameter server
	// replicas to use for the training
	// job. Each replica in the cluster will be of the type specified
	// in
	// `parameter_server_type`.
	//
	// This value can only be used when `scale_tier` is set to `CUSTOM`.If
	// you
	// set this value, you must also set `parameter_server_type`.
	ParameterServerCount int64 `json:"parameterServerCount,omitempty,string"`

	// ParameterServerType: Optional. Specifies the type of virtual machine
	// to use for your training
	// job's parameter server.
	//
	// The supported values are the same as those described in the entry
	// for
	// `master_type`.
	//
	// This value must be present when `scaleTier` is set to `CUSTOM`
	// and
	// `parameter_server_count` is greater than zero.
	ParameterServerType string `json:"parameterServerType,omitempty"`

	// PythonModule: Required. The Python module name to run after
	// installing the packages.
	PythonModule string `json:"pythonModule,omitempty"`

	// Region: Required. The Google Compute Engine region to run the
	// training job in.
	Region string `json:"region,omitempty"`

	// ScaleTier: Required. Specifies the machine types, the number of
	// replicas for workers
	// and parameter servers.
	//
	// Possible values:
	//   "BASIC" - A single worker instance. This tier is suitable for
	// learning how to use
	// Cloud ML, and for experimenting with new models using small datasets.
	//   "STANDARD_1" - Many workers and a few parameter servers.
	//   "PREMIUM_1" - A large number of workers with many parameter
	// servers.
	//   "CUSTOM" - The CUSTOM tier is not a set tier, but rather enables
	// you to use your
	// own cluster specification. When you use this tier, set values
	// to
	// configure your processing cluster according to these guidelines:
	//
	// *   You _must_ set `TrainingInput.masterType` to specify the type
	//     of machine to use for your master node. This is the only
	// required
	//     setting.
	//
	// *   You _may_ set `TrainingInput.workerCount` to specify the number
	// of
	//     workers to use. If you specify one or more workers, you _must_
	// also
	//     set `TrainingInput.workerType` to specify the type of machine to
	// use
	//     for your worker nodes.
	//
	// *   You _may_ set `TrainingInput.parameterServerCount` to specify
	// the
	//     number of parameter servers to use. If you specify one or more
	//     parameter servers, you _must_ also set
	//     `TrainingInput.parameterServerType` to specify the type of
	// machine to
	//     use for your parameter servers.
	//
	// Note that all of your workers must use the same machine type, which
	// can
	// be different from your parameter server type and master type.
	// Your
	// parameter servers must likewise use the same machine type, which can
	// be
	// different from your worker type and master type.
	ScaleTier string `json:"scaleTier,omitempty"`

	// WorkerCount: Optional. The number of worker replicas to use for the
	// training job. Each
	// replica in the cluster will be of the type specified in
	// `worker_type`.
	//
	// This value can only be used when `scale_tier` is set to `CUSTOM`. If
	// you
	// set this value, you must also set `worker_type`.
	WorkerCount int64 `json:"workerCount,omitempty,string"`

	// WorkerType: Optional. Specifies the type of virtual machine to use
	// for your training
	// job's worker nodes.
	//
	// The supported values are the same as those described in the entry
	// for
	// `masterType`.
	//
	// This value must be present when `scaleTier` is set to `CUSTOM`
	// and
	// `workerCount` is greater than zero.
	WorkerType string `json:"workerType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Args") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Args") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__TrainingInput) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__TrainingInput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__TrainingOutput: Represents results of a
// training job.
type GoogleCloudMlV1beta1__TrainingOutput struct {
	// CompletedTrialCount: The number of hyperparameter tuning trials that
	// completed successfully.
	CompletedTrialCount int64 `json:"completedTrialCount,omitempty,string"`

	// ConsumedMlUnits: The amount of ML units consumed by the job.
	ConsumedMlUnits float64 `json:"consumedMlUnits,omitempty"`

	// Trials: Results for individual Hyperparameter trials.
	Trials []*GoogleCloudMlV1beta1__HyperparameterOutput `json:"trials,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CompletedTrialCount")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CompletedTrialCount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *GoogleCloudMlV1beta1__TrainingOutput) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__TrainingOutput
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleCloudMlV1beta1__Version: Represents a version of the
// model.
//
// Each version is a trained model deployed in the cloud, ready to
// handle
// prediction requests. A model can have multiple versions. You can
// get
// information about all of the versions of a given model by
// calling
// [projects.models.versions.list](/ml/reference/rest/v1beta1/pro
// jects.models.versions/list).
type GoogleCloudMlV1beta1__Version struct {
	// CreateTime: Output only. The time the version was created.
	CreateTime string `json:"createTime,omitempty"`

	// DeploymentUri: Required. The Google Cloud Storage location of the
	// trained model used to
	// create the version. See the
	// [overview of model deployment](/ml/docs/concepts/deployment-overview)
	// for
	// more informaiton.
	//
	// When passing Version
	// to
	// [projects.models.versions.create](/ml/reference/rest/v1beta1/projec
	// ts.models.versions/create)
	// the model service uses the specified location as the source of the
	// model.
	// Once deployed, the model version is hosted by the prediction service,
	// so
	// this location is useful only as a historical record.
	DeploymentUri string `json:"deploymentUri,omitempty"`

	// Description: Optional. The description specified for the version when
	// it was created.
	Description string `json:"description,omitempty"`

	// IsDefault: Output only. If true, this version will be used to handle
	// prediction
	// requests that do not specify a version.
	//
	// You can change the default version by
	// calling
	// [projects.methods.versions.setDefault](/ml/reference/rest/v1be
	// ta1/projects.models.versions/setDefault).
	IsDefault bool `json:"isDefault,omitempty"`

	// LastUseTime: Output only. The time the version was last used for
	// prediction.
	LastUseTime string `json:"lastUseTime,omitempty"`

	// Name: Required.The name specified for the version when it was
	// created.
	//
	// The version name must be unique within the model it is created in.
	Name string `json:"name,omitempty"`

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

func (s *GoogleCloudMlV1beta1__Version) MarshalJSON() ([]byte, error) {
	type noMethod GoogleCloudMlV1beta1__Version
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleLongrunning__ListOperationsResponse: The response message for
// Operations.ListOperations.
type GoogleLongrunning__ListOperationsResponse struct {
	// NextPageToken: The standard List next-page token.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Operations: A list of operations that matches the specified filter in
	// the request.
	Operations []*GoogleLongrunning__Operation `json:"operations,omitempty"`

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

func (s *GoogleLongrunning__ListOperationsResponse) MarshalJSON() ([]byte, error) {
	type noMethod GoogleLongrunning__ListOperationsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleLongrunning__Operation: This resource represents a long-running
// operation that is the result of a
// network API call.
type GoogleLongrunning__Operation struct {
	// Done: If the value is `false`, it means the operation is still in
	// progress.
	// If true, the operation is completed, and either `error` or `response`
	// is
	// available.
	Done bool `json:"done,omitempty"`

	// Error: The error result of the operation in case of failure or
	// cancellation.
	Error *GoogleRpc__Status `json:"error,omitempty"`

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

func (s *GoogleLongrunning__Operation) MarshalJSON() ([]byte, error) {
	type noMethod GoogleLongrunning__Operation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GoogleProtobuf__Empty: A generic empty message that you can re-use to
// avoid defining duplicated
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
type GoogleProtobuf__Empty struct {
	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`
}

// GoogleRpc__Status: The `Status` type defines a logical error model
// that is suitable for different
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
type GoogleRpc__Status struct {
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

func (s *GoogleRpc__Status) MarshalJSON() ([]byte, error) {
	type noMethod GoogleRpc__Status
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "ml.projects.getConfig":

type ProjectsGetConfigCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetConfig: Get the service account information associated with your
// project. You need
// this information in order to grant the service account persmissions
// for
// the Google Cloud Storage location where you put your model training
// code
// for training the model with Google Cloud Machine Learning.
func (r *ProjectsService) GetConfig(name string) *ProjectsGetConfigCall {
	c := &ProjectsGetConfigCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsGetConfigCall) Fields(s ...googleapi.Field) *ProjectsGetConfigCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsGetConfigCall) IfNoneMatch(entityTag string) *ProjectsGetConfigCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsGetConfigCall) Context(ctx context.Context) *ProjectsGetConfigCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsGetConfigCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsGetConfigCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}:getConfig")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.getConfig" call.
// Exactly one of *GoogleCloudMlV1beta1__GetConfigResponse or error will
// be non-nil. Any non-2xx status code is an error. Response headers are
// in either
// *GoogleCloudMlV1beta1__GetConfigResponse.ServerResponse.Header or (if
// a response was returned at all) in error.(*googleapi.Error).Header.
// Use googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsGetConfigCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__GetConfigResponse, error) {
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
	ret := &GoogleCloudMlV1beta1__GetConfigResponse{
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
	//   "description": "Get the service account information associated with your project. You need\nthis information in order to grant the service account persmissions for\nthe Google Cloud Storage location where you put your model training code\nfor training the model with Google Cloud Machine Learning.",
	//   "flatPath": "v1beta1/projects/{projectsId}:getConfig",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.getConfig",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The project name.\n\nAuthorization: requires `Viewer` role on the specified project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}:getConfig",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__GetConfigResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.predict":

type ProjectsPredictCall struct {
	s                                    *Service
	name                                 string
	googlecloudmlv1beta1__predictrequest *GoogleCloudMlV1beta1__PredictRequest
	urlParams_                           gensupport.URLParams
	ctx_                                 context.Context
	header_                              http.Header
}

// Predict: Performs prediction on the data in the request.
//
// Responses are very similar to requests. There are two top-level
// fields,
// each of which are JSON lists:
//
// <dl>
//   <dt>predictions</dt>
//   <dd>The list of predictions, one per instance in the request.</dd>
//   <dt>error</dt>
//   <dd>An error message returned instead of a prediction list if any
//       instance produced an error.</dd>
// </dl>
//
// If the call is successful, the response body will contain one
// prediction
// entry per instance in the request body. If prediction fails for
// any
// instance, the response body will contain no predictions and will
// contian
// a single error entry instead.
//
// Even though there is one prediction per instance, the format of
// a
// prediction is not directly related to the format of an
// instance.
// Predictions take whatever format is specified in the outputs
// collection
// defined in the model. The collection of predictions is returned in a
// JSON
// list. Each member of the list can be a simple value, a list, or a
// JSON
// object of any complexity. If your model has more than one output
// tensor,
// each prediction will be a JSON object containing a name/value pair
// for each
// output. The names identify the output aliases in the graph.
//
// The following examples show some possible responses:
//
// A simple set of predictions for three input instances, where
// each
// prediction is an integer value:
// <pre>
// {"predictions": [5, 4, 3]}
// </pre>
// A more complex set of predictions, each containing two named values
// that
// correspond to output tensors, named **label** and **scores**
// respectively.
// The value of **label** is the predicted category ("car" or "beach")
// and
// **scores** contains a list of probabilities for that instance across
// the
// possible categories.
// <pre>
// {"predictions": [{"label": "beach", "scores": [0.1, 0.9]},
//                  {"label": "car", "scores": [0.75, 0.25]}]}
// </pre>
// A response when there is an error processing an input
// instance:
// <pre>
// {"error": "Divide by zero"}
// </pre>
func (r *ProjectsService) Predict(name string, googlecloudmlv1beta1__predictrequest *GoogleCloudMlV1beta1__PredictRequest) *ProjectsPredictCall {
	c := &ProjectsPredictCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.googlecloudmlv1beta1__predictrequest = googlecloudmlv1beta1__predictrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsPredictCall) Fields(s ...googleapi.Field) *ProjectsPredictCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsPredictCall) Context(ctx context.Context) *ProjectsPredictCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsPredictCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsPredictCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.googlecloudmlv1beta1__predictrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}:predict")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.predict" call.
// Exactly one of *GoogleApi__HttpBody or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GoogleApi__HttpBody.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsPredictCall) Do(opts ...googleapi.CallOption) (*GoogleApi__HttpBody, error) {
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
	ret := &GoogleApi__HttpBody{
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
	//   "description": "Performs prediction on the data in the request.\n\nResponses are very similar to requests. There are two top-level fields,\neach of which are JSON lists:\n\n\u003cdl\u003e\n  \u003cdt\u003epredictions\u003c/dt\u003e\n  \u003cdd\u003eThe list of predictions, one per instance in the request.\u003c/dd\u003e\n  \u003cdt\u003eerror\u003c/dt\u003e\n  \u003cdd\u003eAn error message returned instead of a prediction list if any\n      instance produced an error.\u003c/dd\u003e\n\u003c/dl\u003e\n\nIf the call is successful, the response body will contain one prediction\nentry per instance in the request body. If prediction fails for any\ninstance, the response body will contain no predictions and will contian\na single error entry instead.\n\nEven though there is one prediction per instance, the format of a\nprediction is not directly related to the format of an instance.\nPredictions take whatever format is specified in the outputs collection\ndefined in the model. The collection of predictions is returned in a JSON\nlist. Each member of the list can be a simple value, a list, or a JSON\nobject of any complexity. If your model has more than one output tensor,\neach prediction will be a JSON object containing a name/value pair for each\noutput. The names identify the output aliases in the graph.\n\nThe following examples show some possible responses:\n\nA simple set of predictions for three input instances, where each\nprediction is an integer value:\n\u003cpre\u003e\n{\"predictions\": [5, 4, 3]}\n\u003c/pre\u003e\nA more complex set of predictions, each containing two named values that\ncorrespond to output tensors, named **label** and **scores** respectively.\nThe value of **label** is the predicted category (\"car\" or \"beach\") and\n**scores** contains a list of probabilities for that instance across the\npossible categories.\n\u003cpre\u003e\n{\"predictions\": [{\"label\": \"beach\", \"scores\": [0.1, 0.9]},\n                 {\"label\": \"car\", \"scores\": [0.75, 0.25]}]}\n\u003c/pre\u003e\nA response when there is an error processing an input instance:\n\u003cpre\u003e\n{\"error\": \"Divide by zero\"}\n\u003c/pre\u003e",
	//   "flatPath": "v1beta1/projects/{projectsId}:predict",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.predict",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The resource name of a model or a version.\n\nAuthorization: requires `Viewer` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/.+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}:predict",
	//   "request": {
	//     "$ref": "GoogleCloudMlV1beta1__PredictRequest"
	//   },
	//   "response": {
	//     "$ref": "GoogleApi__HttpBody"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.jobs.cancel":

type ProjectsJobsCancelCall struct {
	s                                      *Service
	name                                   string
	googlecloudmlv1beta1__canceljobrequest *GoogleCloudMlV1beta1__CancelJobRequest
	urlParams_                             gensupport.URLParams
	ctx_                                   context.Context
	header_                                http.Header
}

// Cancel: Cancels a running job.
func (r *ProjectsJobsService) Cancel(name string, googlecloudmlv1beta1__canceljobrequest *GoogleCloudMlV1beta1__CancelJobRequest) *ProjectsJobsCancelCall {
	c := &ProjectsJobsCancelCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.googlecloudmlv1beta1__canceljobrequest = googlecloudmlv1beta1__canceljobrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsJobsCancelCall) Fields(s ...googleapi.Field) *ProjectsJobsCancelCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsJobsCancelCall) Context(ctx context.Context) *ProjectsJobsCancelCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsJobsCancelCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsJobsCancelCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.googlecloudmlv1beta1__canceljobrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}:cancel")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.jobs.cancel" call.
// Exactly one of *GoogleProtobuf__Empty or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GoogleProtobuf__Empty.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsCancelCall) Do(opts ...googleapi.CallOption) (*GoogleProtobuf__Empty, error) {
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
	ret := &GoogleProtobuf__Empty{
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
	//   "description": "Cancels a running job.",
	//   "flatPath": "v1beta1/projects/{projectsId}/jobs/{jobsId}:cancel",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.jobs.cancel",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the job to cancel.\n\nAuthorization: requires `Editor` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/jobs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}:cancel",
	//   "request": {
	//     "$ref": "GoogleCloudMlV1beta1__CancelJobRequest"
	//   },
	//   "response": {
	//     "$ref": "GoogleProtobuf__Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.jobs.create":

type ProjectsJobsCreateCall struct {
	s                         *Service
	parent                    string
	googlecloudmlv1beta1__job *GoogleCloudMlV1beta1__Job
	urlParams_                gensupport.URLParams
	ctx_                      context.Context
	header_                   http.Header
}

// Create: Creates a training or a batch prediction job.
func (r *ProjectsJobsService) Create(parent string, googlecloudmlv1beta1__job *GoogleCloudMlV1beta1__Job) *ProjectsJobsCreateCall {
	c := &ProjectsJobsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.googlecloudmlv1beta1__job = googlecloudmlv1beta1__job
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
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.googlecloudmlv1beta1__job)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/jobs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.jobs.create" call.
// Exactly one of *GoogleCloudMlV1beta1__Job or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *GoogleCloudMlV1beta1__Job.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsCreateCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__Job, error) {
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
	ret := &GoogleCloudMlV1beta1__Job{
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
	//   "description": "Creates a training or a batch prediction job.",
	//   "flatPath": "v1beta1/projects/{projectsId}/jobs",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.jobs.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "Required. The project name.\n\nAuthorization: requires `Editor` role on the specified project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/jobs",
	//   "request": {
	//     "$ref": "GoogleCloudMlV1beta1__Job"
	//   },
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.jobs.get":

type ProjectsJobsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Describes a job.
func (r *ProjectsJobsService) Get(name string) *ProjectsJobsGetCall {
	c := &ProjectsJobsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.jobs.get" call.
// Exactly one of *GoogleCloudMlV1beta1__Job or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *GoogleCloudMlV1beta1__Job.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsGetCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__Job, error) {
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
	ret := &GoogleCloudMlV1beta1__Job{
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
	//   "description": "Describes a job.",
	//   "flatPath": "v1beta1/projects/{projectsId}/jobs/{jobsId}",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.jobs.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the job to get the description of.\n\nAuthorization: requires `Viewer` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/jobs/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__Job"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.jobs.list":

type ProjectsJobsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the jobs in the project.
func (r *ProjectsJobsService) List(parent string) *ProjectsJobsListCall {
	c := &ProjectsJobsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// Filter sets the optional parameter "filter": Specifies the subset of
// jobs to retrieve.
func (c *ProjectsJobsListCall) Filter(filter string) *ProjectsJobsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": The number of jobs
// to retrieve per "page" of results. If there
// are more remaining results than this number, the response message
// will
// contain a valid value in the `next_page_token` field.
//
// The default value is 20, and the maximum page size is 100.
func (c *ProjectsJobsListCall) PageSize(pageSize int64) *ProjectsJobsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": A page token to
// request the next page of results.
//
// You get the token from the `next_page_token` field of the response
// from
// the previous call.
func (c *ProjectsJobsListCall) PageToken(pageToken string) *ProjectsJobsListCall {
	c.urlParams_.Set("pageToken", pageToken)
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/jobs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.jobs.list" call.
// Exactly one of *GoogleCloudMlV1beta1__ListJobsResponse or error will
// be non-nil. Any non-2xx status code is an error. Response headers are
// in either
// *GoogleCloudMlV1beta1__ListJobsResponse.ServerResponse.Header or (if
// a response was returned at all) in error.(*googleapi.Error).Header.
// Use googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsJobsListCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__ListJobsResponse, error) {
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
	ret := &GoogleCloudMlV1beta1__ListJobsResponse{
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
	//   "description": "Lists the jobs in the project.",
	//   "flatPath": "v1beta1/projects/{projectsId}/jobs",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.jobs.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "Optional. Specifies the subset of jobs to retrieve.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Optional. The number of jobs to retrieve per \"page\" of results. If there\nare more remaining results than this number, the response message will\ncontain a valid value in the `next_page_token` field.\n\nThe default value is 20, and the maximum page size is 100.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A page token to request the next page of results.\n\nYou get the token from the `next_page_token` field of the response from\nthe previous call.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Required. The name of the project for which to list jobs.\n\nAuthorization: requires `Viewer` role on the specified project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/jobs",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__ListJobsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsJobsListCall) Pages(ctx context.Context, f func(*GoogleCloudMlV1beta1__ListJobsResponse) error) error {
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

// method id "ml.projects.models.create":

type ProjectsModelsCreateCall struct {
	s                           *Service
	parent                      string
	googlecloudmlv1beta1__model *GoogleCloudMlV1beta1__Model
	urlParams_                  gensupport.URLParams
	ctx_                        context.Context
	header_                     http.Header
}

// Create: Creates a model which will later contain one or more
// versions.
//
// You must add at least one version before you can request predictions
// from
// the model. Add versions by
// calling
// [projects.models.versions.create](/ml/reference/rest/v1beta1/p
// rojects.models.versions/create).
func (r *ProjectsModelsService) Create(parent string, googlecloudmlv1beta1__model *GoogleCloudMlV1beta1__Model) *ProjectsModelsCreateCall {
	c := &ProjectsModelsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.googlecloudmlv1beta1__model = googlecloudmlv1beta1__model
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsCreateCall) Fields(s ...googleapi.Field) *ProjectsModelsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsCreateCall) Context(ctx context.Context) *ProjectsModelsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.googlecloudmlv1beta1__model)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/models")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.models.create" call.
// Exactly one of *GoogleCloudMlV1beta1__Model or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *GoogleCloudMlV1beta1__Model.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsCreateCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__Model, error) {
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
	ret := &GoogleCloudMlV1beta1__Model{
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
	//   "description": "Creates a model which will later contain one or more versions.\n\nYou must add at least one version before you can request predictions from\nthe model. Add versions by calling\n[projects.models.versions.create](/ml/reference/rest/v1beta1/projects.models.versions/create).",
	//   "flatPath": "v1beta1/projects/{projectsId}/models",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.models.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "Required. The project name.\n\nAuthorization: requires `Editor` role on the specified project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/models",
	//   "request": {
	//     "$ref": "GoogleCloudMlV1beta1__Model"
	//   },
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__Model"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.models.delete":

type ProjectsModelsDeleteCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a model.
//
// You can only delete a model if there are no versions in it. You can
// delete
// versions by
// calling
// [projects.models.versions.delete](/ml/reference/rest/v1beta1/p
// rojects.models.versions/delete).
func (r *ProjectsModelsService) Delete(name string) *ProjectsModelsDeleteCall {
	c := &ProjectsModelsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsDeleteCall) Fields(s ...googleapi.Field) *ProjectsModelsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsDeleteCall) Context(ctx context.Context) *ProjectsModelsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsDeleteCall) doRequest(alt string) (*http.Response, error) {
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

// Do executes the "ml.projects.models.delete" call.
// Exactly one of *GoogleLongrunning__Operation or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GoogleLongrunning__Operation.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsDeleteCall) Do(opts ...googleapi.CallOption) (*GoogleLongrunning__Operation, error) {
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
	ret := &GoogleLongrunning__Operation{
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
	//   "description": "Deletes a model.\n\nYou can only delete a model if there are no versions in it. You can delete\nversions by calling\n[projects.models.versions.delete](/ml/reference/rest/v1beta1/projects.models.versions/delete).",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}",
	//   "httpMethod": "DELETE",
	//   "id": "ml.projects.models.delete",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the model.\n\nAuthorization: requires `Editor` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleLongrunning__Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.models.get":

type ProjectsModelsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a model, including its name, the
// description (if
// set), and the default version (if at least one version of the model
// has
// been deployed).
func (r *ProjectsModelsService) Get(name string) *ProjectsModelsGetCall {
	c := &ProjectsModelsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsGetCall) Fields(s ...googleapi.Field) *ProjectsModelsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsModelsGetCall) IfNoneMatch(entityTag string) *ProjectsModelsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsGetCall) Context(ctx context.Context) *ProjectsModelsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsGetCall) doRequest(alt string) (*http.Response, error) {
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

// Do executes the "ml.projects.models.get" call.
// Exactly one of *GoogleCloudMlV1beta1__Model or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *GoogleCloudMlV1beta1__Model.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsGetCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__Model, error) {
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
	ret := &GoogleCloudMlV1beta1__Model{
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
	//   "description": "Gets information about a model, including its name, the description (if\nset), and the default version (if at least one version of the model has\nbeen deployed).",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.models.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the model.\n\nAuthorization: requires `Viewer` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__Model"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.models.list":

type ProjectsModelsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the models in a project.
//
// Each project can contain multiple models, and each model can have
// multiple
// versions.
func (r *ProjectsModelsService) List(parent string) *ProjectsModelsListCall {
	c := &ProjectsModelsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// PageSize sets the optional parameter "pageSize": The number of models
// to retrieve per "page" of results. If there
// are more remaining results than this number, the response message
// will
// contain a valid value in the `next_page_token` field.
//
// The default value is 20, and the maximum page size is 100.
func (c *ProjectsModelsListCall) PageSize(pageSize int64) *ProjectsModelsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": A page token to
// request the next page of results.
//
// You get the token from the `next_page_token` field of the response
// from
// the previous call.
func (c *ProjectsModelsListCall) PageToken(pageToken string) *ProjectsModelsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsListCall) Fields(s ...googleapi.Field) *ProjectsModelsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsModelsListCall) IfNoneMatch(entityTag string) *ProjectsModelsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsListCall) Context(ctx context.Context) *ProjectsModelsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/models")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.models.list" call.
// Exactly one of *GoogleCloudMlV1beta1__ListModelsResponse or error
// will be non-nil. Any non-2xx status code is an error. Response
// headers are in either
// *GoogleCloudMlV1beta1__ListModelsResponse.ServerResponse.Header or
// (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsModelsListCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__ListModelsResponse, error) {
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
	ret := &GoogleCloudMlV1beta1__ListModelsResponse{
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
	//   "description": "Lists the models in a project.\n\nEach project can contain multiple models, and each model can have multiple\nversions.",
	//   "flatPath": "v1beta1/projects/{projectsId}/models",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.models.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "pageSize": {
	//       "description": "Optional. The number of models to retrieve per \"page\" of results. If there\nare more remaining results than this number, the response message will\ncontain a valid value in the `next_page_token` field.\n\nThe default value is 20, and the maximum page size is 100.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A page token to request the next page of results.\n\nYou get the token from the `next_page_token` field of the response from\nthe previous call.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Required. The name of the project whose models are to be listed.\n\nAuthorization: requires `Viewer` role on the specified project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/models",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__ListModelsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsModelsListCall) Pages(ctx context.Context, f func(*GoogleCloudMlV1beta1__ListModelsResponse) error) error {
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

// method id "ml.projects.models.versions.create":

type ProjectsModelsVersionsCreateCall struct {
	s                             *Service
	parent                        string
	googlecloudmlv1beta1__version *GoogleCloudMlV1beta1__Version
	urlParams_                    gensupport.URLParams
	ctx_                          context.Context
	header_                       http.Header
}

// Create: Creates a new version of a model from a trained TensorFlow
// model.
//
// If the version created in the cloud by this call is the first
// deployed
// version of the specified model, it will be made the default version
// of the
// model. When you add a version to a model that already has one or
// more
// versions, the default version does not automatically change. If you
// want a
// new version to be the default, you must
// call
// [projects.models.versions.setDefault](/ml/reference/rest/v1beta1/
// projects.models.versions/setDefault).
func (r *ProjectsModelsVersionsService) Create(parent string, googlecloudmlv1beta1__version *GoogleCloudMlV1beta1__Version) *ProjectsModelsVersionsCreateCall {
	c := &ProjectsModelsVersionsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.googlecloudmlv1beta1__version = googlecloudmlv1beta1__version
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsVersionsCreateCall) Fields(s ...googleapi.Field) *ProjectsModelsVersionsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsVersionsCreateCall) Context(ctx context.Context) *ProjectsModelsVersionsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsVersionsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsVersionsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.googlecloudmlv1beta1__version)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/versions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.models.versions.create" call.
// Exactly one of *GoogleLongrunning__Operation or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GoogleLongrunning__Operation.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsVersionsCreateCall) Do(opts ...googleapi.CallOption) (*GoogleLongrunning__Operation, error) {
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
	ret := &GoogleLongrunning__Operation{
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
	//   "description": "Creates a new version of a model from a trained TensorFlow model.\n\nIf the version created in the cloud by this call is the first deployed\nversion of the specified model, it will be made the default version of the\nmodel. When you add a version to a model that already has one or more\nversions, the default version does not automatically change. If you want a\nnew version to be the default, you must call\n[projects.models.versions.setDefault](/ml/reference/rest/v1beta1/projects.models.versions/setDefault).",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}/versions",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.models.versions.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "Required. The name of the model.\n\nAuthorization: requires `Editor` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/versions",
	//   "request": {
	//     "$ref": "GoogleCloudMlV1beta1__Version"
	//   },
	//   "response": {
	//     "$ref": "GoogleLongrunning__Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.models.versions.delete":

type ProjectsModelsVersionsDeleteCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a model version.
//
// Each model can have multiple versions deployed and in use at any
// given
// time. Use this method to remove a single version.
//
// Note: You cannot delete the version that is set as the default
// version
// of the model unless it is the only remaining version.
func (r *ProjectsModelsVersionsService) Delete(name string) *ProjectsModelsVersionsDeleteCall {
	c := &ProjectsModelsVersionsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsVersionsDeleteCall) Fields(s ...googleapi.Field) *ProjectsModelsVersionsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsVersionsDeleteCall) Context(ctx context.Context) *ProjectsModelsVersionsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsVersionsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsVersionsDeleteCall) doRequest(alt string) (*http.Response, error) {
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

// Do executes the "ml.projects.models.versions.delete" call.
// Exactly one of *GoogleLongrunning__Operation or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GoogleLongrunning__Operation.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsVersionsDeleteCall) Do(opts ...googleapi.CallOption) (*GoogleLongrunning__Operation, error) {
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
	ret := &GoogleLongrunning__Operation{
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
	//   "description": "Deletes a model version.\n\nEach model can have multiple versions deployed and in use at any given\ntime. Use this method to remove a single version.\n\nNote: You cannot delete the version that is set as the default version\nof the model unless it is the only remaining version.",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}/versions/{versionsId}",
	//   "httpMethod": "DELETE",
	//   "id": "ml.projects.models.versions.delete",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the version. You can get the names of all the\nversions of a model by calling\n[projects.models.versions.list](/ml/reference/rest/v1beta1/projects.models.versions/list).\n\nAuthorization: requires `Editor` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+/versions/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleLongrunning__Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.models.versions.get":

type ProjectsModelsVersionsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a model version.
//
// Models can have multiple versions. You can
// call
// [projects.models.versions.list](/ml/reference/rest/v1beta1/projec
// ts.models.versions/list)
// to get the same information that this method returns for all of
// the
// versions of a model.
func (r *ProjectsModelsVersionsService) Get(name string) *ProjectsModelsVersionsGetCall {
	c := &ProjectsModelsVersionsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsVersionsGetCall) Fields(s ...googleapi.Field) *ProjectsModelsVersionsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsModelsVersionsGetCall) IfNoneMatch(entityTag string) *ProjectsModelsVersionsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsVersionsGetCall) Context(ctx context.Context) *ProjectsModelsVersionsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsVersionsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsVersionsGetCall) doRequest(alt string) (*http.Response, error) {
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

// Do executes the "ml.projects.models.versions.get" call.
// Exactly one of *GoogleCloudMlV1beta1__Version or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GoogleCloudMlV1beta1__Version.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsVersionsGetCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__Version, error) {
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
	ret := &GoogleCloudMlV1beta1__Version{
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
	//   "description": "Gets information about a model version.\n\nModels can have multiple versions. You can call\n[projects.models.versions.list](/ml/reference/rest/v1beta1/projects.models.versions/list)\nto get the same information that this method returns for all of the\nversions of a model.",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}/versions/{versionsId}",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.models.versions.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the version.\n\nAuthorization: requires `Viewer` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+/versions/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__Version"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.models.versions.list":

type ProjectsModelsVersionsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Gets basic information about all the versions of a model.
//
// If you expect that a model has a lot of versions, or if you need to
// handle
// only a limited number of results at a time, you can request that the
// list
// be retrieved in batches (called pages):
func (r *ProjectsModelsVersionsService) List(parent string) *ProjectsModelsVersionsListCall {
	c := &ProjectsModelsVersionsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// PageSize sets the optional parameter "pageSize": The number of
// versions to retrieve per "page" of results. If
// there are more remaining results than this number, the response
// message
// will contain a valid value in the `next_page_token` field.
//
// The default value is 20, and the maximum page size is 100.
func (c *ProjectsModelsVersionsListCall) PageSize(pageSize int64) *ProjectsModelsVersionsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": A page token to
// request the next page of results.
//
// You get the token from the `next_page_token` field of the response
// from
// the previous call.
func (c *ProjectsModelsVersionsListCall) PageToken(pageToken string) *ProjectsModelsVersionsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsVersionsListCall) Fields(s ...googleapi.Field) *ProjectsModelsVersionsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsModelsVersionsListCall) IfNoneMatch(entityTag string) *ProjectsModelsVersionsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsVersionsListCall) Context(ctx context.Context) *ProjectsModelsVersionsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsVersionsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsVersionsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+parent}/versions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.models.versions.list" call.
// Exactly one of *GoogleCloudMlV1beta1__ListVersionsResponse or error
// will be non-nil. Any non-2xx status code is an error. Response
// headers are in either
// *GoogleCloudMlV1beta1__ListVersionsResponse.ServerResponse.Header or
// (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsModelsVersionsListCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__ListVersionsResponse, error) {
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
	ret := &GoogleCloudMlV1beta1__ListVersionsResponse{
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
	//   "description": "Gets basic information about all the versions of a model.\n\nIf you expect that a model has a lot of versions, or if you need to handle\nonly a limited number of results at a time, you can request that the list\nbe retrieved in batches (called pages):",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}/versions",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.models.versions.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "pageSize": {
	//       "description": "Optional. The number of versions to retrieve per \"page\" of results. If\nthere are more remaining results than this number, the response message\nwill contain a valid value in the `next_page_token` field.\n\nThe default value is 20, and the maximum page size is 100.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A page token to request the next page of results.\n\nYou get the token from the `next_page_token` field of the response from\nthe previous call.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Required. The name of the model for which to list the version.\n\nAuthorization: requires `Viewer` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+parent}/versions",
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__ListVersionsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsModelsVersionsListCall) Pages(ctx context.Context, f func(*GoogleCloudMlV1beta1__ListVersionsResponse) error) error {
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

// method id "ml.projects.models.versions.setDefault":

type ProjectsModelsVersionsSetDefaultCall struct {
	s                                              *Service
	name                                           string
	googlecloudmlv1beta1__setdefaultversionrequest *GoogleCloudMlV1beta1__SetDefaultVersionRequest
	urlParams_                                     gensupport.URLParams
	ctx_                                           context.Context
	header_                                        http.Header
}

// SetDefault: Designates a version to be the default for the
// model.
//
// The default version is used for prediction requests made against the
// model
// that don't specify a version.
//
// The first version to be created for a model is automatically set as
// the
// default. You must make any subsequent changes to the default
// version
// setting manually using this method.
func (r *ProjectsModelsVersionsService) SetDefault(name string, googlecloudmlv1beta1__setdefaultversionrequest *GoogleCloudMlV1beta1__SetDefaultVersionRequest) *ProjectsModelsVersionsSetDefaultCall {
	c := &ProjectsModelsVersionsSetDefaultCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.googlecloudmlv1beta1__setdefaultversionrequest = googlecloudmlv1beta1__setdefaultversionrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsModelsVersionsSetDefaultCall) Fields(s ...googleapi.Field) *ProjectsModelsVersionsSetDefaultCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsModelsVersionsSetDefaultCall) Context(ctx context.Context) *ProjectsModelsVersionsSetDefaultCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsModelsVersionsSetDefaultCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsModelsVersionsSetDefaultCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.googlecloudmlv1beta1__setdefaultversionrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}:setDefault")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.models.versions.setDefault" call.
// Exactly one of *GoogleCloudMlV1beta1__Version or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GoogleCloudMlV1beta1__Version.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsModelsVersionsSetDefaultCall) Do(opts ...googleapi.CallOption) (*GoogleCloudMlV1beta1__Version, error) {
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
	ret := &GoogleCloudMlV1beta1__Version{
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
	//   "description": "Designates a version to be the default for the model.\n\nThe default version is used for prediction requests made against the model\nthat don't specify a version.\n\nThe first version to be created for a model is automatically set as the\ndefault. You must make any subsequent changes to the default version\nsetting manually using this method.",
	//   "flatPath": "v1beta1/projects/{projectsId}/models/{modelsId}/versions/{versionsId}:setDefault",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.models.versions.setDefault",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. The name of the version to make the default for the model. You\ncan get the names of all the versions of a model by calling\n[projects.models.versions.list](/ml/reference/rest/v1beta1/projects.models.versions/list).\n\nAuthorization: requires `Editor` role on the parent project.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/models/[^/]+/versions/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}:setDefault",
	//   "request": {
	//     "$ref": "GoogleCloudMlV1beta1__SetDefaultVersionRequest"
	//   },
	//   "response": {
	//     "$ref": "GoogleCloudMlV1beta1__Version"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.operations.cancel":

type ProjectsOperationsCancelCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Cancel: Starts asynchronous cancellation on a long-running operation.
//  The server
// makes a best effort to cancel the operation, but success is
// not
// guaranteed.  If the server doesn't support this method, it
// returns
// `google.rpc.Code.UNIMPLEMENTED`.  Clients can
// use
// Operations.GetOperation or
// other methods to check whether the cancellation succeeded or whether
// the
// operation completed despite cancellation. On successful
// cancellation,
// the operation is not deleted; instead, it becomes an operation
// with
// an Operation.error value with a google.rpc.Status.code of
// 1,
// corresponding to `Code.CANCELLED`.
func (r *ProjectsOperationsService) Cancel(name string) *ProjectsOperationsCancelCall {
	c := &ProjectsOperationsCancelCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsOperationsCancelCall) Fields(s ...googleapi.Field) *ProjectsOperationsCancelCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsOperationsCancelCall) Context(ctx context.Context) *ProjectsOperationsCancelCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsOperationsCancelCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsOperationsCancelCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}:cancel")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.operations.cancel" call.
// Exactly one of *GoogleProtobuf__Empty or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GoogleProtobuf__Empty.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsOperationsCancelCall) Do(opts ...googleapi.CallOption) (*GoogleProtobuf__Empty, error) {
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
	ret := &GoogleProtobuf__Empty{
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
	//   "description": "Starts asynchronous cancellation on a long-running operation.  The server\nmakes a best effort to cancel the operation, but success is not\nguaranteed.  If the server doesn't support this method, it returns\n`google.rpc.Code.UNIMPLEMENTED`.  Clients can use\nOperations.GetOperation or\nother methods to check whether the cancellation succeeded or whether the\noperation completed despite cancellation. On successful cancellation,\nthe operation is not deleted; instead, it becomes an operation with\nan Operation.error value with a google.rpc.Status.code of 1,\ncorresponding to `Code.CANCELLED`.",
	//   "flatPath": "v1beta1/projects/{projectsId}/operations/{operationsId}:cancel",
	//   "httpMethod": "POST",
	//   "id": "ml.projects.operations.cancel",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the operation resource to be cancelled.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/operations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}:cancel",
	//   "response": {
	//     "$ref": "GoogleProtobuf__Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.operations.delete":

type ProjectsOperationsDeleteCall struct {
	s          *Service
	name       string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a long-running operation. This method indicates that
// the client is
// no longer interested in the operation result. It does not cancel
// the
// operation. If the server doesn't support this method, it
// returns
// `google.rpc.Code.UNIMPLEMENTED`.
func (r *ProjectsOperationsService) Delete(name string) *ProjectsOperationsDeleteCall {
	c := &ProjectsOperationsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsOperationsDeleteCall) Fields(s ...googleapi.Field) *ProjectsOperationsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsOperationsDeleteCall) Context(ctx context.Context) *ProjectsOperationsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsOperationsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsOperationsDeleteCall) doRequest(alt string) (*http.Response, error) {
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

// Do executes the "ml.projects.operations.delete" call.
// Exactly one of *GoogleProtobuf__Empty or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GoogleProtobuf__Empty.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsOperationsDeleteCall) Do(opts ...googleapi.CallOption) (*GoogleProtobuf__Empty, error) {
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
	ret := &GoogleProtobuf__Empty{
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
	//   "description": "Deletes a long-running operation. This method indicates that the client is\nno longer interested in the operation result. It does not cancel the\noperation. If the server doesn't support this method, it returns\n`google.rpc.Code.UNIMPLEMENTED`.",
	//   "flatPath": "v1beta1/projects/{projectsId}/operations/{operationsId}",
	//   "httpMethod": "DELETE",
	//   "id": "ml.projects.operations.delete",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the operation resource to be deleted.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/operations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleProtobuf__Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.operations.get":

type ProjectsOperationsGetCall struct {
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
func (r *ProjectsOperationsService) Get(name string) *ProjectsOperationsGetCall {
	c := &ProjectsOperationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsOperationsGetCall) Fields(s ...googleapi.Field) *ProjectsOperationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsOperationsGetCall) IfNoneMatch(entityTag string) *ProjectsOperationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsOperationsGetCall) Context(ctx context.Context) *ProjectsOperationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsOperationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsOperationsGetCall) doRequest(alt string) (*http.Response, error) {
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

// Do executes the "ml.projects.operations.get" call.
// Exactly one of *GoogleLongrunning__Operation or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GoogleLongrunning__Operation.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsOperationsGetCall) Do(opts ...googleapi.CallOption) (*GoogleLongrunning__Operation, error) {
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
	ret := &GoogleLongrunning__Operation{
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
	//   "flatPath": "v1beta1/projects/{projectsId}/operations/{operationsId}",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.operations.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the operation resource.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/operations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+name}",
	//   "response": {
	//     "$ref": "GoogleLongrunning__Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "ml.projects.operations.list":

type ProjectsOperationsListCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists operations that match the specified filter in the
// request. If the
// server doesn't support this method, it returns
// `UNIMPLEMENTED`.
//
// NOTE: the `name` binding below allows API services to override the
// binding
// to use different resource name schemes, such as `users/*/operations`.
func (r *ProjectsOperationsService) List(name string) *ProjectsOperationsListCall {
	c := &ProjectsOperationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Filter sets the optional parameter "filter": The standard list
// filter.
func (c *ProjectsOperationsListCall) Filter(filter string) *ProjectsOperationsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": The standard list
// page size.
func (c *ProjectsOperationsListCall) PageSize(pageSize int64) *ProjectsOperationsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The standard list
// page token.
func (c *ProjectsOperationsListCall) PageToken(pageToken string) *ProjectsOperationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsOperationsListCall) Fields(s ...googleapi.Field) *ProjectsOperationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsOperationsListCall) IfNoneMatch(entityTag string) *ProjectsOperationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsOperationsListCall) Context(ctx context.Context) *ProjectsOperationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsOperationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsOperationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+name}/operations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "ml.projects.operations.list" call.
// Exactly one of *GoogleLongrunning__ListOperationsResponse or error
// will be non-nil. Any non-2xx status code is an error. Response
// headers are in either
// *GoogleLongrunning__ListOperationsResponse.ServerResponse.Header or
// (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ProjectsOperationsListCall) Do(opts ...googleapi.CallOption) (*GoogleLongrunning__ListOperationsResponse, error) {
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
	ret := &GoogleLongrunning__ListOperationsResponse{
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
	//   "description": "Lists operations that match the specified filter in the request. If the\nserver doesn't support this method, it returns `UNIMPLEMENTED`.\n\nNOTE: the `name` binding below allows API services to override the binding\nto use different resource name schemes, such as `users/*/operations`.",
	//   "flatPath": "v1beta1/projects/{projectsId}/operations",
	//   "httpMethod": "GET",
	//   "id": "ml.projects.operations.list",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "The standard list filter.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "name": {
	//       "description": "The name of the operation collection.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+$",
	//       "required": true,
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
	//   "path": "v1beta1/{+name}/operations",
	//   "response": {
	//     "$ref": "GoogleLongrunning__ListOperationsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsOperationsListCall) Pages(ctx context.Context, f func(*GoogleLongrunning__ListOperationsResponse) error) error {
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

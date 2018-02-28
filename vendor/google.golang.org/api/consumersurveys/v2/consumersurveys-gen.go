// Package consumersurveys provides access to the Consumer Surveys API.
//
// Usage example:
//
//   import "google.golang.org/api/consumersurveys/v2"
//   ...
//   consumersurveysService, err := consumersurveys.New(oauthHttpClient)
package consumersurveys // import "google.golang.org/api/consumersurveys/v2"

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

const apiId = "consumersurveys:v2"
const apiName = "consumersurveys"
const apiVersion = "v2"
const basePath = "https://www.googleapis.com/consumersurveys/v2/"

// OAuth2 scopes used by this API.
const (
	// View and edit your surveys and results
	ConsumersurveysScope = "https://www.googleapis.com/auth/consumersurveys"

	// View the results for your surveys
	ConsumersurveysReadonlyScope = "https://www.googleapis.com/auth/consumersurveys.readonly"

	// View your email address
	UserinfoEmailScope = "https://www.googleapis.com/auth/userinfo.email"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Mobileapppanels = NewMobileapppanelsService(s)
	s.Results = NewResultsService(s)
	s.Surveys = NewSurveysService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Mobileapppanels *MobileapppanelsService

	Results *ResultsService

	Surveys *SurveysService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewMobileapppanelsService(s *Service) *MobileapppanelsService {
	rs := &MobileapppanelsService{s: s}
	return rs
}

type MobileapppanelsService struct {
	s *Service
}

func NewResultsService(s *Service) *ResultsService {
	rs := &ResultsService{s: s}
	return rs
}

type ResultsService struct {
	s *Service
}

func NewSurveysService(s *Service) *SurveysService {
	rs := &SurveysService{s: s}
	return rs
}

type SurveysService struct {
	s *Service
}

type FieldMask struct {
	Fields []*FieldMask `json:"fields,omitempty"`

	Id int64 `json:"id,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FieldMask) MarshalJSON() ([]byte, error) {
	type noMethod FieldMask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MobileAppPanel struct {
	Country string `json:"country,omitempty"`

	IsPublicPanel bool `json:"isPublicPanel,omitempty"`

	Language string `json:"language,omitempty"`

	MobileAppPanelId string `json:"mobileAppPanelId,omitempty"`

	Name string `json:"name,omitempty"`

	Owners []string `json:"owners,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Country") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Country") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MobileAppPanel) MarshalJSON() ([]byte, error) {
	type noMethod MobileAppPanel
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MobileAppPanelsListResponse struct {
	PageInfo *PageInfo `json:"pageInfo,omitempty"`

	// RequestId: Unique request ID used for logging and debugging. Please
	// include in any error reporting or troubleshooting requests.
	RequestId string `json:"requestId,omitempty"`

	// Resources: An individual predefined panel of Opinion Rewards mobile
	// users.
	Resources []*MobileAppPanel `json:"resources,omitempty"`

	TokenPagination *TokenPagination `json:"tokenPagination,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "PageInfo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PageInfo") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MobileAppPanelsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod MobileAppPanelsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PageInfo struct {
	ResultPerPage int64 `json:"resultPerPage,omitempty"`

	StartIndex int64 `json:"startIndex,omitempty"`

	TotalResults int64 `json:"totalResults,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ResultPerPage") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ResultPerPage") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PageInfo) MarshalJSON() ([]byte, error) {
	type noMethod PageInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResultsGetRequest struct {
	ResultMask *ResultsMask `json:"resultMask,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ResultMask") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ResultMask") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultsGetRequest) MarshalJSON() ([]byte, error) {
	type noMethod ResultsGetRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResultsMask struct {
	Fields []*FieldMask `json:"fields,omitempty"`

	Projection string `json:"projection,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultsMask) MarshalJSON() ([]byte, error) {
	type noMethod ResultsMask
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Survey struct {
	Audience *SurveyAudience `json:"audience,omitempty"`

	Cost *SurveyCost `json:"cost,omitempty"`

	CustomerData string `json:"customerData,omitempty"`

	Description string `json:"description,omitempty"`

	Owners []string `json:"owners,omitempty"`

	Questions []*SurveyQuestion `json:"questions,omitempty"`

	RejectionReason *SurveyRejection `json:"rejectionReason,omitempty"`

	State string `json:"state,omitempty"`

	SurveyUrlId string `json:"surveyUrlId,omitempty"`

	Title string `json:"title,omitempty"`

	WantedResponseCount int64 `json:"wantedResponseCount,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Audience") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Audience") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Survey) MarshalJSON() ([]byte, error) {
	type noMethod Survey
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveyAudience struct {
	Ages []string `json:"ages,omitempty"`

	Country string `json:"country,omitempty"`

	CountrySubdivision string `json:"countrySubdivision,omitempty"`

	Gender string `json:"gender,omitempty"`

	Languages []string `json:"languages,omitempty"`

	MobileAppPanelId string `json:"mobileAppPanelId,omitempty"`

	PopulationSource string `json:"populationSource,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Ages") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Ages") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveyAudience) MarshalJSON() ([]byte, error) {
	type noMethod SurveyAudience
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveyCost struct {
	CostPerResponseNanos int64 `json:"costPerResponseNanos,omitempty,string"`

	CurrencyCode string `json:"currencyCode,omitempty"`

	MaxCostPerResponseNanos int64 `json:"maxCostPerResponseNanos,omitempty,string"`

	Nanos int64 `json:"nanos,omitempty,string"`

	// ForceSendFields is a list of field names (e.g.
	// "CostPerResponseNanos") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CostPerResponseNanos") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SurveyCost) MarshalJSON() ([]byte, error) {
	type noMethod SurveyCost
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveyQuestion struct {
	AnswerOrder string `json:"answerOrder,omitempty"`

	Answers []string `json:"answers,omitempty"`

	HasOther bool `json:"hasOther,omitempty"`

	HighValueLabel string `json:"highValueLabel,omitempty"`

	Images []*SurveyQuestionImage `json:"images,omitempty"`

	LastAnswerPositionPinned bool `json:"lastAnswerPositionPinned,omitempty"`

	LowValueLabel string `json:"lowValueLabel,omitempty"`

	MustPickSuggestion bool `json:"mustPickSuggestion,omitempty"`

	NumStars string `json:"numStars,omitempty"`

	OpenTextPlaceholder string `json:"openTextPlaceholder,omitempty"`

	OpenTextSuggestions []string `json:"openTextSuggestions,omitempty"`

	Question string `json:"question,omitempty"`

	SentimentText string `json:"sentimentText,omitempty"`

	SingleLineResponse bool `json:"singleLineResponse,omitempty"`

	ThresholdAnswers []string `json:"thresholdAnswers,omitempty"`

	Type string `json:"type,omitempty"`

	UnitOfMeasurementLabel string `json:"unitOfMeasurementLabel,omitempty"`

	VideoId string `json:"videoId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AnswerOrder") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AnswerOrder") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveyQuestion) MarshalJSON() ([]byte, error) {
	type noMethod SurveyQuestion
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveyQuestionImage struct {
	AltText string `json:"altText,omitempty"`

	Data string `json:"data,omitempty"`

	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AltText") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AltText") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveyQuestionImage) MarshalJSON() ([]byte, error) {
	type noMethod SurveyQuestionImage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveyRejection struct {
	Explanation string `json:"explanation,omitempty"`

	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Explanation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Explanation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveyRejection) MarshalJSON() ([]byte, error) {
	type noMethod SurveyRejection
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveyResults struct {
	Status string `json:"status,omitempty"`

	SurveyUrlId string `json:"surveyUrlId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Status") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Status") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveyResults) MarshalJSON() ([]byte, error) {
	type noMethod SurveyResults
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveysDeleteResponse struct {
	// RequestId: Unique request ID used for logging and debugging. Please
	// include in any error reporting or troubleshooting requests.
	RequestId string `json:"requestId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "RequestId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RequestId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveysDeleteResponse) MarshalJSON() ([]byte, error) {
	type noMethod SurveysDeleteResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveysListResponse struct {
	PageInfo *PageInfo `json:"pageInfo,omitempty"`

	// RequestId: Unique request ID used for logging and debugging. Please
	// include in any error reporting or troubleshooting requests.
	RequestId string `json:"requestId,omitempty"`

	// Resources: An individual survey resource.
	Resources []*Survey `json:"resources,omitempty"`

	TokenPagination *TokenPagination `json:"tokenPagination,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "PageInfo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PageInfo") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveysListResponse) MarshalJSON() ([]byte, error) {
	type noMethod SurveysListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveysStartRequest struct {
	// MaxCostPerResponseNanos: Threshold to start a survey automically if
	// the quoted prices is less than or equal to this value. See
	// Survey.Cost for more details.
	MaxCostPerResponseNanos int64 `json:"maxCostPerResponseNanos,omitempty,string"`

	// ForceSendFields is a list of field names (e.g.
	// "MaxCostPerResponseNanos") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MaxCostPerResponseNanos")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SurveysStartRequest) MarshalJSON() ([]byte, error) {
	type noMethod SurveysStartRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveysStartResponse struct {
	// RequestId: Unique request ID used for logging and debugging. Please
	// include in any error reporting or troubleshooting requests.
	RequestId string `json:"requestId,omitempty"`

	// Resource: Survey object containing the specification of the started
	// Survey.
	Resource *Survey `json:"resource,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "RequestId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RequestId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveysStartResponse) MarshalJSON() ([]byte, error) {
	type noMethod SurveysStartResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SurveysStopResponse struct {
	// RequestId: Unique request ID used for logging and debugging. Please
	// include in any error reporting or troubleshooting requests.
	RequestId string `json:"requestId,omitempty"`

	// Resource: Survey object containing the specification of the stopped
	// Survey.
	Resource *Survey `json:"resource,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "RequestId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RequestId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SurveysStopResponse) MarshalJSON() ([]byte, error) {
	type noMethod SurveysStopResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TokenPagination struct {
	NextPageToken string `json:"nextPageToken,omitempty"`

	PreviousPageToken string `json:"previousPageToken,omitempty"`

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

func (s *TokenPagination) MarshalJSON() ([]byte, error) {
	type noMethod TokenPagination
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "consumersurveys.mobileapppanels.get":

type MobileapppanelsGetCall struct {
	s            *Service
	panelId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a MobileAppPanel that is available to the
// authenticated user.
func (r *MobileapppanelsService) Get(panelId string) *MobileapppanelsGetCall {
	c := &MobileapppanelsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.panelId = panelId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobileapppanelsGetCall) Fields(s ...googleapi.Field) *MobileapppanelsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MobileapppanelsGetCall) IfNoneMatch(entityTag string) *MobileapppanelsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobileapppanelsGetCall) Context(ctx context.Context) *MobileapppanelsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobileapppanelsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobileapppanelsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mobileAppPanels/{panelId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"panelId": c.panelId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.mobileapppanels.get" call.
// Exactly one of *MobileAppPanel or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *MobileAppPanel.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MobileapppanelsGetCall) Do(opts ...googleapi.CallOption) (*MobileAppPanel, error) {
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
	ret := &MobileAppPanel{
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
	//   "description": "Retrieves a MobileAppPanel that is available to the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "consumersurveys.mobileapppanels.get",
	//   "parameterOrder": [
	//     "panelId"
	//   ],
	//   "parameters": {
	//     "panelId": {
	//       "description": "External URL ID for the panel.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mobileAppPanels/{panelId}",
	//   "response": {
	//     "$ref": "MobileAppPanel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/consumersurveys.readonly",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.mobileapppanels.list":

type MobileapppanelsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the MobileAppPanels available to the authenticated user.
func (r *MobileapppanelsService) List() *MobileapppanelsListCall {
	c := &MobileapppanelsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// MaxResults sets the optional parameter "maxResults":
func (c *MobileapppanelsListCall) MaxResults(maxResults int64) *MobileapppanelsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// StartIndex sets the optional parameter "startIndex":
func (c *MobileapppanelsListCall) StartIndex(startIndex int64) *MobileapppanelsListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Token sets the optional parameter "token":
func (c *MobileapppanelsListCall) Token(token string) *MobileapppanelsListCall {
	c.urlParams_.Set("token", token)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobileapppanelsListCall) Fields(s ...googleapi.Field) *MobileapppanelsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MobileapppanelsListCall) IfNoneMatch(entityTag string) *MobileapppanelsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobileapppanelsListCall) Context(ctx context.Context) *MobileapppanelsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobileapppanelsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobileapppanelsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mobileAppPanels")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.mobileapppanels.list" call.
// Exactly one of *MobileAppPanelsListResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *MobileAppPanelsListResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MobileapppanelsListCall) Do(opts ...googleapi.CallOption) (*MobileAppPanelsListResponse, error) {
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
	ret := &MobileAppPanelsListResponse{
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
	//   "description": "Lists the MobileAppPanels available to the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "consumersurveys.mobileapppanels.list",
	//   "parameters": {
	//     "maxResults": {
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "startIndex": {
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "token": {
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mobileAppPanels",
	//   "response": {
	//     "$ref": "MobileAppPanelsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/consumersurveys.readonly",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.mobileapppanels.update":

type MobileapppanelsUpdateCall struct {
	s              *Service
	panelId        string
	mobileapppanel *MobileAppPanel
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Update: Updates a MobileAppPanel. Currently the only property that
// can be updated is the owners property.
func (r *MobileapppanelsService) Update(panelId string, mobileapppanel *MobileAppPanel) *MobileapppanelsUpdateCall {
	c := &MobileapppanelsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.panelId = panelId
	c.mobileapppanel = mobileapppanel
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobileapppanelsUpdateCall) Fields(s ...googleapi.Field) *MobileapppanelsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobileapppanelsUpdateCall) Context(ctx context.Context) *MobileapppanelsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobileapppanelsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobileapppanelsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.mobileapppanel)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mobileAppPanels/{panelId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"panelId": c.panelId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.mobileapppanels.update" call.
// Exactly one of *MobileAppPanel or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *MobileAppPanel.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MobileapppanelsUpdateCall) Do(opts ...googleapi.CallOption) (*MobileAppPanel, error) {
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
	ret := &MobileAppPanel{
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
	//   "description": "Updates a MobileAppPanel. Currently the only property that can be updated is the owners property.",
	//   "httpMethod": "PUT",
	//   "id": "consumersurveys.mobileapppanels.update",
	//   "parameterOrder": [
	//     "panelId"
	//   ],
	//   "parameters": {
	//     "panelId": {
	//       "description": "External URL ID for the panel.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mobileAppPanels/{panelId}",
	//   "request": {
	//     "$ref": "MobileAppPanel"
	//   },
	//   "response": {
	//     "$ref": "MobileAppPanel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.results.get":

type ResultsGetCall struct {
	s                 *Service
	surveyUrlId       string
	resultsgetrequest *ResultsGetRequest
	urlParams_        gensupport.URLParams
	ifNoneMatch_      string
	ctx_              context.Context
	header_           http.Header
}

// Get: Retrieves any survey results that have been produced so far.
// Results are formatted as an Excel file. You must add "?alt=media" to
// the URL as an argument to get results.
func (r *ResultsService) Get(surveyUrlId string, resultsgetrequest *ResultsGetRequest) *ResultsGetCall {
	c := &ResultsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.surveyUrlId = surveyUrlId
	c.resultsgetrequest = resultsgetrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResultsGetCall) Fields(s ...googleapi.Field) *ResultsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ResultsGetCall) IfNoneMatch(entityTag string) *ResultsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do and Download
// methods. Any pending HTTP request will be aborted if the provided
// context is canceled.
func (c *ResultsGetCall) Context(ctx context.Context) *ResultsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResultsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResultsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys/{surveyUrlId}/results")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"surveyUrlId": c.surveyUrlId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Download fetches the API endpoint's "media" value, instead of the normal
// API response value. If the returned error is nil, the Response is guaranteed to
// have a 2xx status code. Callers must close the Response.Body as usual.
func (c *ResultsGetCall) Download(opts ...googleapi.CallOption) (*http.Response, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("media")
	if err != nil {
		return nil, err
	}
	if err := googleapi.CheckMediaResponse(res); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res, nil
}

// Do executes the "consumersurveys.results.get" call.
// Exactly one of *SurveyResults or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *SurveyResults.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResultsGetCall) Do(opts ...googleapi.CallOption) (*SurveyResults, error) {
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
	ret := &SurveyResults{
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
	//   "description": "Retrieves any survey results that have been produced so far. Results are formatted as an Excel file. You must add \"?alt=media\" to the URL as an argument to get results.",
	//   "httpMethod": "GET",
	//   "id": "consumersurveys.results.get",
	//   "parameterOrder": [
	//     "surveyUrlId"
	//   ],
	//   "parameters": {
	//     "surveyUrlId": {
	//       "description": "External URL ID for the survey.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys/{surveyUrlId}/results",
	//   "request": {
	//     "$ref": "ResultsGetRequest"
	//   },
	//   "response": {
	//     "$ref": "SurveyResults"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/consumersurveys.readonly",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ],
	//   "supportsMediaDownload": true
	// }

}

// method id "consumersurveys.surveys.delete":

type SurveysDeleteCall struct {
	s           *Service
	surveyUrlId string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Delete: Removes a survey from view in all user GET requests.
func (r *SurveysService) Delete(surveyUrlId string) *SurveysDeleteCall {
	c := &SurveysDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.surveyUrlId = surveyUrlId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysDeleteCall) Fields(s ...googleapi.Field) *SurveysDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysDeleteCall) Context(ctx context.Context) *SurveysDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys/{surveyUrlId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"surveyUrlId": c.surveyUrlId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.delete" call.
// Exactly one of *SurveysDeleteResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *SurveysDeleteResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SurveysDeleteCall) Do(opts ...googleapi.CallOption) (*SurveysDeleteResponse, error) {
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
	ret := &SurveysDeleteResponse{
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
	//   "description": "Removes a survey from view in all user GET requests.",
	//   "httpMethod": "DELETE",
	//   "id": "consumersurveys.surveys.delete",
	//   "parameterOrder": [
	//     "surveyUrlId"
	//   ],
	//   "parameters": {
	//     "surveyUrlId": {
	//       "description": "External URL ID for the survey.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys/{surveyUrlId}",
	//   "response": {
	//     "$ref": "SurveysDeleteResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.surveys.get":

type SurveysGetCall struct {
	s            *Service
	surveyUrlId  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves information about the specified survey.
func (r *SurveysService) Get(surveyUrlId string) *SurveysGetCall {
	c := &SurveysGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.surveyUrlId = surveyUrlId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysGetCall) Fields(s ...googleapi.Field) *SurveysGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SurveysGetCall) IfNoneMatch(entityTag string) *SurveysGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysGetCall) Context(ctx context.Context) *SurveysGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys/{surveyUrlId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"surveyUrlId": c.surveyUrlId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.get" call.
// Exactly one of *Survey or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Survey.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SurveysGetCall) Do(opts ...googleapi.CallOption) (*Survey, error) {
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
	ret := &Survey{
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
	//   "description": "Retrieves information about the specified survey.",
	//   "httpMethod": "GET",
	//   "id": "consumersurveys.surveys.get",
	//   "parameterOrder": [
	//     "surveyUrlId"
	//   ],
	//   "parameters": {
	//     "surveyUrlId": {
	//       "description": "External URL ID for the survey.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys/{surveyUrlId}",
	//   "response": {
	//     "$ref": "Survey"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/consumersurveys.readonly",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.surveys.insert":

type SurveysInsertCall struct {
	s          *Service
	survey     *Survey
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates a survey.
func (r *SurveysService) Insert(survey *Survey) *SurveysInsertCall {
	c := &SurveysInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.survey = survey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysInsertCall) Fields(s ...googleapi.Field) *SurveysInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysInsertCall) Context(ctx context.Context) *SurveysInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.survey)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.insert" call.
// Exactly one of *Survey or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Survey.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SurveysInsertCall) Do(opts ...googleapi.CallOption) (*Survey, error) {
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
	ret := &Survey{
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
	//   "description": "Creates a survey.",
	//   "httpMethod": "POST",
	//   "id": "consumersurveys.surveys.insert",
	//   "path": "surveys",
	//   "request": {
	//     "$ref": "Survey"
	//   },
	//   "response": {
	//     "$ref": "Survey"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.surveys.list":

type SurveysListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the surveys owned by the authenticated user.
func (r *SurveysService) List() *SurveysListCall {
	c := &SurveysListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// MaxResults sets the optional parameter "maxResults":
func (c *SurveysListCall) MaxResults(maxResults int64) *SurveysListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// StartIndex sets the optional parameter "startIndex":
func (c *SurveysListCall) StartIndex(startIndex int64) *SurveysListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Token sets the optional parameter "token":
func (c *SurveysListCall) Token(token string) *SurveysListCall {
	c.urlParams_.Set("token", token)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysListCall) Fields(s ...googleapi.Field) *SurveysListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SurveysListCall) IfNoneMatch(entityTag string) *SurveysListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysListCall) Context(ctx context.Context) *SurveysListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.list" call.
// Exactly one of *SurveysListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *SurveysListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SurveysListCall) Do(opts ...googleapi.CallOption) (*SurveysListResponse, error) {
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
	ret := &SurveysListResponse{
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
	//   "description": "Lists the surveys owned by the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "consumersurveys.surveys.list",
	//   "parameters": {
	//     "maxResults": {
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "startIndex": {
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "token": {
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys",
	//   "response": {
	//     "$ref": "SurveysListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/consumersurveys.readonly",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.surveys.start":

type SurveysStartCall struct {
	s                   *Service
	resourceId          string
	surveysstartrequest *SurveysStartRequest
	urlParams_          gensupport.URLParams
	ctx_                context.Context
	header_             http.Header
}

// Start: Begins running a survey.
func (r *SurveysService) Start(resourceId string, surveysstartrequest *SurveysStartRequest) *SurveysStartCall {
	c := &SurveysStartCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.resourceId = resourceId
	c.surveysstartrequest = surveysstartrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysStartCall) Fields(s ...googleapi.Field) *SurveysStartCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysStartCall) Context(ctx context.Context) *SurveysStartCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysStartCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysStartCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.surveysstartrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys/{resourceId}/start")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"resourceId": c.resourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.start" call.
// Exactly one of *SurveysStartResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *SurveysStartResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SurveysStartCall) Do(opts ...googleapi.CallOption) (*SurveysStartResponse, error) {
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
	ret := &SurveysStartResponse{
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
	//   "description": "Begins running a survey.",
	//   "httpMethod": "POST",
	//   "id": "consumersurveys.surveys.start",
	//   "parameterOrder": [
	//     "resourceId"
	//   ],
	//   "parameters": {
	//     "resourceId": {
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys/{resourceId}/start",
	//   "request": {
	//     "$ref": "SurveysStartRequest"
	//   },
	//   "response": {
	//     "$ref": "SurveysStartResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.surveys.stop":

type SurveysStopCall struct {
	s          *Service
	resourceId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Stop: Stops a running survey.
func (r *SurveysService) Stop(resourceId string) *SurveysStopCall {
	c := &SurveysStopCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.resourceId = resourceId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysStopCall) Fields(s ...googleapi.Field) *SurveysStopCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysStopCall) Context(ctx context.Context) *SurveysStopCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysStopCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysStopCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys/{resourceId}/stop")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"resourceId": c.resourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.stop" call.
// Exactly one of *SurveysStopResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *SurveysStopResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SurveysStopCall) Do(opts ...googleapi.CallOption) (*SurveysStopResponse, error) {
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
	ret := &SurveysStopResponse{
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
	//   "description": "Stops a running survey.",
	//   "httpMethod": "POST",
	//   "id": "consumersurveys.surveys.stop",
	//   "parameterOrder": [
	//     "resourceId"
	//   ],
	//   "parameters": {
	//     "resourceId": {
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys/{resourceId}/stop",
	//   "response": {
	//     "$ref": "SurveysStopResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

// method id "consumersurveys.surveys.update":

type SurveysUpdateCall struct {
	s           *Service
	surveyUrlId string
	survey      *Survey
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Update: Updates a survey. Currently the only property that can be
// updated is the owners property.
func (r *SurveysService) Update(surveyUrlId string, survey *Survey) *SurveysUpdateCall {
	c := &SurveysUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.surveyUrlId = surveyUrlId
	c.survey = survey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SurveysUpdateCall) Fields(s ...googleapi.Field) *SurveysUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SurveysUpdateCall) Context(ctx context.Context) *SurveysUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SurveysUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SurveysUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.survey)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "surveys/{surveyUrlId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"surveyUrlId": c.surveyUrlId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "consumersurveys.surveys.update" call.
// Exactly one of *Survey or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Survey.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SurveysUpdateCall) Do(opts ...googleapi.CallOption) (*Survey, error) {
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
	ret := &Survey{
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
	//   "description": "Updates a survey. Currently the only property that can be updated is the owners property.",
	//   "httpMethod": "PUT",
	//   "id": "consumersurveys.surveys.update",
	//   "parameterOrder": [
	//     "surveyUrlId"
	//   ],
	//   "parameters": {
	//     "surveyUrlId": {
	//       "description": "External URL ID for the survey.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "surveys/{surveyUrlId}",
	//   "request": {
	//     "$ref": "Survey"
	//   },
	//   "response": {
	//     "$ref": "Survey"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/consumersurveys",
	//     "https://www.googleapis.com/auth/userinfo.email"
	//   ]
	// }

}

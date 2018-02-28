// Package doubleclicksearch provides access to the DoubleClick Search API.
//
// See https://developers.google.com/doubleclick-search/
//
// Usage example:
//
//   import "google.golang.org/api/doubleclicksearch/v2"
//   ...
//   doubleclicksearchService, err := doubleclicksearch.New(oauthHttpClient)
package doubleclicksearch // import "google.golang.org/api/doubleclicksearch/v2"

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

const apiId = "doubleclicksearch:v2"
const apiName = "doubleclicksearch"
const apiVersion = "v2"
const basePath = "https://www.googleapis.com/doubleclicksearch/v2/"

// OAuth2 scopes used by this API.
const (
	// View and manage your advertising data in DoubleClick Search
	DoubleclicksearchScope = "https://www.googleapis.com/auth/doubleclicksearch"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Conversion = NewConversionService(s)
	s.Reports = NewReportsService(s)
	s.SavedColumns = NewSavedColumnsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Conversion *ConversionService

	Reports *ReportsService

	SavedColumns *SavedColumnsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewConversionService(s *Service) *ConversionService {
	rs := &ConversionService{s: s}
	return rs
}

type ConversionService struct {
	s *Service
}

func NewReportsService(s *Service) *ReportsService {
	rs := &ReportsService{s: s}
	return rs
}

type ReportsService struct {
	s *Service
}

func NewSavedColumnsService(s *Service) *SavedColumnsService {
	rs := &SavedColumnsService{s: s}
	return rs
}

type SavedColumnsService struct {
	s *Service
}

// Availability: A message containing availability data relevant to
// DoubleClick Search.
type Availability struct {
	// AdvertiserId: DS advertiser ID.
	AdvertiserId int64 `json:"advertiserId,omitempty,string"`

	// AgencyId: DS agency ID.
	AgencyId int64 `json:"agencyId,omitempty,string"`

	// AvailabilityTimestamp: The time by which all conversions have been
	// uploaded, in epoch millis UTC.
	AvailabilityTimestamp uint64 `json:"availabilityTimestamp,omitempty,string"`

	// SegmentationId: The numeric segmentation identifier (for example,
	// DoubleClick Search Floodlight activity ID).
	SegmentationId int64 `json:"segmentationId,omitempty,string"`

	// SegmentationName: The friendly segmentation identifier (for example,
	// DoubleClick Search Floodlight activity name).
	SegmentationName string `json:"segmentationName,omitempty"`

	// SegmentationType: The segmentation type that this availability is for
	// (its default value is FLOODLIGHT).
	SegmentationType string `json:"segmentationType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AdvertiserId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdvertiserId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Availability) MarshalJSON() ([]byte, error) {
	type noMethod Availability
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Conversion: A conversion containing data relevant to DoubleClick
// Search.
type Conversion struct {
	// AdGroupId: DS ad group ID.
	AdGroupId int64 `json:"adGroupId,omitempty,string"`

	// AdId: DS ad ID.
	AdId int64 `json:"adId,omitempty,string"`

	// AdvertiserId: DS advertiser ID.
	AdvertiserId int64 `json:"advertiserId,omitempty,string"`

	// AgencyId: DS agency ID.
	AgencyId int64 `json:"agencyId,omitempty,string"`

	// AttributionModel: Available to advertisers only after contacting
	// DoubleClick Search customer support.
	AttributionModel string `json:"attributionModel,omitempty"`

	// CampaignId: DS campaign ID.
	CampaignId int64 `json:"campaignId,omitempty,string"`

	// Channel: Sales channel for the product. Acceptable values are:
	// - "local": a physical store
	// - "online": an online store
	Channel string `json:"channel,omitempty"`

	// ClickId: DS click ID for the conversion.
	ClickId string `json:"clickId,omitempty"`

	// ConversionId: For offline conversions, this is an ID that advertisers
	// are required to provide. Advertisers can specify any ID that is
	// meaningful to them. For online conversions, DS copies the
	// dsConversionId or floodlightOrderId into this property depending on
	// the advertiser's Floodlight instructions.
	ConversionId string `json:"conversionId,omitempty"`

	// ConversionModifiedTimestamp: The time at which the conversion was
	// last modified, in epoch millis UTC.
	ConversionModifiedTimestamp uint64 `json:"conversionModifiedTimestamp,omitempty,string"`

	// ConversionTimestamp: The time at which the conversion took place, in
	// epoch millis UTC.
	ConversionTimestamp uint64 `json:"conversionTimestamp,omitempty,string"`

	// CountMillis: Available to advertisers only after contacting
	// DoubleClick Search customer support.
	CountMillis int64 `json:"countMillis,omitempty,string"`

	// CriterionId: DS criterion (keyword) ID.
	CriterionId int64 `json:"criterionId,omitempty,string"`

	// CurrencyCode: The currency code for the conversion's revenue. Should
	// be in ISO 4217 alphabetic (3-char) format.
	CurrencyCode string `json:"currencyCode,omitempty"`

	// CustomDimension: Custom dimensions for the conversion, which can be
	// used to filter data in a report.
	CustomDimension []*CustomDimension `json:"customDimension,omitempty"`

	// CustomMetric: Custom metrics for the conversion.
	CustomMetric []*CustomMetric `json:"customMetric,omitempty"`

	// DeviceType: The type of device on which the conversion occurred.
	DeviceType string `json:"deviceType,omitempty"`

	// DsConversionId: ID that DoubleClick Search generates for each
	// conversion.
	DsConversionId int64 `json:"dsConversionId,omitempty,string"`

	// EngineAccountId: DS engine account ID.
	EngineAccountId int64 `json:"engineAccountId,omitempty,string"`

	// FloodlightOrderId: The Floodlight order ID provided by the advertiser
	// for the conversion.
	FloodlightOrderId string `json:"floodlightOrderId,omitempty"`

	// InventoryAccountId: ID that DS generates and uses to uniquely
	// identify the inventory account that contains the product.
	InventoryAccountId int64 `json:"inventoryAccountId,omitempty,string"`

	// ProductCountry: The country registered for the Merchant Center feed
	// that contains the product. Use an ISO 3166 code to specify a country.
	ProductCountry string `json:"productCountry,omitempty"`

	// ProductGroupId: DS product group ID.
	ProductGroupId int64 `json:"productGroupId,omitempty,string"`

	// ProductId: The product ID (SKU).
	ProductId string `json:"productId,omitempty"`

	// ProductLanguage: The language registered for the Merchant Center feed
	// that contains the product. Use an ISO 639 code to specify a language.
	ProductLanguage string `json:"productLanguage,omitempty"`

	// QuantityMillis: The quantity of this conversion, in millis.
	QuantityMillis int64 `json:"quantityMillis,omitempty,string"`

	// RevenueMicros: The revenue amount of this TRANSACTION conversion, in
	// micros (value multiplied by 1000000, no decimal). For example, to
	// specify a revenue value of "10" enter "10000000" (10 million) in your
	// request.
	RevenueMicros int64 `json:"revenueMicros,omitempty,string"`

	// SegmentationId: The numeric segmentation identifier (for example,
	// DoubleClick Search Floodlight activity ID).
	SegmentationId int64 `json:"segmentationId,omitempty,string"`

	// SegmentationName: The friendly segmentation identifier (for example,
	// DoubleClick Search Floodlight activity name).
	SegmentationName string `json:"segmentationName,omitempty"`

	// SegmentationType: The segmentation type of this conversion (for
	// example, FLOODLIGHT).
	SegmentationType string `json:"segmentationType,omitempty"`

	// State: The state of the conversion, that is, either ACTIVE or
	// REMOVED. Note: state DELETED is deprecated.
	State string `json:"state,omitempty"`

	// StoreId: The ID of the local store for which the product was
	// advertised. Applicable only when the channel is "local".
	StoreId string `json:"storeId,omitempty"`

	// Type: The type of the conversion, that is, either ACTION or
	// TRANSACTION. An ACTION conversion is an action by the user that has
	// no monetarily quantifiable value, while a TRANSACTION conversion is
	// an action that does have a monetarily quantifiable value. Examples
	// are email list signups (ACTION) versus ecommerce purchases
	// (TRANSACTION).
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AdGroupId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdGroupId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Conversion) MarshalJSON() ([]byte, error) {
	type noMethod Conversion
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ConversionList: A list of conversions.
type ConversionList struct {
	// Conversion: The conversions being requested.
	Conversion []*Conversion `json:"conversion,omitempty"`

	// Kind: Identifies this as a ConversionList resource. Value: the fixed
	// string doubleclicksearch#conversionList.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Conversion") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Conversion") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ConversionList) MarshalJSON() ([]byte, error) {
	type noMethod ConversionList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CustomDimension: A message containing the custome dimension.
type CustomDimension struct {
	// Name: Custom dimension name.
	Name string `json:"name,omitempty"`

	// Value: Custom dimension value.
	Value string `json:"value,omitempty"`

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

func (s *CustomDimension) MarshalJSON() ([]byte, error) {
	type noMethod CustomDimension
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CustomMetric: A message containing the custome metric.
type CustomMetric struct {
	// Name: Custom metric name.
	Name string `json:"name,omitempty"`

	// Value: Custom metric numeric value.
	Value float64 `json:"value,omitempty"`

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

func (s *CustomMetric) MarshalJSON() ([]byte, error) {
	type noMethod CustomMetric
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Report: A DoubleClick Search report. This object contains the report
// request, some report metadata such as currency code, and the
// generated report rows or report files.
type Report struct {
	// Files: Asynchronous report only. Contains a list of generated report
	// files once the report has succesfully completed.
	Files []*ReportFiles `json:"files,omitempty"`

	// Id: Asynchronous report only. Id of the report.
	Id string `json:"id,omitempty"`

	// IsReportReady: Asynchronous report only. True if and only if the
	// report has completed successfully and the report files are ready to
	// be downloaded.
	IsReportReady bool `json:"isReportReady,omitempty"`

	// Kind: Identifies this as a Report resource. Value: the fixed string
	// doubleclicksearch#report.
	Kind string `json:"kind,omitempty"`

	// Request: The request that created the report. Optional fields not
	// specified in the original request are filled with default values.
	Request *ReportRequest `json:"request,omitempty"`

	// RowCount: The number of report rows generated by the report, not
	// including headers.
	RowCount int64 `json:"rowCount,omitempty"`

	// Rows: Synchronous report only. Generated report rows.
	Rows []googleapi.RawMessage `json:"rows,omitempty"`

	// StatisticsCurrencyCode: The currency code of all monetary values
	// produced in the report, including values that are set by users (e.g.,
	// keyword bid settings) and metrics (e.g., cost and revenue). The
	// currency code of a report is determined by the statisticsCurrency
	// field of the report request.
	StatisticsCurrencyCode string `json:"statisticsCurrencyCode,omitempty"`

	// StatisticsTimeZone: If all statistics of the report are sourced from
	// the same time zone, this would be it. Otherwise the field is unset.
	StatisticsTimeZone string `json:"statisticsTimeZone,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Files") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Files") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Report) MarshalJSON() ([]byte, error) {
	type noMethod Report
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ReportFiles struct {
	// ByteCount: The size of this report file in bytes.
	ByteCount int64 `json:"byteCount,omitempty,string"`

	// Url: Use this url to download the report file.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ByteCount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ByteCount") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportFiles) MarshalJSON() ([]byte, error) {
	type noMethod ReportFiles
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportApiColumnSpec: A request object used to create a DoubleClick
// Search report.
type ReportApiColumnSpec struct {
	// ColumnName: Name of a DoubleClick Search column to include in the
	// report.
	ColumnName string `json:"columnName,omitempty"`

	// CustomDimensionName: Segments a report by a custom dimension. The
	// report must be scoped to an advertiser or lower, and the custom
	// dimension must already be set up in DoubleClick Search. The custom
	// dimension name, which appears in DoubleClick Search, is case
	// sensitive.
	// If used in a conversion report, returns the value of the specified
	// custom dimension for the given conversion, if set. This column does
	// not segment the conversion report.
	CustomDimensionName string `json:"customDimensionName,omitempty"`

	// CustomMetricName: Name of a custom metric to include in the report.
	// The report must be scoped to an advertiser or lower, and the custom
	// metric must already be set up in DoubleClick Search. The custom
	// metric name, which appears in DoubleClick Search, is case sensitive.
	CustomMetricName string `json:"customMetricName,omitempty"`

	// EndDate: Inclusive day in YYYY-MM-DD format. When provided, this
	// overrides the overall time range of the report for this column only.
	// Must be provided together with startDate.
	EndDate string `json:"endDate,omitempty"`

	// GroupByColumn: Synchronous report only. Set to true to group by this
	// column. Defaults to false.
	GroupByColumn bool `json:"groupByColumn,omitempty"`

	// HeaderText: Text used to identify this column in the report output;
	// defaults to columnName or savedColumnName when not specified. This
	// can be used to prevent collisions between DoubleClick Search columns
	// and saved columns with the same name.
	HeaderText string `json:"headerText,omitempty"`

	// PlatformSource: The platform that is used to provide data for the
	// custom dimension. Acceptable values are "floodlight".
	PlatformSource string `json:"platformSource,omitempty"`

	// ProductReportPerspective: Returns metrics only for a specific type of
	// product activity. Accepted values are:
	// - "sold": returns metrics only for products that were sold
	// - "advertised": returns metrics only for products that were
	// advertised in a Shopping campaign, and that might or might not have
	// been sold
	ProductReportPerspective string `json:"productReportPerspective,omitempty"`

	// SavedColumnName: Name of a saved column to include in the report. The
	// report must be scoped at advertiser or lower, and this saved column
	// must already be created in the DoubleClick Search UI.
	SavedColumnName string `json:"savedColumnName,omitempty"`

	// StartDate: Inclusive date in YYYY-MM-DD format. When provided, this
	// overrides the overall time range of the report for this column only.
	// Must be provided together with endDate.
	StartDate string `json:"startDate,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColumnName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColumnName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportApiColumnSpec) MarshalJSON() ([]byte, error) {
	type noMethod ReportApiColumnSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportRequest: A request object used to create a DoubleClick Search
// report.
type ReportRequest struct {
	// Columns: The columns to include in the report. This includes both
	// DoubleClick Search columns and saved columns. For DoubleClick Search
	// columns, only the columnName parameter is required. For saved columns
	// only the savedColumnName parameter is required. Both columnName and
	// savedColumnName cannot be set in the same stanza.
	Columns []*ReportApiColumnSpec `json:"columns,omitempty"`

	// DownloadFormat: Format that the report should be returned in.
	// Currently csv or tsv is supported.
	DownloadFormat string `json:"downloadFormat,omitempty"`

	// Filters: A list of filters to be applied to the report.
	Filters []*ReportRequestFilters `json:"filters,omitempty"`

	// IncludeDeletedEntities: Determines if removed entities should be
	// included in the report. Defaults to false. Deprecated, please use
	// includeRemovedEntities instead.
	IncludeDeletedEntities bool `json:"includeDeletedEntities,omitempty"`

	// IncludeRemovedEntities: Determines if removed entities should be
	// included in the report. Defaults to false.
	IncludeRemovedEntities bool `json:"includeRemovedEntities,omitempty"`

	// MaxRowsPerFile: Asynchronous report only. The maximum number of rows
	// per report file. A large report is split into many files based on
	// this field. Acceptable values are 1000000 to 100000000, inclusive.
	MaxRowsPerFile int64 `json:"maxRowsPerFile,omitempty"`

	// OrderBy: Synchronous report only. A list of columns and directions
	// defining sorting to be performed on the report rows.
	OrderBy []*ReportRequestOrderBy `json:"orderBy,omitempty"`

	// ReportScope: The reportScope is a set of IDs that are used to
	// determine which subset of entities will be returned in the report.
	// The full lineage of IDs from the lowest scoped level desired up
	// through agency is required.
	ReportScope *ReportRequestReportScope `json:"reportScope,omitempty"`

	// ReportType: Determines the type of rows that are returned in the
	// report. For example, if you specify reportType: keyword, each row in
	// the report will contain data about a keyword. See the Types of
	// Reports reference for the columns that are available for each type.
	ReportType string `json:"reportType,omitempty"`

	// RowCount: Synchronous report only. The maxinum number of rows to
	// return; additional rows are dropped. Acceptable values are 0 to
	// 10000, inclusive. Defaults to 10000.
	//
	// Default: 10000
	RowCount *int64 `json:"rowCount,omitempty"`

	// StartRow: Synchronous report only. Zero-based index of the first row
	// to return. Acceptable values are 0 to 50000, inclusive. Defaults to
	// 0.
	StartRow int64 `json:"startRow,omitempty"`

	// StatisticsCurrency: Specifies the currency in which monetary will be
	// returned. Possible values are: usd, agency (valid if the report is
	// scoped to agency or lower), advertiser (valid if the report is scoped
	// to * advertiser or lower), or account (valid if the report is scoped
	// to engine account or lower).
	StatisticsCurrency string `json:"statisticsCurrency,omitempty"`

	// TimeRange: If metrics are requested in a report, this argument will
	// be used to restrict the metrics to a specific time range.
	TimeRange *ReportRequestTimeRange `json:"timeRange,omitempty"`

	// VerifySingleTimeZone: If true, the report would only be created if
	// all the requested stat data are sourced from a single timezone.
	// Defaults to false.
	VerifySingleTimeZone bool `json:"verifySingleTimeZone,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Columns") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Columns") to include in
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

type ReportRequestFilters struct {
	// Column: Column to perform the filter on. This can be a DoubleClick
	// Search column or a saved column.
	Column *ReportApiColumnSpec `json:"column,omitempty"`

	// Operator: Operator to use in the filter. See the filter reference for
	// a list of available operators.
	Operator string `json:"operator,omitempty"`

	// Values: A list of values to filter the column value against.
	Values []interface{} `json:"values,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Column") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Column") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportRequestFilters) MarshalJSON() ([]byte, error) {
	type noMethod ReportRequestFilters
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ReportRequestOrderBy struct {
	// Column: Column to perform the sort on. This can be a DoubleClick
	// Search-defined column or a saved column.
	Column *ReportApiColumnSpec `json:"column,omitempty"`

	// SortOrder: The sort direction, which is either ascending or
	// descending.
	SortOrder string `json:"sortOrder,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Column") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Column") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportRequestOrderBy) MarshalJSON() ([]byte, error) {
	type noMethod ReportRequestOrderBy
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportRequestReportScope: The reportScope is a set of IDs that are
// used to determine which subset of entities will be returned in the
// report. The full lineage of IDs from the lowest scoped level desired
// up through agency is required.
type ReportRequestReportScope struct {
	// AdGroupId: DS ad group ID.
	AdGroupId int64 `json:"adGroupId,omitempty,string"`

	// AdId: DS ad ID.
	AdId int64 `json:"adId,omitempty,string"`

	// AdvertiserId: DS advertiser ID.
	AdvertiserId int64 `json:"advertiserId,omitempty,string"`

	// AgencyId: DS agency ID.
	AgencyId int64 `json:"agencyId,omitempty,string"`

	// CampaignId: DS campaign ID.
	CampaignId int64 `json:"campaignId,omitempty,string"`

	// EngineAccountId: DS engine account ID.
	EngineAccountId int64 `json:"engineAccountId,omitempty,string"`

	// KeywordId: DS keyword ID.
	KeywordId int64 `json:"keywordId,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "AdGroupId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdGroupId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportRequestReportScope) MarshalJSON() ([]byte, error) {
	type noMethod ReportRequestReportScope
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReportRequestTimeRange: If metrics are requested in a report, this
// argument will be used to restrict the metrics to a specific time
// range.
type ReportRequestTimeRange struct {
	// ChangedAttributesSinceTimestamp: Inclusive UTC timestamp in RFC
	// format, e.g., 2013-07-16T10:16:23.555Z. See additional references on
	// how changed attribute reports work.
	ChangedAttributesSinceTimestamp string `json:"changedAttributesSinceTimestamp,omitempty"`

	// ChangedMetricsSinceTimestamp: Inclusive UTC timestamp in RFC format,
	// e.g., 2013-07-16T10:16:23.555Z. See additional references on how
	// changed metrics reports work.
	ChangedMetricsSinceTimestamp string `json:"changedMetricsSinceTimestamp,omitempty"`

	// EndDate: Inclusive date in YYYY-MM-DD format.
	EndDate string `json:"endDate,omitempty"`

	// StartDate: Inclusive date in YYYY-MM-DD format.
	StartDate string `json:"startDate,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "ChangedAttributesSinceTimestamp") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "ChangedAttributesSinceTimestamp") to include in API requests with
	// the JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReportRequestTimeRange) MarshalJSON() ([]byte, error) {
	type noMethod ReportRequestTimeRange
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SavedColumn: A saved column
type SavedColumn struct {
	// Kind: Identifies this as a SavedColumn resource. Value: the fixed
	// string doubleclicksearch#savedColumn.
	Kind string `json:"kind,omitempty"`

	// SavedColumnName: The name of the saved column.
	SavedColumnName string `json:"savedColumnName,omitempty"`

	// Type: The type of data this saved column will produce.
	Type string `json:"type,omitempty"`

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

func (s *SavedColumn) MarshalJSON() ([]byte, error) {
	type noMethod SavedColumn
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SavedColumnList: A list of saved columns. Advertisers create saved
// columns to report on Floodlight activities, Google Analytics goals,
// or custom KPIs. To request reports with saved columns, you'll need
// the saved column names that are available from this list.
type SavedColumnList struct {
	// Items: The saved columns being requested.
	Items []*SavedColumn `json:"items,omitempty"`

	// Kind: Identifies this as a SavedColumnList resource. Value: the fixed
	// string doubleclicksearch#savedColumnList.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SavedColumnList) MarshalJSON() ([]byte, error) {
	type noMethod SavedColumnList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateAvailabilityRequest: The request to update availability.
type UpdateAvailabilityRequest struct {
	// Availabilities: The availabilities being requested.
	Availabilities []*Availability `json:"availabilities,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Availabilities") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Availabilities") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *UpdateAvailabilityRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateAvailabilityRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateAvailabilityResponse: The response to a update availability
// request.
type UpdateAvailabilityResponse struct {
	// Availabilities: The availabilities being returned.
	Availabilities []*Availability `json:"availabilities,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Availabilities") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Availabilities") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *UpdateAvailabilityResponse) MarshalJSON() ([]byte, error) {
	type noMethod UpdateAvailabilityResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "doubleclicksearch.conversion.get":

type ConversionGetCall struct {
	s               *Service
	agencyId        int64
	advertiserId    int64
	engineAccountId int64
	urlParams_      gensupport.URLParams
	ifNoneMatch_    string
	ctx_            context.Context
	header_         http.Header
}

// Get: Retrieves a list of conversions from a DoubleClick Search engine
// account.
func (r *ConversionService) Get(agencyId int64, advertiserId int64, engineAccountId int64, endDate int64, rowCount int64, startDate int64, startRow int64) *ConversionGetCall {
	c := &ConversionGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.agencyId = agencyId
	c.advertiserId = advertiserId
	c.engineAccountId = engineAccountId
	c.urlParams_.Set("endDate", fmt.Sprint(endDate))
	c.urlParams_.Set("rowCount", fmt.Sprint(rowCount))
	c.urlParams_.Set("startDate", fmt.Sprint(startDate))
	c.urlParams_.Set("startRow", fmt.Sprint(startRow))
	return c
}

// AdGroupId sets the optional parameter "adGroupId": Numeric ID of the
// ad group.
func (c *ConversionGetCall) AdGroupId(adGroupId int64) *ConversionGetCall {
	c.urlParams_.Set("adGroupId", fmt.Sprint(adGroupId))
	return c
}

// AdId sets the optional parameter "adId": Numeric ID of the ad.
func (c *ConversionGetCall) AdId(adId int64) *ConversionGetCall {
	c.urlParams_.Set("adId", fmt.Sprint(adId))
	return c
}

// CampaignId sets the optional parameter "campaignId": Numeric ID of
// the campaign.
func (c *ConversionGetCall) CampaignId(campaignId int64) *ConversionGetCall {
	c.urlParams_.Set("campaignId", fmt.Sprint(campaignId))
	return c
}

// CriterionId sets the optional parameter "criterionId": Numeric ID of
// the criterion.
func (c *ConversionGetCall) CriterionId(criterionId int64) *ConversionGetCall {
	c.urlParams_.Set("criterionId", fmt.Sprint(criterionId))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ConversionGetCall) Fields(s ...googleapi.Field) *ConversionGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ConversionGetCall) IfNoneMatch(entityTag string) *ConversionGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ConversionGetCall) Context(ctx context.Context) *ConversionGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ConversionGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ConversionGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "agency/{agencyId}/advertiser/{advertiserId}/engine/{engineAccountId}/conversion")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"agencyId":        strconv.FormatInt(c.agencyId, 10),
		"advertiserId":    strconv.FormatInt(c.advertiserId, 10),
		"engineAccountId": strconv.FormatInt(c.engineAccountId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.conversion.get" call.
// Exactly one of *ConversionList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ConversionList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ConversionGetCall) Do(opts ...googleapi.CallOption) (*ConversionList, error) {
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
	ret := &ConversionList{
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
	//   "description": "Retrieves a list of conversions from a DoubleClick Search engine account.",
	//   "httpMethod": "GET",
	//   "id": "doubleclicksearch.conversion.get",
	//   "parameterOrder": [
	//     "agencyId",
	//     "advertiserId",
	//     "engineAccountId",
	//     "endDate",
	//     "rowCount",
	//     "startDate",
	//     "startRow"
	//   ],
	//   "parameters": {
	//     "adGroupId": {
	//       "description": "Numeric ID of the ad group.",
	//       "format": "int64",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "adId": {
	//       "description": "Numeric ID of the ad.",
	//       "format": "int64",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "advertiserId": {
	//       "description": "Numeric ID of the advertiser.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "agencyId": {
	//       "description": "Numeric ID of the agency.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "campaignId": {
	//       "description": "Numeric ID of the campaign.",
	//       "format": "int64",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "criterionId": {
	//       "description": "Numeric ID of the criterion.",
	//       "format": "int64",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "endDate": {
	//       "description": "Last date (inclusive) on which to retrieve conversions. Format is yyyymmdd.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "99991231",
	//       "minimum": "20091101",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "engineAccountId": {
	//       "description": "Numeric ID of the engine account.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "rowCount": {
	//       "description": "The number of conversions to return per call.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "1000",
	//       "minimum": "1",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "startDate": {
	//       "description": "First date (inclusive) on which to retrieve conversions. Format is yyyymmdd.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "99991231",
	//       "minimum": "20091101",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "startRow": {
	//       "description": "The 0-based starting index for retrieving conversions results.",
	//       "format": "uint32",
	//       "location": "query",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "agency/{agencyId}/advertiser/{advertiserId}/engine/{engineAccountId}/conversion",
	//   "response": {
	//     "$ref": "ConversionList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.conversion.insert":

type ConversionInsertCall struct {
	s              *Service
	conversionlist *ConversionList
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Insert: Inserts a batch of new conversions into DoubleClick Search.
func (r *ConversionService) Insert(conversionlist *ConversionList) *ConversionInsertCall {
	c := &ConversionInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.conversionlist = conversionlist
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ConversionInsertCall) Fields(s ...googleapi.Field) *ConversionInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ConversionInsertCall) Context(ctx context.Context) *ConversionInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ConversionInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ConversionInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.conversionlist)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "conversion")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.conversion.insert" call.
// Exactly one of *ConversionList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ConversionList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ConversionInsertCall) Do(opts ...googleapi.CallOption) (*ConversionList, error) {
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
	ret := &ConversionList{
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
	//   "description": "Inserts a batch of new conversions into DoubleClick Search.",
	//   "httpMethod": "POST",
	//   "id": "doubleclicksearch.conversion.insert",
	//   "path": "conversion",
	//   "request": {
	//     "$ref": "ConversionList"
	//   },
	//   "response": {
	//     "$ref": "ConversionList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.conversion.patch":

type ConversionPatchCall struct {
	s              *Service
	conversionlist *ConversionList
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Patch: Updates a batch of conversions in DoubleClick Search. This
// method supports patch semantics.
func (r *ConversionService) Patch(advertiserId int64, agencyId int64, endDate int64, engineAccountId int64, rowCount int64, startDate int64, startRow int64, conversionlist *ConversionList) *ConversionPatchCall {
	c := &ConversionPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("advertiserId", fmt.Sprint(advertiserId))
	c.urlParams_.Set("agencyId", fmt.Sprint(agencyId))
	c.urlParams_.Set("endDate", fmt.Sprint(endDate))
	c.urlParams_.Set("engineAccountId", fmt.Sprint(engineAccountId))
	c.urlParams_.Set("rowCount", fmt.Sprint(rowCount))
	c.urlParams_.Set("startDate", fmt.Sprint(startDate))
	c.urlParams_.Set("startRow", fmt.Sprint(startRow))
	c.conversionlist = conversionlist
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ConversionPatchCall) Fields(s ...googleapi.Field) *ConversionPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ConversionPatchCall) Context(ctx context.Context) *ConversionPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ConversionPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ConversionPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.conversionlist)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "conversion")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.conversion.patch" call.
// Exactly one of *ConversionList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ConversionList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ConversionPatchCall) Do(opts ...googleapi.CallOption) (*ConversionList, error) {
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
	ret := &ConversionList{
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
	//   "description": "Updates a batch of conversions in DoubleClick Search. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "doubleclicksearch.conversion.patch",
	//   "parameterOrder": [
	//     "advertiserId",
	//     "agencyId",
	//     "endDate",
	//     "engineAccountId",
	//     "rowCount",
	//     "startDate",
	//     "startRow"
	//   ],
	//   "parameters": {
	//     "advertiserId": {
	//       "description": "Numeric ID of the advertiser.",
	//       "format": "int64",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "agencyId": {
	//       "description": "Numeric ID of the agency.",
	//       "format": "int64",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "endDate": {
	//       "description": "Last date (inclusive) on which to retrieve conversions. Format is yyyymmdd.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "99991231",
	//       "minimum": "20091101",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "engineAccountId": {
	//       "description": "Numeric ID of the engine account.",
	//       "format": "int64",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "rowCount": {
	//       "description": "The number of conversions to return per call.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "1000",
	//       "minimum": "1",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "startDate": {
	//       "description": "First date (inclusive) on which to retrieve conversions. Format is yyyymmdd.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "99991231",
	//       "minimum": "20091101",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "startRow": {
	//       "description": "The 0-based starting index for retrieving conversions results.",
	//       "format": "uint32",
	//       "location": "query",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "conversion",
	//   "request": {
	//     "$ref": "ConversionList"
	//   },
	//   "response": {
	//     "$ref": "ConversionList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.conversion.update":

type ConversionUpdateCall struct {
	s              *Service
	conversionlist *ConversionList
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Update: Updates a batch of conversions in DoubleClick Search.
func (r *ConversionService) Update(conversionlist *ConversionList) *ConversionUpdateCall {
	c := &ConversionUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.conversionlist = conversionlist
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ConversionUpdateCall) Fields(s ...googleapi.Field) *ConversionUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ConversionUpdateCall) Context(ctx context.Context) *ConversionUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ConversionUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ConversionUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.conversionlist)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "conversion")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.conversion.update" call.
// Exactly one of *ConversionList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ConversionList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ConversionUpdateCall) Do(opts ...googleapi.CallOption) (*ConversionList, error) {
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
	ret := &ConversionList{
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
	//   "description": "Updates a batch of conversions in DoubleClick Search.",
	//   "httpMethod": "PUT",
	//   "id": "doubleclicksearch.conversion.update",
	//   "path": "conversion",
	//   "request": {
	//     "$ref": "ConversionList"
	//   },
	//   "response": {
	//     "$ref": "ConversionList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.conversion.updateAvailability":

type ConversionUpdateAvailabilityCall struct {
	s                         *Service
	updateavailabilityrequest *UpdateAvailabilityRequest
	urlParams_                gensupport.URLParams
	ctx_                      context.Context
	header_                   http.Header
}

// UpdateAvailability: Updates the availabilities of a batch of
// floodlight activities in DoubleClick Search.
func (r *ConversionService) UpdateAvailability(updateavailabilityrequest *UpdateAvailabilityRequest) *ConversionUpdateAvailabilityCall {
	c := &ConversionUpdateAvailabilityCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.updateavailabilityrequest = updateavailabilityrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ConversionUpdateAvailabilityCall) Fields(s ...googleapi.Field) *ConversionUpdateAvailabilityCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ConversionUpdateAvailabilityCall) Context(ctx context.Context) *ConversionUpdateAvailabilityCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ConversionUpdateAvailabilityCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ConversionUpdateAvailabilityCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.updateavailabilityrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "conversion/updateAvailability")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.conversion.updateAvailability" call.
// Exactly one of *UpdateAvailabilityResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *UpdateAvailabilityResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ConversionUpdateAvailabilityCall) Do(opts ...googleapi.CallOption) (*UpdateAvailabilityResponse, error) {
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
	ret := &UpdateAvailabilityResponse{
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
	//   "description": "Updates the availabilities of a batch of floodlight activities in DoubleClick Search.",
	//   "httpMethod": "POST",
	//   "id": "doubleclicksearch.conversion.updateAvailability",
	//   "path": "conversion/updateAvailability",
	//   "request": {
	//     "$ref": "UpdateAvailabilityRequest",
	//     "parameterName": "empty"
	//   },
	//   "response": {
	//     "$ref": "UpdateAvailabilityResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.reports.generate":

type ReportsGenerateCall struct {
	s             *Service
	reportrequest *ReportRequest
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// Generate: Generates and returns a report immediately.
func (r *ReportsService) Generate(reportrequest *ReportRequest) *ReportsGenerateCall {
	c := &ReportsGenerateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.reportrequest = reportrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReportsGenerateCall) Fields(s ...googleapi.Field) *ReportsGenerateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReportsGenerateCall) Context(ctx context.Context) *ReportsGenerateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReportsGenerateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReportsGenerateCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "reports/generate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.reports.generate" call.
// Exactly one of *Report or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Report.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ReportsGenerateCall) Do(opts ...googleapi.CallOption) (*Report, error) {
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
	ret := &Report{
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
	//   "description": "Generates and returns a report immediately.",
	//   "httpMethod": "POST",
	//   "id": "doubleclicksearch.reports.generate",
	//   "path": "reports/generate",
	//   "request": {
	//     "$ref": "ReportRequest",
	//     "parameterName": "reportRequest"
	//   },
	//   "response": {
	//     "$ref": "Report"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.reports.get":

type ReportsGetCall struct {
	s            *Service
	reportId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Polls for the status of a report request.
func (r *ReportsService) Get(reportId string) *ReportsGetCall {
	c := &ReportsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.reportId = reportId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReportsGetCall) Fields(s ...googleapi.Field) *ReportsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ReportsGetCall) IfNoneMatch(entityTag string) *ReportsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReportsGetCall) Context(ctx context.Context) *ReportsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReportsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReportsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "reports/{reportId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"reportId": c.reportId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.reports.get" call.
// Exactly one of *Report or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Report.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ReportsGetCall) Do(opts ...googleapi.CallOption) (*Report, error) {
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
	ret := &Report{
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
	//   "description": "Polls for the status of a report request.",
	//   "httpMethod": "GET",
	//   "id": "doubleclicksearch.reports.get",
	//   "parameterOrder": [
	//     "reportId"
	//   ],
	//   "parameters": {
	//     "reportId": {
	//       "description": "ID of the report request being polled.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "reports/{reportId}",
	//   "response": {
	//     "$ref": "Report"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.reports.getFile":

type ReportsGetFileCall struct {
	s              *Service
	reportId       string
	reportFragment int64
	urlParams_     gensupport.URLParams
	ifNoneMatch_   string
	ctx_           context.Context
	header_        http.Header
}

// GetFile: Downloads a report file encoded in UTF-8.
func (r *ReportsService) GetFile(reportId string, reportFragment int64) *ReportsGetFileCall {
	c := &ReportsGetFileCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.reportId = reportId
	c.reportFragment = reportFragment
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReportsGetFileCall) Fields(s ...googleapi.Field) *ReportsGetFileCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ReportsGetFileCall) IfNoneMatch(entityTag string) *ReportsGetFileCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do and Download
// methods. Any pending HTTP request will be aborted if the provided
// context is canceled.
func (c *ReportsGetFileCall) Context(ctx context.Context) *ReportsGetFileCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReportsGetFileCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReportsGetFileCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "reports/{reportId}/files/{reportFragment}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"reportId":       c.reportId,
		"reportFragment": strconv.FormatInt(c.reportFragment, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Download fetches the API endpoint's "media" value, instead of the normal
// API response value. If the returned error is nil, the Response is guaranteed to
// have a 2xx status code. Callers must close the Response.Body as usual.
func (c *ReportsGetFileCall) Download(opts ...googleapi.CallOption) (*http.Response, error) {
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

// Do executes the "doubleclicksearch.reports.getFile" call.
func (c *ReportsGetFileCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Downloads a report file encoded in UTF-8.",
	//   "httpMethod": "GET",
	//   "id": "doubleclicksearch.reports.getFile",
	//   "parameterOrder": [
	//     "reportId",
	//     "reportFragment"
	//   ],
	//   "parameters": {
	//     "reportFragment": {
	//       "description": "The index of the report fragment to download.",
	//       "format": "int32",
	//       "location": "path",
	//       "minimum": "0",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "reportId": {
	//       "description": "ID of the report.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "reports/{reportId}/files/{reportFragment}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ],
	//   "supportsMediaDownload": true,
	//   "useMediaDownloadService": true
	// }

}

// method id "doubleclicksearch.reports.request":

type ReportsRequestCall struct {
	s             *Service
	reportrequest *ReportRequest
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// Request: Inserts a report request into the reporting system.
func (r *ReportsService) Request(reportrequest *ReportRequest) *ReportsRequestCall {
	c := &ReportsRequestCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.reportrequest = reportrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ReportsRequestCall) Fields(s ...googleapi.Field) *ReportsRequestCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ReportsRequestCall) Context(ctx context.Context) *ReportsRequestCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ReportsRequestCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ReportsRequestCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "reports")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.reports.request" call.
// Exactly one of *Report or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Report.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ReportsRequestCall) Do(opts ...googleapi.CallOption) (*Report, error) {
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
	ret := &Report{
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
	//   "description": "Inserts a report request into the reporting system.",
	//   "httpMethod": "POST",
	//   "id": "doubleclicksearch.reports.request",
	//   "path": "reports",
	//   "request": {
	//     "$ref": "ReportRequest",
	//     "parameterName": "reportRequest"
	//   },
	//   "response": {
	//     "$ref": "Report"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

// method id "doubleclicksearch.savedColumns.list":

type SavedColumnsListCall struct {
	s            *Service
	agencyId     int64
	advertiserId int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve the list of saved columns for a specified advertiser.
func (r *SavedColumnsService) List(agencyId int64, advertiserId int64) *SavedColumnsListCall {
	c := &SavedColumnsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.agencyId = agencyId
	c.advertiserId = advertiserId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SavedColumnsListCall) Fields(s ...googleapi.Field) *SavedColumnsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SavedColumnsListCall) IfNoneMatch(entityTag string) *SavedColumnsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SavedColumnsListCall) Context(ctx context.Context) *SavedColumnsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SavedColumnsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SavedColumnsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "agency/{agencyId}/advertiser/{advertiserId}/savedcolumns")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"agencyId":     strconv.FormatInt(c.agencyId, 10),
		"advertiserId": strconv.FormatInt(c.advertiserId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "doubleclicksearch.savedColumns.list" call.
// Exactly one of *SavedColumnList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *SavedColumnList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SavedColumnsListCall) Do(opts ...googleapi.CallOption) (*SavedColumnList, error) {
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
	ret := &SavedColumnList{
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
	//   "description": "Retrieve the list of saved columns for a specified advertiser.",
	//   "httpMethod": "GET",
	//   "id": "doubleclicksearch.savedColumns.list",
	//   "parameterOrder": [
	//     "agencyId",
	//     "advertiserId"
	//   ],
	//   "parameters": {
	//     "advertiserId": {
	//       "description": "DS ID of the advertiser.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "agencyId": {
	//       "description": "DS ID of the agency.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "agency/{agencyId}/advertiser/{advertiserId}/savedcolumns",
	//   "response": {
	//     "$ref": "SavedColumnList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/doubleclicksearch"
	//   ]
	// }

}

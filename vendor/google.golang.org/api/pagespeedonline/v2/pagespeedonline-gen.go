// Package pagespeedonline provides access to the PageSpeed Insights API.
//
// See https://developers.google.com/speed/docs/insights/v2/getting-started
//
// Usage example:
//
//   import "google.golang.org/api/pagespeedonline/v2"
//   ...
//   pagespeedonlineService, err := pagespeedonline.New(oauthHttpClient)
package pagespeedonline // import "google.golang.org/api/pagespeedonline/v2"

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

const apiId = "pagespeedonline:v2"
const apiName = "pagespeedonline"
const apiVersion = "v2"
const basePath = "https://www.googleapis.com/pagespeedonline/v2/"

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Pagespeedapi = NewPagespeedapiService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Pagespeedapi *PagespeedapiService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewPagespeedapiService(s *Service) *PagespeedapiService {
	rs := &PagespeedapiService{s: s}
	return rs
}

type PagespeedapiService struct {
	s *Service
}

type PagespeedApiFormatStringV2 struct {
	// Args: List of arguments for the format string.
	Args []*PagespeedApiFormatStringV2Args `json:"args,omitempty"`

	// Format: A localized format string with {{FOO}} placeholders, where
	// 'FOO' is the key of the argument whose value should be substituted.
	// For HYPERLINK arguments, the format string will instead contain
	// {{BEGIN_FOO}} and {{END_FOO}} for the argument with key 'FOO'.
	Format string `json:"format,omitempty"`

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

func (s *PagespeedApiFormatStringV2) MarshalJSON() ([]byte, error) {
	type noMethod PagespeedApiFormatStringV2
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PagespeedApiFormatStringV2Args struct {
	// Key: The placeholder key for this arg, as a string.
	Key string `json:"key,omitempty"`

	// Rects: The screen rectangles being referred to, with dimensions
	// measured in CSS pixels. This is only ever used for SNAPSHOT_RECT
	// arguments. If this is absent for a SNAPSHOT_RECT argument, it means
	// that that argument refers to the entire snapshot.
	Rects []*PagespeedApiFormatStringV2ArgsRects `json:"rects,omitempty"`

	// SecondaryRects: Secondary screen rectangles being referred to, with
	// dimensions measured in CSS pixels. This is only ever used for
	// SNAPSHOT_RECT arguments.
	SecondaryRects []*PagespeedApiFormatStringV2ArgsSecondaryRects `json:"secondary_rects,omitempty"`

	// Type: Type of argument. One of URL, STRING_LITERAL, INT_LITERAL,
	// BYTES, DURATION, VERBATIM_STRING, PERCENTAGE, HYPERLINK, or
	// SNAPSHOT_RECT.
	Type string `json:"type,omitempty"`

	// Value: Argument value, as a localized string.
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

func (s *PagespeedApiFormatStringV2Args) MarshalJSON() ([]byte, error) {
	type noMethod PagespeedApiFormatStringV2Args
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PagespeedApiFormatStringV2ArgsRects struct {
	// Height: The height of the rect.
	Height int64 `json:"height,omitempty"`

	// Left: The left coordinate of the rect, in page coordinates.
	Left int64 `json:"left,omitempty"`

	// Top: The top coordinate of the rect, in page coordinates.
	Top int64 `json:"top,omitempty"`

	// Width: The width of the rect.
	Width int64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Height") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Height") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PagespeedApiFormatStringV2ArgsRects) MarshalJSON() ([]byte, error) {
	type noMethod PagespeedApiFormatStringV2ArgsRects
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PagespeedApiFormatStringV2ArgsSecondaryRects struct {
	// Height: The height of the rect.
	Height int64 `json:"height,omitempty"`

	// Left: The left coordinate of the rect, in page coordinates.
	Left int64 `json:"left,omitempty"`

	// Top: The top coordinate of the rect, in page coordinates.
	Top int64 `json:"top,omitempty"`

	// Width: The width of the rect.
	Width int64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Height") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Height") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PagespeedApiFormatStringV2ArgsSecondaryRects) MarshalJSON() ([]byte, error) {
	type noMethod PagespeedApiFormatStringV2ArgsSecondaryRects
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PagespeedApiImageV2 struct {
	// Data: Image data base64 encoded.
	Data string `json:"data,omitempty"`

	// Height: Height of screenshot in pixels.
	Height int64 `json:"height,omitempty"`

	// Key: Unique string key, if any, identifying this image.
	Key string `json:"key,omitempty"`

	// MimeType: Mime type of image data (e.g. "image/jpeg").
	MimeType string `json:"mime_type,omitempty"`

	// PageRect: The region of the page that is captured by this image, with
	// dimensions measured in CSS pixels.
	PageRect *PagespeedApiImageV2PageRect `json:"page_rect,omitempty"`

	// Width: Width of screenshot in pixels.
	Width int64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Data") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Data") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PagespeedApiImageV2) MarshalJSON() ([]byte, error) {
	type noMethod PagespeedApiImageV2
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PagespeedApiImageV2PageRect: The region of the page that is captured
// by this image, with dimensions measured in CSS pixels.
type PagespeedApiImageV2PageRect struct {
	// Height: The height of the rect.
	Height int64 `json:"height,omitempty"`

	// Left: The left coordinate of the rect, in page coordinates.
	Left int64 `json:"left,omitempty"`

	// Top: The top coordinate of the rect, in page coordinates.
	Top int64 `json:"top,omitempty"`

	// Width: The width of the rect.
	Width int64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Height") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Height") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PagespeedApiImageV2PageRect) MarshalJSON() ([]byte, error) {
	type noMethod PagespeedApiImageV2PageRect
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Result struct {
	// FormattedResults: Localized PageSpeed results. Contains a ruleResults
	// entry for each PageSpeed rule instantiated and run by the server.
	FormattedResults *ResultFormattedResults `json:"formattedResults,omitempty"`

	// Id: Canonicalized and final URL for the document, after following
	// page redirects (if any).
	Id string `json:"id,omitempty"`

	// InvalidRules: List of rules that were specified in the request, but
	// which the server did not know how to instantiate.
	InvalidRules []string `json:"invalidRules,omitempty"`

	// Kind: Kind of result.
	Kind string `json:"kind,omitempty"`

	// PageStats: Summary statistics for the page, such as number of
	// JavaScript bytes, number of HTML bytes, etc.
	PageStats *ResultPageStats `json:"pageStats,omitempty"`

	// ResponseCode: Response code for the document. 200 indicates a normal
	// page load. 4xx/5xx indicates an error.
	ResponseCode int64 `json:"responseCode,omitempty"`

	// RuleGroups: A map with one entry for each rule group in these
	// results.
	RuleGroups map[string]ResultRuleGroups `json:"ruleGroups,omitempty"`

	// Screenshot: Base64-encoded screenshot of the page that was analyzed.
	Screenshot *PagespeedApiImageV2 `json:"screenshot,omitempty"`

	// Title: Title of the page, as displayed in the browser's title bar.
	Title string `json:"title,omitempty"`

	// Version: The version of PageSpeed used to generate these results.
	Version *ResultVersion `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "FormattedResults") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FormattedResults") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Result) MarshalJSON() ([]byte, error) {
	type noMethod Result
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResultFormattedResults: Localized PageSpeed results. Contains a
// ruleResults entry for each PageSpeed rule instantiated and run by the
// server.
type ResultFormattedResults struct {
	// Locale: The locale of the formattedResults, e.g. "en_US".
	Locale string `json:"locale,omitempty"`

	// RuleResults: Dictionary of formatted rule results, with one entry for
	// each PageSpeed rule instantiated and run by the server.
	RuleResults map[string]ResultFormattedResultsRuleResults `json:"ruleResults,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Locale") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Locale") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultFormattedResults) MarshalJSON() ([]byte, error) {
	type noMethod ResultFormattedResults
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResultFormattedResultsRuleResults: The enum-like identifier for this
// rule. For instance "EnableKeepAlive" or "AvoidCssImport". Not
// localized.
type ResultFormattedResultsRuleResults struct {
	// Groups: List of rule groups that this rule belongs to. Each entry in
	// the list is one of "SPEED" or "USABILITY".
	Groups []string `json:"groups,omitempty"`

	// LocalizedRuleName: Localized name of the rule, intended for
	// presentation to a user.
	LocalizedRuleName string `json:"localizedRuleName,omitempty"`

	// RuleImpact: The impact (unbounded floating point value) that
	// implementing the suggestions for this rule would have on making the
	// page faster. Impact is comparable between rules to determine which
	// rule's suggestions would have a higher or lower impact on making a
	// page faster. For instance, if enabling compression would save 1MB,
	// while optimizing images would save 500kB, the enable compression rule
	// would have 2x the impact of the image optimization rule, all other
	// things being equal.
	RuleImpact float64 `json:"ruleImpact,omitempty"`

	// Summary: A brief summary description for the rule, indicating at a
	// high level what should be done to follow the rule and what benefit
	// can be gained by doing so.
	Summary *PagespeedApiFormatStringV2 `json:"summary,omitempty"`

	// UrlBlocks: List of blocks of URLs. Each block may contain a heading
	// and a list of URLs. Each URL may optionally include additional
	// details.
	UrlBlocks []*ResultFormattedResultsRuleResultsUrlBlocks `json:"urlBlocks,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Groups") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Groups") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultFormattedResultsRuleResults) MarshalJSON() ([]byte, error) {
	type noMethod ResultFormattedResultsRuleResults
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResultFormattedResultsRuleResultsUrlBlocks struct {
	// Header: Heading to be displayed with the list of URLs.
	Header *PagespeedApiFormatStringV2 `json:"header,omitempty"`

	// Urls: List of entries that provide information about URLs in the url
	// block. Optional.
	Urls []*ResultFormattedResultsRuleResultsUrlBlocksUrls `json:"urls,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Header") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Header") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultFormattedResultsRuleResultsUrlBlocks) MarshalJSON() ([]byte, error) {
	type noMethod ResultFormattedResultsRuleResultsUrlBlocks
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResultFormattedResultsRuleResultsUrlBlocksUrls struct {
	// Details: List of entries that provide additional details about a
	// single URL. Optional.
	Details []*PagespeedApiFormatStringV2 `json:"details,omitempty"`

	// Result: A format string that gives information about the URL, and a
	// list of arguments for that format string.
	Result *PagespeedApiFormatStringV2 `json:"result,omitempty"`

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

func (s *ResultFormattedResultsRuleResultsUrlBlocksUrls) MarshalJSON() ([]byte, error) {
	type noMethod ResultFormattedResultsRuleResultsUrlBlocksUrls
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResultPageStats: Summary statistics for the page, such as number of
// JavaScript bytes, number of HTML bytes, etc.
type ResultPageStats struct {
	// CssResponseBytes: Number of uncompressed response bytes for CSS
	// resources on the page.
	CssResponseBytes int64 `json:"cssResponseBytes,omitempty,string"`

	// FlashResponseBytes: Number of response bytes for flash resources on
	// the page.
	FlashResponseBytes int64 `json:"flashResponseBytes,omitempty,string"`

	// HtmlResponseBytes: Number of uncompressed response bytes for the main
	// HTML document and all iframes on the page.
	HtmlResponseBytes int64 `json:"htmlResponseBytes,omitempty,string"`

	// ImageResponseBytes: Number of response bytes for image resources on
	// the page.
	ImageResponseBytes int64 `json:"imageResponseBytes,omitempty,string"`

	// JavascriptResponseBytes: Number of uncompressed response bytes for JS
	// resources on the page.
	JavascriptResponseBytes int64 `json:"javascriptResponseBytes,omitempty,string"`

	// NumberCssResources: Number of CSS resources referenced by the page.
	NumberCssResources int64 `json:"numberCssResources,omitempty"`

	// NumberHosts: Number of unique hosts referenced by the page.
	NumberHosts int64 `json:"numberHosts,omitempty"`

	// NumberJsResources: Number of JavaScript resources referenced by the
	// page.
	NumberJsResources int64 `json:"numberJsResources,omitempty"`

	// NumberResources: Number of HTTP resources loaded by the page.
	NumberResources int64 `json:"numberResources,omitempty"`

	// NumberStaticResources: Number of static (i.e. cacheable) resources on
	// the page.
	NumberStaticResources int64 `json:"numberStaticResources,omitempty"`

	// OtherResponseBytes: Number of response bytes for other resources on
	// the page.
	OtherResponseBytes int64 `json:"otherResponseBytes,omitempty,string"`

	// TextResponseBytes: Number of uncompressed response bytes for text
	// resources not covered by other statistics (i.e non-HTML, non-script,
	// non-CSS resources) on the page.
	TextResponseBytes int64 `json:"textResponseBytes,omitempty,string"`

	// TotalRequestBytes: Total size of all request bytes sent by the page.
	TotalRequestBytes int64 `json:"totalRequestBytes,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "CssResponseBytes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CssResponseBytes") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ResultPageStats) MarshalJSON() ([]byte, error) {
	type noMethod ResultPageStats
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResultRuleGroups: The name of this rule group: one of "SPEED" or
// "USABILITY".
type ResultRuleGroups struct {
	// Score: The score (0-100) for this rule group, which indicates how
	// much better a page could be in that category (e.g. how much faster,
	// or how much more usable). A high score indicates little room for
	// improvement, while a lower score indicates more room for improvement.
	Score int64 `json:"score,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Score") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Score") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultRuleGroups) MarshalJSON() ([]byte, error) {
	type noMethod ResultRuleGroups
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResultVersion: The version of PageSpeed used to generate these
// results.
type ResultVersion struct {
	// Major: The major version number of PageSpeed used to generate these
	// results.
	Major int64 `json:"major,omitempty"`

	// Minor: The minor version number of PageSpeed used to generate these
	// results.
	Minor int64 `json:"minor,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Major") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Major") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultVersion) MarshalJSON() ([]byte, error) {
	type noMethod ResultVersion
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "pagespeedonline.pagespeedapi.runpagespeed":

type PagespeedapiRunpagespeedCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Runpagespeed: Runs PageSpeed analysis on the page at the specified
// URL, and returns PageSpeed scores, a list of suggestions to make that
// page faster, and other information.
func (r *PagespeedapiService) Runpagespeed(url string) *PagespeedapiRunpagespeedCall {
	c := &PagespeedapiRunpagespeedCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("url", url)
	return c
}

// FilterThirdPartyResources sets the optional parameter
// "filter_third_party_resources": Indicates if third party resources
// should be filtered out before PageSpeed analysis.
func (c *PagespeedapiRunpagespeedCall) FilterThirdPartyResources(filterThirdPartyResources bool) *PagespeedapiRunpagespeedCall {
	c.urlParams_.Set("filter_third_party_resources", fmt.Sprint(filterThirdPartyResources))
	return c
}

// Locale sets the optional parameter "locale": The locale used to
// localize formatted results
func (c *PagespeedapiRunpagespeedCall) Locale(locale string) *PagespeedapiRunpagespeedCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Rule sets the optional parameter "rule": A PageSpeed rule to run; if
// none are given, all rules are run
func (c *PagespeedapiRunpagespeedCall) Rule(rule ...string) *PagespeedapiRunpagespeedCall {
	c.urlParams_.SetMulti("rule", append([]string{}, rule...))
	return c
}

// Screenshot sets the optional parameter "screenshot": Indicates if
// binary data containing a screenshot should be included
func (c *PagespeedapiRunpagespeedCall) Screenshot(screenshot bool) *PagespeedapiRunpagespeedCall {
	c.urlParams_.Set("screenshot", fmt.Sprint(screenshot))
	return c
}

// Strategy sets the optional parameter "strategy": The analysis
// strategy to use
//
// Possible values:
//   "desktop" - Fetch and analyze the URL for desktop browsers
//   "mobile" - Fetch and analyze the URL for mobile devices
func (c *PagespeedapiRunpagespeedCall) Strategy(strategy string) *PagespeedapiRunpagespeedCall {
	c.urlParams_.Set("strategy", strategy)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PagespeedapiRunpagespeedCall) Fields(s ...googleapi.Field) *PagespeedapiRunpagespeedCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PagespeedapiRunpagespeedCall) IfNoneMatch(entityTag string) *PagespeedapiRunpagespeedCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PagespeedapiRunpagespeedCall) Context(ctx context.Context) *PagespeedapiRunpagespeedCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PagespeedapiRunpagespeedCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PagespeedapiRunpagespeedCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "runPagespeed")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "pagespeedonline.pagespeedapi.runpagespeed" call.
// Exactly one of *Result or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Result.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *PagespeedapiRunpagespeedCall) Do(opts ...googleapi.CallOption) (*Result, error) {
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
	ret := &Result{
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
	//   "description": "Runs PageSpeed analysis on the page at the specified URL, and returns PageSpeed scores, a list of suggestions to make that page faster, and other information.",
	//   "httpMethod": "GET",
	//   "id": "pagespeedonline.pagespeedapi.runpagespeed",
	//   "parameterOrder": [
	//     "url"
	//   ],
	//   "parameters": {
	//     "filter_third_party_resources": {
	//       "default": "false",
	//       "description": "Indicates if third party resources should be filtered out before PageSpeed analysis.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "locale": {
	//       "description": "The locale used to localize formatted results",
	//       "location": "query",
	//       "pattern": "[a-zA-Z]+(_[a-zA-Z]+)?",
	//       "type": "string"
	//     },
	//     "rule": {
	//       "description": "A PageSpeed rule to run; if none are given, all rules are run",
	//       "location": "query",
	//       "pattern": "[a-zA-Z]+",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "screenshot": {
	//       "default": "false",
	//       "description": "Indicates if binary data containing a screenshot should be included",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "strategy": {
	//       "description": "The analysis strategy to use",
	//       "enum": [
	//         "desktop",
	//         "mobile"
	//       ],
	//       "enumDescriptions": [
	//         "Fetch and analyze the URL for desktop browsers",
	//         "Fetch and analyze the URL for mobile devices"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "url": {
	//       "description": "The URL to fetch and analyze",
	//       "location": "query",
	//       "pattern": "(?i)http(s)?://.*",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "runPagespeed",
	//   "response": {
	//     "$ref": "Result"
	//   }
	// }

}

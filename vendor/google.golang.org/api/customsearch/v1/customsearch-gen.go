// Package customsearch provides access to the CustomSearch API.
//
// See https://developers.google.com/custom-search/v1/using_rest
//
// Usage example:
//
//   import "google.golang.org/api/customsearch/v1"
//   ...
//   customsearchService, err := customsearch.New(oauthHttpClient)
package customsearch // import "google.golang.org/api/customsearch/v1"

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

const apiId = "customsearch:v1"
const apiName = "customsearch"
const apiVersion = "v1"
const basePath = "https://www.googleapis.com/customsearch/"

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Cse = NewCseService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Cse *CseService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewCseService(s *Service) *CseService {
	rs := &CseService{s: s}
	return rs
}

type CseService struct {
	s *Service
}

type Context struct {
	Facets [][]*ContextFacetsItem `json:"facets,omitempty"`

	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Facets") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Facets") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Context) MarshalJSON() ([]byte, error) {
	type noMethod Context
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ContextFacetsItem struct {
	Anchor string `json:"anchor,omitempty"`

	Label string `json:"label,omitempty"`

	LabelWithOp string `json:"label_with_op,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Anchor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Anchor") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ContextFacetsItem) MarshalJSON() ([]byte, error) {
	type noMethod ContextFacetsItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Promotion struct {
	BodyLines []*PromotionBodyLines `json:"bodyLines,omitempty"`

	DisplayLink string `json:"displayLink,omitempty"`

	HtmlTitle string `json:"htmlTitle,omitempty"`

	Image *PromotionImage `json:"image,omitempty"`

	Link string `json:"link,omitempty"`

	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BodyLines") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BodyLines") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Promotion) MarshalJSON() ([]byte, error) {
	type noMethod Promotion
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PromotionBodyLines struct {
	HtmlTitle string `json:"htmlTitle,omitempty"`

	Link string `json:"link,omitempty"`

	Title string `json:"title,omitempty"`

	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "HtmlTitle") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HtmlTitle") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PromotionBodyLines) MarshalJSON() ([]byte, error) {
	type noMethod PromotionBodyLines
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PromotionImage struct {
	Height int64 `json:"height,omitempty"`

	Source string `json:"source,omitempty"`

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

func (s *PromotionImage) MarshalJSON() ([]byte, error) {
	type noMethod PromotionImage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Query struct {
	Count int64 `json:"count,omitempty"`

	Cr string `json:"cr,omitempty"`

	Cref string `json:"cref,omitempty"`

	Cx string `json:"cx,omitempty"`

	DateRestrict string `json:"dateRestrict,omitempty"`

	DisableCnTwTranslation string `json:"disableCnTwTranslation,omitempty"`

	ExactTerms string `json:"exactTerms,omitempty"`

	ExcludeTerms string `json:"excludeTerms,omitempty"`

	FileType string `json:"fileType,omitempty"`

	Filter string `json:"filter,omitempty"`

	Gl string `json:"gl,omitempty"`

	GoogleHost string `json:"googleHost,omitempty"`

	HighRange string `json:"highRange,omitempty"`

	Hl string `json:"hl,omitempty"`

	Hq string `json:"hq,omitempty"`

	ImgColorType string `json:"imgColorType,omitempty"`

	ImgDominantColor string `json:"imgDominantColor,omitempty"`

	ImgSize string `json:"imgSize,omitempty"`

	ImgType string `json:"imgType,omitempty"`

	InputEncoding string `json:"inputEncoding,omitempty"`

	Language string `json:"language,omitempty"`

	LinkSite string `json:"linkSite,omitempty"`

	LowRange string `json:"lowRange,omitempty"`

	OrTerms string `json:"orTerms,omitempty"`

	OutputEncoding string `json:"outputEncoding,omitempty"`

	RelatedSite string `json:"relatedSite,omitempty"`

	Rights string `json:"rights,omitempty"`

	Safe string `json:"safe,omitempty"`

	SearchTerms string `json:"searchTerms,omitempty"`

	SearchType string `json:"searchType,omitempty"`

	SiteSearch string `json:"siteSearch,omitempty"`

	SiteSearchFilter string `json:"siteSearchFilter,omitempty"`

	Sort string `json:"sort,omitempty"`

	StartIndex int64 `json:"startIndex,omitempty"`

	StartPage int64 `json:"startPage,omitempty"`

	Title string `json:"title,omitempty"`

	TotalResults int64 `json:"totalResults,omitempty,string"`

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

func (s *Query) MarshalJSON() ([]byte, error) {
	type noMethod Query
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Result struct {
	CacheId string `json:"cacheId,omitempty"`

	DisplayLink string `json:"displayLink,omitempty"`

	FileFormat string `json:"fileFormat,omitempty"`

	FormattedUrl string `json:"formattedUrl,omitempty"`

	HtmlFormattedUrl string `json:"htmlFormattedUrl,omitempty"`

	HtmlSnippet string `json:"htmlSnippet,omitempty"`

	HtmlTitle string `json:"htmlTitle,omitempty"`

	Image *ResultImage `json:"image,omitempty"`

	Kind string `json:"kind,omitempty"`

	Labels []*ResultLabels `json:"labels,omitempty"`

	Link string `json:"link,omitempty"`

	Mime string `json:"mime,omitempty"`

	Pagemap map[string][]googleapi.RawMessage `json:"pagemap,omitempty"`

	Snippet string `json:"snippet,omitempty"`

	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CacheId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CacheId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Result) MarshalJSON() ([]byte, error) {
	type noMethod Result
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResultImage struct {
	ByteSize int64 `json:"byteSize,omitempty"`

	ContextLink string `json:"contextLink,omitempty"`

	Height int64 `json:"height,omitempty"`

	ThumbnailHeight int64 `json:"thumbnailHeight,omitempty"`

	ThumbnailLink string `json:"thumbnailLink,omitempty"`

	ThumbnailWidth int64 `json:"thumbnailWidth,omitempty"`

	Width int64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ByteSize") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ByteSize") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultImage) MarshalJSON() ([]byte, error) {
	type noMethod ResultImage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResultLabels struct {
	DisplayName string `json:"displayName,omitempty"`

	LabelWithOp string `json:"label_with_op,omitempty"`

	Name string `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DisplayName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DisplayName") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResultLabels) MarshalJSON() ([]byte, error) {
	type noMethod ResultLabels
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Search struct {
	Context *Context `json:"context,omitempty"`

	Items []*Result `json:"items,omitempty"`

	Kind string `json:"kind,omitempty"`

	Promotions []*Promotion `json:"promotions,omitempty"`

	Queries map[string][]Query `json:"queries,omitempty"`

	SearchInformation *SearchSearchInformation `json:"searchInformation,omitempty"`

	Spelling *SearchSpelling `json:"spelling,omitempty"`

	Url *SearchUrl `json:"url,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *Search) MarshalJSON() ([]byte, error) {
	type noMethod Search
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SearchSearchInformation struct {
	FormattedSearchTime string `json:"formattedSearchTime,omitempty"`

	FormattedTotalResults string `json:"formattedTotalResults,omitempty"`

	SearchTime float64 `json:"searchTime,omitempty"`

	TotalResults int64 `json:"totalResults,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "FormattedSearchTime")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FormattedSearchTime") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SearchSearchInformation) MarshalJSON() ([]byte, error) {
	type noMethod SearchSearchInformation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SearchSpelling struct {
	CorrectedQuery string `json:"correctedQuery,omitempty"`

	HtmlCorrectedQuery string `json:"htmlCorrectedQuery,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CorrectedQuery") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CorrectedQuery") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SearchSpelling) MarshalJSON() ([]byte, error) {
	type noMethod SearchSpelling
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SearchUrl struct {
	Template string `json:"template,omitempty"`

	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Template") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Template") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SearchUrl) MarshalJSON() ([]byte, error) {
	type noMethod SearchUrl
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "search.cse.list":

type CseListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns metadata about the search performed, metadata about the
// custom search engine used for the search, and the search results.
func (r *CseService) List(q string) *CseListCall {
	c := &CseListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("q", q)
	return c
}

// C2coff sets the optional parameter "c2coff": Turns off the
// translation between zh-CN and zh-TW.
func (c *CseListCall) C2coff(c2coff string) *CseListCall {
	c.urlParams_.Set("c2coff", c2coff)
	return c
}

// Cr sets the optional parameter "cr": Country restrict(s).
func (c *CseListCall) Cr(cr string) *CseListCall {
	c.urlParams_.Set("cr", cr)
	return c
}

// Cref sets the optional parameter "cref": The URL of a linked custom
// search engine
func (c *CseListCall) Cref(cref string) *CseListCall {
	c.urlParams_.Set("cref", cref)
	return c
}

// Cx sets the optional parameter "cx": The custom search engine ID to
// scope this search query
func (c *CseListCall) Cx(cx string) *CseListCall {
	c.urlParams_.Set("cx", cx)
	return c
}

// DateRestrict sets the optional parameter "dateRestrict": Specifies
// all search results are from a time period
func (c *CseListCall) DateRestrict(dateRestrict string) *CseListCall {
	c.urlParams_.Set("dateRestrict", dateRestrict)
	return c
}

// ExactTerms sets the optional parameter "exactTerms": Identifies a
// phrase that all documents in the search results must contain
func (c *CseListCall) ExactTerms(exactTerms string) *CseListCall {
	c.urlParams_.Set("exactTerms", exactTerms)
	return c
}

// ExcludeTerms sets the optional parameter "excludeTerms": Identifies a
// word or phrase that should not appear in any documents in the search
// results
func (c *CseListCall) ExcludeTerms(excludeTerms string) *CseListCall {
	c.urlParams_.Set("excludeTerms", excludeTerms)
	return c
}

// FileType sets the optional parameter "fileType": Returns images of a
// specified type. Some of the allowed values are: bmp, gif, png, jpg,
// svg, pdf, ...
func (c *CseListCall) FileType(fileType string) *CseListCall {
	c.urlParams_.Set("fileType", fileType)
	return c
}

// Filter sets the optional parameter "filter": Controls turning on or
// off the duplicate content filter.
//
// Possible values:
//   "0" - Turns off duplicate content filter.
//   "1" - Turns on duplicate content filter.
func (c *CseListCall) Filter(filter string) *CseListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// Gl sets the optional parameter "gl": Geolocation of end user.
func (c *CseListCall) Gl(gl string) *CseListCall {
	c.urlParams_.Set("gl", gl)
	return c
}

// Googlehost sets the optional parameter "googlehost": The local Google
// domain to use to perform the search.
func (c *CseListCall) Googlehost(googlehost string) *CseListCall {
	c.urlParams_.Set("googlehost", googlehost)
	return c
}

// HighRange sets the optional parameter "highRange": Creates a range in
// form as_nlo value..as_nhi value and attempts to append it to query
func (c *CseListCall) HighRange(highRange string) *CseListCall {
	c.urlParams_.Set("highRange", highRange)
	return c
}

// Hl sets the optional parameter "hl": Sets the user interface
// language.
func (c *CseListCall) Hl(hl string) *CseListCall {
	c.urlParams_.Set("hl", hl)
	return c
}

// Hq sets the optional parameter "hq": Appends the extra query terms to
// the query.
func (c *CseListCall) Hq(hq string) *CseListCall {
	c.urlParams_.Set("hq", hq)
	return c
}

// ImgColorType sets the optional parameter "imgColorType": Returns
// black and white, grayscale, or color images: mono, gray, and color.
//
// Possible values:
//   "color" - color
//   "gray" - gray
//   "mono" - mono
func (c *CseListCall) ImgColorType(imgColorType string) *CseListCall {
	c.urlParams_.Set("imgColorType", imgColorType)
	return c
}

// ImgDominantColor sets the optional parameter "imgDominantColor":
// Returns images of a specific dominant color: yellow, green, teal,
// blue, purple, pink, white, gray, black and brown.
//
// Possible values:
//   "black" - black
//   "blue" - blue
//   "brown" - brown
//   "gray" - gray
//   "green" - green
//   "pink" - pink
//   "purple" - purple
//   "teal" - teal
//   "white" - white
//   "yellow" - yellow
func (c *CseListCall) ImgDominantColor(imgDominantColor string) *CseListCall {
	c.urlParams_.Set("imgDominantColor", imgDominantColor)
	return c
}

// ImgSize sets the optional parameter "imgSize": Returns images of a
// specified size, where size can be one of: icon, small, medium, large,
// xlarge, xxlarge, and huge.
//
// Possible values:
//   "huge" - huge
//   "icon" - icon
//   "large" - large
//   "medium" - medium
//   "small" - small
//   "xlarge" - xlarge
//   "xxlarge" - xxlarge
func (c *CseListCall) ImgSize(imgSize string) *CseListCall {
	c.urlParams_.Set("imgSize", imgSize)
	return c
}

// ImgType sets the optional parameter "imgType": Returns images of a
// type, which can be one of: clipart, face, lineart, news, and photo.
//
// Possible values:
//   "clipart" - clipart
//   "face" - face
//   "lineart" - lineart
//   "news" - news
//   "photo" - photo
func (c *CseListCall) ImgType(imgType string) *CseListCall {
	c.urlParams_.Set("imgType", imgType)
	return c
}

// LinkSite sets the optional parameter "linkSite": Specifies that all
// search results should contain a link to a particular URL
func (c *CseListCall) LinkSite(linkSite string) *CseListCall {
	c.urlParams_.Set("linkSite", linkSite)
	return c
}

// LowRange sets the optional parameter "lowRange": Creates a range in
// form as_nlo value..as_nhi value and attempts to append it to query
func (c *CseListCall) LowRange(lowRange string) *CseListCall {
	c.urlParams_.Set("lowRange", lowRange)
	return c
}

// Lr sets the optional parameter "lr": The language restriction for the
// search results
//
// Possible values:
//   "lang_ar" - Arabic
//   "lang_bg" - Bulgarian
//   "lang_ca" - Catalan
//   "lang_cs" - Czech
//   "lang_da" - Danish
//   "lang_de" - German
//   "lang_el" - Greek
//   "lang_en" - English
//   "lang_es" - Spanish
//   "lang_et" - Estonian
//   "lang_fi" - Finnish
//   "lang_fr" - French
//   "lang_hr" - Croatian
//   "lang_hu" - Hungarian
//   "lang_id" - Indonesian
//   "lang_is" - Icelandic
//   "lang_it" - Italian
//   "lang_iw" - Hebrew
//   "lang_ja" - Japanese
//   "lang_ko" - Korean
//   "lang_lt" - Lithuanian
//   "lang_lv" - Latvian
//   "lang_nl" - Dutch
//   "lang_no" - Norwegian
//   "lang_pl" - Polish
//   "lang_pt" - Portuguese
//   "lang_ro" - Romanian
//   "lang_ru" - Russian
//   "lang_sk" - Slovak
//   "lang_sl" - Slovenian
//   "lang_sr" - Serbian
//   "lang_sv" - Swedish
//   "lang_tr" - Turkish
//   "lang_zh-CN" - Chinese (Simplified)
//   "lang_zh-TW" - Chinese (Traditional)
func (c *CseListCall) Lr(lr string) *CseListCall {
	c.urlParams_.Set("lr", lr)
	return c
}

// Num sets the optional parameter "num": Number of search results to
// return
func (c *CseListCall) Num(num int64) *CseListCall {
	c.urlParams_.Set("num", fmt.Sprint(num))
	return c
}

// OrTerms sets the optional parameter "orTerms": Provides additional
// search terms to check for in a document, where each document in the
// search results must contain at least one of the additional search
// terms
func (c *CseListCall) OrTerms(orTerms string) *CseListCall {
	c.urlParams_.Set("orTerms", orTerms)
	return c
}

// RelatedSite sets the optional parameter "relatedSite": Specifies that
// all search results should be pages that are related to the specified
// URL
func (c *CseListCall) RelatedSite(relatedSite string) *CseListCall {
	c.urlParams_.Set("relatedSite", relatedSite)
	return c
}

// Rights sets the optional parameter "rights": Filters based on
// licensing. Supported values include: cc_publicdomain, cc_attribute,
// cc_sharealike, cc_noncommercial, cc_nonderived and combinations of
// these.
func (c *CseListCall) Rights(rights string) *CseListCall {
	c.urlParams_.Set("rights", rights)
	return c
}

// Safe sets the optional parameter "safe": Search safety level
//
// Possible values:
//   "high" - Enables highest level of safe search filtering.
//   "medium" - Enables moderate safe search filtering.
//   "off" (default) - Disables safe search filtering.
func (c *CseListCall) Safe(safe string) *CseListCall {
	c.urlParams_.Set("safe", safe)
	return c
}

// SearchType sets the optional parameter "searchType": Specifies the
// search type: image.
//
// Possible values:
//   "image" - custom image search
func (c *CseListCall) SearchType(searchType string) *CseListCall {
	c.urlParams_.Set("searchType", searchType)
	return c
}

// SiteSearch sets the optional parameter "siteSearch": Specifies all
// search results should be pages from a given site
func (c *CseListCall) SiteSearch(siteSearch string) *CseListCall {
	c.urlParams_.Set("siteSearch", siteSearch)
	return c
}

// SiteSearchFilter sets the optional parameter "siteSearchFilter":
// Controls whether to include or exclude results from the site named in
// the as_sitesearch parameter
//
// Possible values:
//   "e" - exclude
//   "i" - include
func (c *CseListCall) SiteSearchFilter(siteSearchFilter string) *CseListCall {
	c.urlParams_.Set("siteSearchFilter", siteSearchFilter)
	return c
}

// Sort sets the optional parameter "sort": The sort expression to apply
// to the results
func (c *CseListCall) Sort(sort string) *CseListCall {
	c.urlParams_.Set("sort", sort)
	return c
}

// Start sets the optional parameter "start": The index of the first
// result to return
func (c *CseListCall) Start(start int64) *CseListCall {
	c.urlParams_.Set("start", fmt.Sprint(start))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CseListCall) Fields(s ...googleapi.Field) *CseListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CseListCall) IfNoneMatch(entityTag string) *CseListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CseListCall) Context(ctx context.Context) *CseListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CseListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CseListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "search.cse.list" call.
// Exactly one of *Search or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Search.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CseListCall) Do(opts ...googleapi.CallOption) (*Search, error) {
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
	ret := &Search{
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
	//   "description": "Returns metadata about the search performed, metadata about the custom search engine used for the search, and the search results.",
	//   "httpMethod": "GET",
	//   "id": "search.cse.list",
	//   "parameterOrder": [
	//     "q"
	//   ],
	//   "parameters": {
	//     "c2coff": {
	//       "description": "Turns off the translation between zh-CN and zh-TW.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "cr": {
	//       "description": "Country restrict(s).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "cref": {
	//       "description": "The URL of a linked custom search engine",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "cx": {
	//       "description": "The custom search engine ID to scope this search query",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "dateRestrict": {
	//       "description": "Specifies all search results are from a time period",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "exactTerms": {
	//       "description": "Identifies a phrase that all documents in the search results must contain",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "excludeTerms": {
	//       "description": "Identifies a word or phrase that should not appear in any documents in the search results",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "fileType": {
	//       "description": "Returns images of a specified type. Some of the allowed values are: bmp, gif, png, jpg, svg, pdf, ...",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "filter": {
	//       "description": "Controls turning on or off the duplicate content filter.",
	//       "enum": [
	//         "0",
	//         "1"
	//       ],
	//       "enumDescriptions": [
	//         "Turns off duplicate content filter.",
	//         "Turns on duplicate content filter."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "gl": {
	//       "description": "Geolocation of end user.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "googlehost": {
	//       "description": "The local Google domain to use to perform the search.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "highRange": {
	//       "description": "Creates a range in form as_nlo value..as_nhi value and attempts to append it to query",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "hl": {
	//       "description": "Sets the user interface language.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "hq": {
	//       "description": "Appends the extra query terms to the query.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "imgColorType": {
	//       "description": "Returns black and white, grayscale, or color images: mono, gray, and color.",
	//       "enum": [
	//         "color",
	//         "gray",
	//         "mono"
	//       ],
	//       "enumDescriptions": [
	//         "color",
	//         "gray",
	//         "mono"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "imgDominantColor": {
	//       "description": "Returns images of a specific dominant color: yellow, green, teal, blue, purple, pink, white, gray, black and brown.",
	//       "enum": [
	//         "black",
	//         "blue",
	//         "brown",
	//         "gray",
	//         "green",
	//         "pink",
	//         "purple",
	//         "teal",
	//         "white",
	//         "yellow"
	//       ],
	//       "enumDescriptions": [
	//         "black",
	//         "blue",
	//         "brown",
	//         "gray",
	//         "green",
	//         "pink",
	//         "purple",
	//         "teal",
	//         "white",
	//         "yellow"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "imgSize": {
	//       "description": "Returns images of a specified size, where size can be one of: icon, small, medium, large, xlarge, xxlarge, and huge.",
	//       "enum": [
	//         "huge",
	//         "icon",
	//         "large",
	//         "medium",
	//         "small",
	//         "xlarge",
	//         "xxlarge"
	//       ],
	//       "enumDescriptions": [
	//         "huge",
	//         "icon",
	//         "large",
	//         "medium",
	//         "small",
	//         "xlarge",
	//         "xxlarge"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "imgType": {
	//       "description": "Returns images of a type, which can be one of: clipart, face, lineart, news, and photo.",
	//       "enum": [
	//         "clipart",
	//         "face",
	//         "lineart",
	//         "news",
	//         "photo"
	//       ],
	//       "enumDescriptions": [
	//         "clipart",
	//         "face",
	//         "lineart",
	//         "news",
	//         "photo"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "linkSite": {
	//       "description": "Specifies that all search results should contain a link to a particular URL",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "lowRange": {
	//       "description": "Creates a range in form as_nlo value..as_nhi value and attempts to append it to query",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "lr": {
	//       "description": "The language restriction for the search results",
	//       "enum": [
	//         "lang_ar",
	//         "lang_bg",
	//         "lang_ca",
	//         "lang_cs",
	//         "lang_da",
	//         "lang_de",
	//         "lang_el",
	//         "lang_en",
	//         "lang_es",
	//         "lang_et",
	//         "lang_fi",
	//         "lang_fr",
	//         "lang_hr",
	//         "lang_hu",
	//         "lang_id",
	//         "lang_is",
	//         "lang_it",
	//         "lang_iw",
	//         "lang_ja",
	//         "lang_ko",
	//         "lang_lt",
	//         "lang_lv",
	//         "lang_nl",
	//         "lang_no",
	//         "lang_pl",
	//         "lang_pt",
	//         "lang_ro",
	//         "lang_ru",
	//         "lang_sk",
	//         "lang_sl",
	//         "lang_sr",
	//         "lang_sv",
	//         "lang_tr",
	//         "lang_zh-CN",
	//         "lang_zh-TW"
	//       ],
	//       "enumDescriptions": [
	//         "Arabic",
	//         "Bulgarian",
	//         "Catalan",
	//         "Czech",
	//         "Danish",
	//         "German",
	//         "Greek",
	//         "English",
	//         "Spanish",
	//         "Estonian",
	//         "Finnish",
	//         "French",
	//         "Croatian",
	//         "Hungarian",
	//         "Indonesian",
	//         "Icelandic",
	//         "Italian",
	//         "Hebrew",
	//         "Japanese",
	//         "Korean",
	//         "Lithuanian",
	//         "Latvian",
	//         "Dutch",
	//         "Norwegian",
	//         "Polish",
	//         "Portuguese",
	//         "Romanian",
	//         "Russian",
	//         "Slovak",
	//         "Slovenian",
	//         "Serbian",
	//         "Swedish",
	//         "Turkish",
	//         "Chinese (Simplified)",
	//         "Chinese (Traditional)"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "num": {
	//       "default": "10",
	//       "description": "Number of search results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "orTerms": {
	//       "description": "Provides additional search terms to check for in a document, where each document in the search results must contain at least one of the additional search terms",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Query",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "relatedSite": {
	//       "description": "Specifies that all search results should be pages that are related to the specified URL",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "rights": {
	//       "description": "Filters based on licensing. Supported values include: cc_publicdomain, cc_attribute, cc_sharealike, cc_noncommercial, cc_nonderived and combinations of these.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "safe": {
	//       "default": "off",
	//       "description": "Search safety level",
	//       "enum": [
	//         "high",
	//         "medium",
	//         "off"
	//       ],
	//       "enumDescriptions": [
	//         "Enables highest level of safe search filtering.",
	//         "Enables moderate safe search filtering.",
	//         "Disables safe search filtering."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "searchType": {
	//       "description": "Specifies the search type: image.",
	//       "enum": [
	//         "image"
	//       ],
	//       "enumDescriptions": [
	//         "custom image search"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "siteSearch": {
	//       "description": "Specifies all search results should be pages from a given site",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "siteSearchFilter": {
	//       "description": "Controls whether to include or exclude results from the site named in the as_sitesearch parameter",
	//       "enum": [
	//         "e",
	//         "i"
	//       ],
	//       "enumDescriptions": [
	//         "exclude",
	//         "include"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sort": {
	//       "description": "The sort expression to apply to the results",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "start": {
	//       "description": "The index of the first result to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     }
	//   },
	//   "path": "v1",
	//   "response": {
	//     "$ref": "Search"
	//   }
	// }

}

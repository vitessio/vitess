// Package manufacturers provides access to the Manufacturer Center API.
//
// See https://developers.google.com/manufacturers/
//
// Usage example:
//
//   import "google.golang.org/api/manufacturers/v1"
//   ...
//   manufacturersService, err := manufacturers.New(oauthHttpClient)
package manufacturers // import "google.golang.org/api/manufacturers/v1"

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

const apiId = "manufacturers:v1"
const apiName = "manufacturers"
const apiVersion = "v1"
const basePath = "https://manufacturers.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// Manage your product listings for Google Manufacturer Center
	ManufacturercenterScope = "https://www.googleapis.com/auth/manufacturercenter"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Accounts = NewAccountsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Accounts *AccountsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewAccountsService(s *Service) *AccountsService {
	rs := &AccountsService{s: s}
	rs.Products = NewAccountsProductsService(s)
	return rs
}

type AccountsService struct {
	s *Service

	Products *AccountsProductsService
}

func NewAccountsProductsService(s *Service) *AccountsProductsService {
	rs := &AccountsProductsService{s: s}
	return rs
}

type AccountsProductsService struct {
	s *Service
}

// Attributes: Attributes of the product. For more information,
// see
// https://support.google.com/manufacturers/answer/6124116.
type Attributes struct {
	// Brand: The brand name of the product. For more information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#brand.
	Brand string `json:"brand,omitempty"`

	// Gtin: The Global Trade Item Number (GTIN) of the product. For more
	// information,
	// see https://support.google.com/manufacturers/answer/6124116#gtin.
	Gtin []string `json:"gtin,omitempty"`

	// Mpn: The Manufacturer Part Number (MPN) of the product. For more
	// information,
	// see https://support.google.com/manufacturers/answer/6124116#mpn.
	Mpn string `json:"mpn,omitempty"`

	// ProductLine: The name of the group of products related to the
	// product. For more
	// information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#productlin
	// e.
	ProductLine string `json:"productLine,omitempty"`

	// ProductName: The canonical name of the product. For more information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#productnam
	// e.
	ProductName string `json:"productName,omitempty"`

	// ProductPageUrl: The URL of the manufacturer's detail page of the
	// product. For more
	// information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#productpag
	// e.
	ProductPageUrl string `json:"productPageUrl,omitempty"`

	// ProductType: The manufacturer's category of the product. For more
	// information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#producttyp
	// e.
	ProductType []string `json:"productType,omitempty"`

	// Title: The title of the product. For more information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#title.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Brand") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Brand") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Attributes) MarshalJSON() ([]byte, error) {
	type noMethod Attributes
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Issue: Product issue.
type Issue struct {
	// Attribute: If present, the attribute that triggered the issue. For
	// more information
	// about attributes,
	// see
	// https://support.google.com/manufacturers/answer/6124116.
	Attribute string `json:"attribute,omitempty"`

	// Description: Description of the issue.
	Description string `json:"description,omitempty"`

	// Severity: The severity of the issue.
	//
	// Possible values:
	//   "SEVERITY_UNSPECIFIED" - Unspecified severity, never used.
	//   "ERROR" - Error severity. The issue prevents the usage of the whole
	// item.
	//   "WARNING" - Warning severity. The issue is either one that prevents
	// the usage of the
	// attribute that triggered it or one that will soon prevent the usage
	// of
	// the whole item.
	//   "INFO" - Info severity. The issue is one that doesn't require
	// immediate attention.
	// It is, for example, used to communicate which attributes are
	// still
	// pending review.
	Severity string `json:"severity,omitempty"`

	// Type: The server-generated type of the issue, for
	// example,
	// “INCORRECT_TEXT_FORMATTING”, “IMAGE_NOT_SERVEABLE”, etc.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Attribute") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Attribute") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Issue) MarshalJSON() ([]byte, error) {
	type noMethod Issue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListProductsResponse struct {
	// NextPageToken: The token for the retrieval of the next page of
	// product statuses.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Products: List of the products.
	Products []*Product `json:"products,omitempty"`

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

func (s *ListProductsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListProductsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Product: Product data.
type Product struct {
	// ContentLanguage: The content language of the product as a two-letter
	// ISO 639-1 language code
	// (for example, en).
	// @OutputOnly
	ContentLanguage string `json:"contentLanguage,omitempty"`

	// FinalAttributes: Final attributes of the product. The final
	// attributes are obtained by
	// overriding the uploaded attributes with the manually provided and
	// deleted
	// attributes. Google systems only process, evaluate, review, and/or use
	// final
	// attributes.
	// @OutputOnly
	FinalAttributes *Attributes `json:"finalAttributes,omitempty"`

	// Issues: A server-generated list of issues associated with the
	// product.
	// @OutputOnly
	Issues []*Issue `json:"issues,omitempty"`

	// ManuallyDeletedAttributes: Names of the attributes of the product
	// deleted manually via the
	// Manufacturer Center UI.
	// @OutputOnly
	ManuallyDeletedAttributes []string `json:"manuallyDeletedAttributes,omitempty"`

	// ManuallyProvidedAttributes: Attributes of the product provided
	// manually via the Manufacturer Center UI.
	// @OutputOnly
	ManuallyProvidedAttributes *Attributes `json:"manuallyProvidedAttributes,omitempty"`

	// Name: Name in the format
	// `{target_country}:{content_language}:{product_id}`.
	//
	// `target_country`   - The target country of the product as a CLDR
	// territory
	//                      code (for example, US).
	//
	// `content_language` - The content language of the product as a
	// two-letter
	//                      ISO 639-1 language code (for example,
	// en).
	//
	// `product_id`     -   The ID of the product. For more information,
	// see
	//
	// https://support.google.com/manufacturers/answer/6124116#id.
	// @OutputOnl
	// y
	Name string `json:"name,omitempty"`

	// Parent: Parent ID in the format
	// `accounts/{account_id}`.
	//
	// `account_id` - The ID of the Manufacturer Center account.
	// @OutputOnly
	Parent string `json:"parent,omitempty"`

	// ProductId: The ID of the product. For more information,
	// see
	// https://support.google.com/manufacturers/answer/6124116#id.
	// @Outpu
	// tOnly
	ProductId string `json:"productId,omitempty"`

	// TargetCountry: The target country of the product as a CLDR territory
	// code (for example,
	// US).
	// @OutputOnly
	TargetCountry string `json:"targetCountry,omitempty"`

	// UploadedAttributes: Attributes of the product uploaded via the
	// Manufacturer Center API or via
	// feeds.
	UploadedAttributes *Attributes `json:"uploadedAttributes,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ContentLanguage") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContentLanguage") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Product) MarshalJSON() ([]byte, error) {
	type noMethod Product
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "manufacturers.accounts.products.get":

type AccountsProductsGetCall struct {
	s            *Service
	parent       string
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the product from a Manufacturer Center account, including
// product
// issues.
func (r *AccountsProductsService) Get(parent string, name string) *AccountsProductsGetCall {
	c := &AccountsProductsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsProductsGetCall) Fields(s ...googleapi.Field) *AccountsProductsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AccountsProductsGetCall) IfNoneMatch(entityTag string) *AccountsProductsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsProductsGetCall) Context(ctx context.Context) *AccountsProductsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsProductsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsProductsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/{+parent}/products/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
		"name":   c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "manufacturers.accounts.products.get" call.
// Exactly one of *Product or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Product.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AccountsProductsGetCall) Do(opts ...googleapi.CallOption) (*Product, error) {
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
	ret := &Product{
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
	//   "description": "Gets the product from a Manufacturer Center account, including product\nissues.",
	//   "flatPath": "v1/accounts/{accountsId}/products/{productsId}",
	//   "httpMethod": "GET",
	//   "id": "manufacturers.accounts.products.get",
	//   "parameterOrder": [
	//     "parent",
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Name in the format `{target_country}:{content_language}:{product_id}`.\n\n`target_country`   - The target country of the product as a CLDR territory\n                     code (for example, US).\n\n`content_language` - The content language of the product as a two-letter\n                     ISO 639-1 language code (for example, en).\n\n`product_id`     -   The ID of the product. For more information, see\n                     https://support.google.com/manufacturers/answer/6124116#id.",
	//       "location": "path",
	//       "pattern": "^[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Parent ID in the format `accounts/{account_id}`.\n\n`account_id` - The ID of the Manufacturer Center account.",
	//       "location": "path",
	//       "pattern": "^accounts/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/{+parent}/products/{+name}",
	//   "response": {
	//     "$ref": "Product"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/manufacturercenter"
	//   ]
	// }

}

// method id "manufacturers.accounts.products.list":

type AccountsProductsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists all the products in a Manufacturer Center account.
func (r *AccountsProductsService) List(parent string) *AccountsProductsListCall {
	c := &AccountsProductsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// product statuses to return in the response, used for
// paging.
func (c *AccountsProductsListCall) PageSize(pageSize int64) *AccountsProductsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The token returned
// by the previous request.
func (c *AccountsProductsListCall) PageToken(pageToken string) *AccountsProductsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsProductsListCall) Fields(s ...googleapi.Field) *AccountsProductsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AccountsProductsListCall) IfNoneMatch(entityTag string) *AccountsProductsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsProductsListCall) Context(ctx context.Context) *AccountsProductsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsProductsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsProductsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/{+parent}/products")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "manufacturers.accounts.products.list" call.
// Exactly one of *ListProductsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListProductsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *AccountsProductsListCall) Do(opts ...googleapi.CallOption) (*ListProductsResponse, error) {
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
	ret := &ListProductsResponse{
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
	//   "description": "Lists all the products in a Manufacturer Center account.",
	//   "flatPath": "v1/accounts/{accountsId}/products",
	//   "httpMethod": "GET",
	//   "id": "manufacturers.accounts.products.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "pageSize": {
	//       "description": "Maximum number of product statuses to return in the response, used for\npaging.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The token returned by the previous request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Parent ID in the format `accounts/{account_id}`.\n\n`account_id` - The ID of the Manufacturer Center account.",
	//       "location": "path",
	//       "pattern": "^accounts/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/{+parent}/products",
	//   "response": {
	//     "$ref": "ListProductsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/manufacturercenter"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AccountsProductsListCall) Pages(ctx context.Context, f func(*ListProductsResponse) error) error {
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

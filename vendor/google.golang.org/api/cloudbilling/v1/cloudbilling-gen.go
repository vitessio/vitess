// Package cloudbilling provides access to the Google Cloud Billing API.
//
// See https://cloud.google.com/billing/
//
// Usage example:
//
//   import "google.golang.org/api/cloudbilling/v1"
//   ...
//   cloudbillingService, err := cloudbilling.New(oauthHttpClient)
package cloudbilling // import "google.golang.org/api/cloudbilling/v1"

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

const apiId = "cloudbilling:v1"
const apiName = "cloudbilling"
const apiVersion = "v1"
const basePath = "https://cloudbilling.googleapis.com/"

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
	s.BillingAccounts = NewBillingAccountsService(s)
	s.Projects = NewProjectsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	BillingAccounts *BillingAccountsService

	Projects *ProjectsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewBillingAccountsService(s *Service) *BillingAccountsService {
	rs := &BillingAccountsService{s: s}
	rs.Projects = NewBillingAccountsProjectsService(s)
	return rs
}

type BillingAccountsService struct {
	s *Service

	Projects *BillingAccountsProjectsService
}

func NewBillingAccountsProjectsService(s *Service) *BillingAccountsProjectsService {
	rs := &BillingAccountsProjectsService{s: s}
	return rs
}

type BillingAccountsProjectsService struct {
	s *Service
}

func NewProjectsService(s *Service) *ProjectsService {
	rs := &ProjectsService{s: s}
	return rs
}

type ProjectsService struct {
	s *Service
}

// BillingAccount: A billing account in [Google Developers
// Console](https://console.developers.google.com/). You can assign a
// billing account to one or more projects.
type BillingAccount struct {
	// DisplayName: The display name given to the billing account, such as
	// `My Billing Account`. This name is displayed in the Google Developers
	// Console.
	DisplayName string `json:"displayName,omitempty"`

	// Name: The resource name of the billing account. The resource name has
	// the form `billingAccounts/{billing_account_id}`. For example,
	// `billingAccounts/012345-567890-ABCDEF` would be the resource name for
	// billing account `012345-567890-ABCDEF`.
	Name string `json:"name,omitempty"`

	// Open: True if the billing account is open, and will therefore be
	// charged for any usage on associated projects. False if the billing
	// account is closed, and therefore projects associated with it will be
	// unable to use paid services.
	Open bool `json:"open,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *BillingAccount) MarshalJSON() ([]byte, error) {
	type noMethod BillingAccount
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListBillingAccountsResponse: Response message for
// `ListBillingAccounts`.
type ListBillingAccountsResponse struct {
	// BillingAccounts: A list of billing accounts.
	BillingAccounts []*BillingAccount `json:"billingAccounts,omitempty"`

	// NextPageToken: A token to retrieve the next page of results. To
	// retrieve the next page, call `ListBillingAccounts` again with the
	// `page_token` field set to this value. This field is empty if there
	// are no more results to retrieve.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BillingAccounts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BillingAccounts") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ListBillingAccountsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListBillingAccountsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListProjectBillingInfoResponse: Request message for
// `ListProjectBillingInfoResponse`.
type ListProjectBillingInfoResponse struct {
	// NextPageToken: A token to retrieve the next page of results. To
	// retrieve the next page, call `ListProjectBillingInfo` again with the
	// `page_token` field set to this value. This field is empty if there
	// are no more results to retrieve.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ProjectBillingInfo: A list of `ProjectBillingInfo` resources
	// representing the projects associated with the billing account.
	ProjectBillingInfo []*ProjectBillingInfo `json:"projectBillingInfo,omitempty"`

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

func (s *ListProjectBillingInfoResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListProjectBillingInfoResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ProjectBillingInfo: Encapsulation of billing information for a
// Developers Console project. A project has at most one associated
// billing account at a time (but a billing account can be assigned to
// multiple projects).
type ProjectBillingInfo struct {
	// BillingAccountName: The resource name of the billing account
	// associated with the project, if any. For example,
	// `billingAccounts/012345-567890-ABCDEF`.
	BillingAccountName string `json:"billingAccountName,omitempty"`

	// BillingEnabled: True if the project is associated with an open
	// billing account, to which usage on the project is charged. False if
	// the project is associated with a closed billing account, or no
	// billing account at all, and therefore cannot use paid services. This
	// field is read-only.
	BillingEnabled bool `json:"billingEnabled,omitempty"`

	// Name: The resource name for the `ProjectBillingInfo`; has the form
	// `projects/{project_id}/billingInfo`. For example, the resource name
	// for the billing information for project `tokyo-rain-123` would be
	// `projects/tokyo-rain-123/billingInfo`. This field is read-only.
	Name string `json:"name,omitempty"`

	// ProjectId: The ID of the project that this `ProjectBillingInfo`
	// represents, such as `tokyo-rain-123`. This is a convenience field so
	// that you don't need to parse the `name` field to obtain a project ID.
	// This field is read-only.
	ProjectId string `json:"projectId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BillingAccountName")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BillingAccountName") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ProjectBillingInfo) MarshalJSON() ([]byte, error) {
	type noMethod ProjectBillingInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "cloudbilling.billingAccounts.get":

type BillingAccountsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets information about a billing account. The current
// authenticated user must be an [owner of the billing
// account](https://support.google.com/cloud/answer/4430947).
func (r *BillingAccountsService) Get(name string) *BillingAccountsGetCall {
	c := &BillingAccountsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BillingAccountsGetCall) Fields(s ...googleapi.Field) *BillingAccountsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BillingAccountsGetCall) IfNoneMatch(entityTag string) *BillingAccountsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BillingAccountsGetCall) Context(ctx context.Context) *BillingAccountsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BillingAccountsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BillingAccountsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "cloudbilling.billingAccounts.get" call.
// Exactly one of *BillingAccount or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *BillingAccount.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BillingAccountsGetCall) Do(opts ...googleapi.CallOption) (*BillingAccount, error) {
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
	ret := &BillingAccount{
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
	//   "description": "Gets information about a billing account. The current authenticated user must be an [owner of the billing account](https://support.google.com/cloud/answer/4430947).",
	//   "httpMethod": "GET",
	//   "id": "cloudbilling.billingAccounts.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The resource name of the billing account to retrieve. For example, `billingAccounts/012345-567890-ABCDEF`.",
	//       "location": "path",
	//       "pattern": "^billingAccounts/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/{+name}",
	//   "response": {
	//     "$ref": "BillingAccount"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "cloudbilling.billingAccounts.list":

type BillingAccountsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the billing accounts that the current authenticated user
// [owns](https://support.google.com/cloud/answer/4430947).
func (r *BillingAccountsService) List() *BillingAccountsListCall {
	c := &BillingAccountsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// PageSize sets the optional parameter "pageSize": Requested page size.
// The maximum page size is 100; this is also the default.
func (c *BillingAccountsListCall) PageSize(pageSize int64) *BillingAccountsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": A token
// identifying a page of results to return. This should be a
// `next_page_token` value returned from a previous
// `ListBillingAccounts` call. If unspecified, the first page of results
// is returned.
func (c *BillingAccountsListCall) PageToken(pageToken string) *BillingAccountsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BillingAccountsListCall) Fields(s ...googleapi.Field) *BillingAccountsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BillingAccountsListCall) IfNoneMatch(entityTag string) *BillingAccountsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BillingAccountsListCall) Context(ctx context.Context) *BillingAccountsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BillingAccountsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BillingAccountsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/billingAccounts")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "cloudbilling.billingAccounts.list" call.
// Exactly one of *ListBillingAccountsResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ListBillingAccountsResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BillingAccountsListCall) Do(opts ...googleapi.CallOption) (*ListBillingAccountsResponse, error) {
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
	ret := &ListBillingAccountsResponse{
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
	//   "description": "Lists the billing accounts that the current authenticated user [owns](https://support.google.com/cloud/answer/4430947).",
	//   "httpMethod": "GET",
	//   "id": "cloudbilling.billingAccounts.list",
	//   "parameters": {
	//     "pageSize": {
	//       "description": "Requested page size. The maximum page size is 100; this is also the default.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "A token identifying a page of results to return. This should be a `next_page_token` value returned from a previous `ListBillingAccounts` call. If unspecified, the first page of results is returned.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/billingAccounts",
	//   "response": {
	//     "$ref": "ListBillingAccountsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *BillingAccountsListCall) Pages(ctx context.Context, f func(*ListBillingAccountsResponse) error) error {
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

// method id "cloudbilling.billingAccounts.projects.list":

type BillingAccountsProjectsListCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the projects associated with a billing account. The
// current authenticated user must be an [owner of the billing
// account](https://support.google.com/cloud/answer/4430947).
func (r *BillingAccountsProjectsService) List(name string) *BillingAccountsProjectsListCall {
	c := &BillingAccountsProjectsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// PageSize sets the optional parameter "pageSize": Requested page size.
// The maximum page size is 100; this is also the default.
func (c *BillingAccountsProjectsListCall) PageSize(pageSize int64) *BillingAccountsProjectsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": A token
// identifying a page of results to be returned. This should be a
// `next_page_token` value returned from a previous
// `ListProjectBillingInfo` call. If unspecified, the first page of
// results is returned.
func (c *BillingAccountsProjectsListCall) PageToken(pageToken string) *BillingAccountsProjectsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BillingAccountsProjectsListCall) Fields(s ...googleapi.Field) *BillingAccountsProjectsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BillingAccountsProjectsListCall) IfNoneMatch(entityTag string) *BillingAccountsProjectsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BillingAccountsProjectsListCall) Context(ctx context.Context) *BillingAccountsProjectsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BillingAccountsProjectsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BillingAccountsProjectsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/{+name}/projects")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "cloudbilling.billingAccounts.projects.list" call.
// Exactly one of *ListProjectBillingInfoResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ListProjectBillingInfoResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BillingAccountsProjectsListCall) Do(opts ...googleapi.CallOption) (*ListProjectBillingInfoResponse, error) {
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
	ret := &ListProjectBillingInfoResponse{
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
	//   "description": "Lists the projects associated with a billing account. The current authenticated user must be an [owner of the billing account](https://support.google.com/cloud/answer/4430947).",
	//   "httpMethod": "GET",
	//   "id": "cloudbilling.billingAccounts.projects.list",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The resource name of the billing account associated with the projects that you want to list. For example, `billingAccounts/012345-567890-ABCDEF`.",
	//       "location": "path",
	//       "pattern": "^billingAccounts/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Requested page size. The maximum page size is 100; this is also the default.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "A token identifying a page of results to be returned. This should be a `next_page_token` value returned from a previous `ListProjectBillingInfo` call. If unspecified, the first page of results is returned.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/{+name}/projects",
	//   "response": {
	//     "$ref": "ListProjectBillingInfoResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *BillingAccountsProjectsListCall) Pages(ctx context.Context, f func(*ListProjectBillingInfoResponse) error) error {
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

// method id "cloudbilling.projects.getBillingInfo":

type ProjectsGetBillingInfoCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetBillingInfo: Gets the billing information for a project. The
// current authenticated user must have [permission to view the
// project](https://cloud.google.com/docs/permissions-overview#h.bgs0oxof
// vnoo ).
func (r *ProjectsService) GetBillingInfo(name string) *ProjectsGetBillingInfoCall {
	c := &ProjectsGetBillingInfoCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsGetBillingInfoCall) Fields(s ...googleapi.Field) *ProjectsGetBillingInfoCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsGetBillingInfoCall) IfNoneMatch(entityTag string) *ProjectsGetBillingInfoCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsGetBillingInfoCall) Context(ctx context.Context) *ProjectsGetBillingInfoCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsGetBillingInfoCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsGetBillingInfoCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/{+name}/billingInfo")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "cloudbilling.projects.getBillingInfo" call.
// Exactly one of *ProjectBillingInfo or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ProjectBillingInfo.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsGetBillingInfoCall) Do(opts ...googleapi.CallOption) (*ProjectBillingInfo, error) {
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
	ret := &ProjectBillingInfo{
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
	//   "description": "Gets the billing information for a project. The current authenticated user must have [permission to view the project](https://cloud.google.com/docs/permissions-overview#h.bgs0oxofvnoo ).",
	//   "httpMethod": "GET",
	//   "id": "cloudbilling.projects.getBillingInfo",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The resource name of the project for which billing information is retrieved. For example, `projects/tokyo-rain-123`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/{+name}/billingInfo",
	//   "response": {
	//     "$ref": "ProjectBillingInfo"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "cloudbilling.projects.updateBillingInfo":

type ProjectsUpdateBillingInfoCall struct {
	s                  *Service
	name               string
	projectbillinginfo *ProjectBillingInfo
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// UpdateBillingInfo: Sets or updates the billing account associated
// with a project. You specify the new billing account by setting the
// `billing_account_name` in the `ProjectBillingInfo` resource to the
// resource name of a billing account. Associating a project with an
// open billing account enables billing on the project and allows
// charges for resource usage. If the project already had a billing
// account, this method changes the billing account used for resource
// usage charges. *Note:* Incurred charges that have not yet been
// reported in the transaction history of the Google Developers Console
// may be billed to the new billing account, even if the charge occurred
// before the new billing account was assigned to the project. The
// current authenticated user must have ownership privileges for both
// the
// [project](https://cloud.google.com/docs/permissions-overview#h.bgs0oxo
// fvnoo ) and the [billing
// account](https://support.google.com/cloud/answer/4430947). You can
// disable billing on the project by setting the `billing_account_name`
// field to empty. This action disassociates the current billing account
// from the project. Any billable activity of your in-use services will
// stop, and your application could stop functioning as expected. Any
// unbilled charges to date will be billed to the previously associated
// account. The current authenticated user must be either an owner of
// the project or an owner of the billing account for the project. Note
// that associating a project with a *closed* billing account will have
// much the same effect as disabling billing on the project: any paid
// resources used by the project will be shut down. Thus, unless you
// wish to disable billing, you should always call this method with the
// name of an *open* billing account.
func (r *ProjectsService) UpdateBillingInfo(name string, projectbillinginfo *ProjectBillingInfo) *ProjectsUpdateBillingInfoCall {
	c := &ProjectsUpdateBillingInfoCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.projectbillinginfo = projectbillinginfo
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsUpdateBillingInfoCall) Fields(s ...googleapi.Field) *ProjectsUpdateBillingInfoCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsUpdateBillingInfoCall) Context(ctx context.Context) *ProjectsUpdateBillingInfoCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsUpdateBillingInfoCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsUpdateBillingInfoCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.projectbillinginfo)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/{+name}/billingInfo")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "cloudbilling.projects.updateBillingInfo" call.
// Exactly one of *ProjectBillingInfo or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ProjectBillingInfo.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsUpdateBillingInfoCall) Do(opts ...googleapi.CallOption) (*ProjectBillingInfo, error) {
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
	ret := &ProjectBillingInfo{
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
	//   "description": "Sets or updates the billing account associated with a project. You specify the new billing account by setting the `billing_account_name` in the `ProjectBillingInfo` resource to the resource name of a billing account. Associating a project with an open billing account enables billing on the project and allows charges for resource usage. If the project already had a billing account, this method changes the billing account used for resource usage charges. *Note:* Incurred charges that have not yet been reported in the transaction history of the Google Developers Console may be billed to the new billing account, even if the charge occurred before the new billing account was assigned to the project. The current authenticated user must have ownership privileges for both the [project](https://cloud.google.com/docs/permissions-overview#h.bgs0oxofvnoo ) and the [billing account](https://support.google.com/cloud/answer/4430947). You can disable billing on the project by setting the `billing_account_name` field to empty. This action disassociates the current billing account from the project. Any billable activity of your in-use services will stop, and your application could stop functioning as expected. Any unbilled charges to date will be billed to the previously associated account. The current authenticated user must be either an owner of the project or an owner of the billing account for the project. Note that associating a project with a *closed* billing account will have much the same effect as disabling billing on the project: any paid resources used by the project will be shut down. Thus, unless you wish to disable billing, you should always call this method with the name of an *open* billing account.",
	//   "httpMethod": "PUT",
	//   "id": "cloudbilling.projects.updateBillingInfo",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The resource name of the project associated with the billing information that you want to update. For example, `projects/tokyo-rain-123`.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/{+name}/billingInfo",
	//   "request": {
	//     "$ref": "ProjectBillingInfo"
	//   },
	//   "response": {
	//     "$ref": "ProjectBillingInfo"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

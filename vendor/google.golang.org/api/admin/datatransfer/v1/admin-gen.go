// Package admin provides access to the Admin Data Transfer API.
//
// See https://developers.google.com/admin-sdk/data-transfer/
//
// Usage example:
//
//   import "google.golang.org/api/admin/datatransfer/v1"
//   ...
//   adminService, err := admin.New(oauthHttpClient)
package admin // import "google.golang.org/api/admin/datatransfer/v1"

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

const apiId = "admin:datatransfer_v1"
const apiName = "admin"
const apiVersion = "datatransfer_v1"
const basePath = "https://www.googleapis.com/admin/datatransfer/v1/"

// OAuth2 scopes used by this API.
const (
	// View and manage data transfers between users in your organization
	AdminDatatransferScope = "https://www.googleapis.com/auth/admin.datatransfer"

	// View data transfers between users in your organization
	AdminDatatransferReadonlyScope = "https://www.googleapis.com/auth/admin.datatransfer.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Applications = NewApplicationsService(s)
	s.Transfers = NewTransfersService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Applications *ApplicationsService

	Transfers *TransfersService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewApplicationsService(s *Service) *ApplicationsService {
	rs := &ApplicationsService{s: s}
	return rs
}

type ApplicationsService struct {
	s *Service
}

func NewTransfersService(s *Service) *TransfersService {
	rs := &TransfersService{s: s}
	return rs
}

type TransfersService struct {
	s *Service
}

// Application: The JSON template for an Application resource.
type Application struct {
	// Etag: Etag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: The application's ID.
	Id int64 `json:"id,omitempty,string"`

	// Kind: Identifies the resource as a DataTransfer Application Resource.
	Kind string `json:"kind,omitempty"`

	// Name: The application's name.
	Name string `json:"name,omitempty"`

	// TransferParams: The list of all possible transfer parameters for this
	// application. These parameters can be used to select the data of the
	// user in this application to be transfered.
	TransferParams []*ApplicationTransferParam `json:"transferParams,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Etag") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Etag") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Application) MarshalJSON() ([]byte, error) {
	type noMethod Application
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ApplicationDataTransfer: Template to map fields of
// ApplicationDataTransfer resource.
type ApplicationDataTransfer struct {
	// ApplicationId: The application's ID.
	ApplicationId int64 `json:"applicationId,omitempty,string"`

	// ApplicationTransferParams: The transfer parameters for the
	// application. These parameters are used to select the data which will
	// get transfered in context of this application.
	ApplicationTransferParams []*ApplicationTransferParam `json:"applicationTransferParams,omitempty"`

	// ApplicationTransferStatus: Current status of transfer for this
	// application. (Read-only)
	ApplicationTransferStatus string `json:"applicationTransferStatus,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ApplicationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ApplicationId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ApplicationDataTransfer) MarshalJSON() ([]byte, error) {
	type noMethod ApplicationDataTransfer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ApplicationTransferParam: Template for application transfer
// parameters.
type ApplicationTransferParam struct {
	// Key: The type of the transfer parameter. eg: 'PRIVACY_LEVEL'
	Key string `json:"key,omitempty"`

	// Value: The value of the coressponding transfer parameter. eg:
	// 'PRIVATE' or 'SHARED'
	Value []string `json:"value,omitempty"`

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

func (s *ApplicationTransferParam) MarshalJSON() ([]byte, error) {
	type noMethod ApplicationTransferParam
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ApplicationsListResponse: Template for a collection of Applications.
type ApplicationsListResponse struct {
	// Applications: List of applications that support data transfer and are
	// also installed for the customer.
	Applications []*Application `json:"applications,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Identifies the resource as a collection of Applications.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Continuation token which will be used to specify next
	// page in list API.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Applications") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Applications") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ApplicationsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ApplicationsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DataTransfer: The JSON template for a DataTransfer resource.
type DataTransfer struct {
	// ApplicationDataTransfers: List of per application data transfer
	// resources. It contains data transfer details of the applications
	// associated with this transfer resource. Note that this list is also
	// used to specify the applications for which data transfer has to be
	// done at the time of the transfer resource creation.
	ApplicationDataTransfers []*ApplicationDataTransfer `json:"applicationDataTransfers,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: The transfer's ID (Read-only).
	Id string `json:"id,omitempty"`

	// Kind: Identifies the resource as a DataTransfer request.
	Kind string `json:"kind,omitempty"`

	// NewOwnerUserId: ID of the user to whom the data is being transfered.
	NewOwnerUserId string `json:"newOwnerUserId,omitempty"`

	// OldOwnerUserId: ID of the user whose data is being transfered.
	OldOwnerUserId string `json:"oldOwnerUserId,omitempty"`

	// OverallTransferStatusCode: Overall transfer status (Read-only).
	OverallTransferStatusCode string `json:"overallTransferStatusCode,omitempty"`

	// RequestTime: The time at which the data transfer was requested
	// (Read-only).
	RequestTime string `json:"requestTime,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g.
	// "ApplicationDataTransfers") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ApplicationDataTransfers")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DataTransfer) MarshalJSON() ([]byte, error) {
	type noMethod DataTransfer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DataTransfersListResponse: Template for a collection of DataTransfer
// resources.
type DataTransfersListResponse struct {
	// DataTransfers: List of data transfer requests.
	DataTransfers []*DataTransfer `json:"dataTransfers,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Identifies the resource as a collection of data transfer
	// requests.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Continuation token which will be used to specify next
	// page in list API.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DataTransfers") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataTransfers") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DataTransfersListResponse) MarshalJSON() ([]byte, error) {
	type noMethod DataTransfersListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "datatransfer.applications.get":

type ApplicationsGetCall struct {
	s             *Service
	applicationId int64
	urlParams_    gensupport.URLParams
	ifNoneMatch_  string
	ctx_          context.Context
	header_       http.Header
}

// Get: Retrieves information about an application for the given
// application ID.
func (r *ApplicationsService) Get(applicationId int64) *ApplicationsGetCall {
	c := &ApplicationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.applicationId = applicationId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ApplicationsGetCall) Fields(s ...googleapi.Field) *ApplicationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ApplicationsGetCall) IfNoneMatch(entityTag string) *ApplicationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ApplicationsGetCall) Context(ctx context.Context) *ApplicationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ApplicationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ApplicationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "applications/{applicationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"applicationId": strconv.FormatInt(c.applicationId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "datatransfer.applications.get" call.
// Exactly one of *Application or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Application.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ApplicationsGetCall) Do(opts ...googleapi.CallOption) (*Application, error) {
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
	ret := &Application{
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
	//   "description": "Retrieves information about an application for the given application ID.",
	//   "httpMethod": "GET",
	//   "id": "datatransfer.applications.get",
	//   "parameterOrder": [
	//     "applicationId"
	//   ],
	//   "parameters": {
	//     "applicationId": {
	//       "description": "ID of the application resource to be retrieved.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "applications/{applicationId}",
	//   "response": {
	//     "$ref": "Application"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.datatransfer",
	//     "https://www.googleapis.com/auth/admin.datatransfer.readonly"
	//   ]
	// }

}

// method id "datatransfer.applications.list":

type ApplicationsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the applications available for data transfer for a
// customer.
func (r *ApplicationsService) List() *ApplicationsListCall {
	c := &ApplicationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// CustomerId sets the optional parameter "customerId": Immutable ID of
// the Google Apps account.
func (c *ApplicationsListCall) CustomerId(customerId string) *ApplicationsListCall {
	c.urlParams_.Set("customerId", customerId)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 100.
func (c *ApplicationsListCall) MaxResults(maxResults int64) *ApplicationsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list.
func (c *ApplicationsListCall) PageToken(pageToken string) *ApplicationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ApplicationsListCall) Fields(s ...googleapi.Field) *ApplicationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ApplicationsListCall) IfNoneMatch(entityTag string) *ApplicationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ApplicationsListCall) Context(ctx context.Context) *ApplicationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ApplicationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ApplicationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "applications")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "datatransfer.applications.list" call.
// Exactly one of *ApplicationsListResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ApplicationsListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ApplicationsListCall) Do(opts ...googleapi.CallOption) (*ApplicationsListResponse, error) {
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
	ret := &ApplicationsListResponse{
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
	//   "description": "Lists the applications available for data transfer for a customer.",
	//   "httpMethod": "GET",
	//   "id": "datatransfer.applications.list",
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 100.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "500",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "applications",
	//   "response": {
	//     "$ref": "ApplicationsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.datatransfer",
	//     "https://www.googleapis.com/auth/admin.datatransfer.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ApplicationsListCall) Pages(ctx context.Context, f func(*ApplicationsListResponse) error) error {
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

// method id "datatransfer.transfers.get":

type TransfersGetCall struct {
	s              *Service
	dataTransferId string
	urlParams_     gensupport.URLParams
	ifNoneMatch_   string
	ctx_           context.Context
	header_        http.Header
}

// Get: Retrieves a data transfer request by its resource ID.
func (r *TransfersService) Get(dataTransferId string) *TransfersGetCall {
	c := &TransfersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.dataTransferId = dataTransferId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TransfersGetCall) Fields(s ...googleapi.Field) *TransfersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TransfersGetCall) IfNoneMatch(entityTag string) *TransfersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TransfersGetCall) Context(ctx context.Context) *TransfersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TransfersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TransfersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "transfers/{dataTransferId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"dataTransferId": c.dataTransferId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "datatransfer.transfers.get" call.
// Exactly one of *DataTransfer or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *DataTransfer.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TransfersGetCall) Do(opts ...googleapi.CallOption) (*DataTransfer, error) {
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
	ret := &DataTransfer{
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
	//   "description": "Retrieves a data transfer request by its resource ID.",
	//   "httpMethod": "GET",
	//   "id": "datatransfer.transfers.get",
	//   "parameterOrder": [
	//     "dataTransferId"
	//   ],
	//   "parameters": {
	//     "dataTransferId": {
	//       "description": "ID of the resource to be retrieved. This is returned in the response from the insert method.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "transfers/{dataTransferId}",
	//   "response": {
	//     "$ref": "DataTransfer"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.datatransfer",
	//     "https://www.googleapis.com/auth/admin.datatransfer.readonly"
	//   ]
	// }

}

// method id "datatransfer.transfers.insert":

type TransfersInsertCall struct {
	s            *Service
	datatransfer *DataTransfer
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Insert: Inserts a data transfer request.
func (r *TransfersService) Insert(datatransfer *DataTransfer) *TransfersInsertCall {
	c := &TransfersInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.datatransfer = datatransfer
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TransfersInsertCall) Fields(s ...googleapi.Field) *TransfersInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TransfersInsertCall) Context(ctx context.Context) *TransfersInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TransfersInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TransfersInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.datatransfer)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "transfers")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "datatransfer.transfers.insert" call.
// Exactly one of *DataTransfer or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *DataTransfer.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TransfersInsertCall) Do(opts ...googleapi.CallOption) (*DataTransfer, error) {
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
	ret := &DataTransfer{
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
	//   "description": "Inserts a data transfer request.",
	//   "httpMethod": "POST",
	//   "id": "datatransfer.transfers.insert",
	//   "path": "transfers",
	//   "request": {
	//     "$ref": "DataTransfer"
	//   },
	//   "response": {
	//     "$ref": "DataTransfer"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.datatransfer"
	//   ]
	// }

}

// method id "datatransfer.transfers.list":

type TransfersListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the transfers for a customer by source user, destination
// user, or status.
func (r *TransfersService) List() *TransfersListCall {
	c := &TransfersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// CustomerId sets the optional parameter "customerId": Immutable ID of
// the Google Apps account.
func (c *TransfersListCall) CustomerId(customerId string) *TransfersListCall {
	c.urlParams_.Set("customerId", customerId)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 100.
func (c *TransfersListCall) MaxResults(maxResults int64) *TransfersListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// NewOwnerUserId sets the optional parameter "newOwnerUserId":
// Destination user's profile ID.
func (c *TransfersListCall) NewOwnerUserId(newOwnerUserId string) *TransfersListCall {
	c.urlParams_.Set("newOwnerUserId", newOwnerUserId)
	return c
}

// OldOwnerUserId sets the optional parameter "oldOwnerUserId": Source
// user's profile ID.
func (c *TransfersListCall) OldOwnerUserId(oldOwnerUserId string) *TransfersListCall {
	c.urlParams_.Set("oldOwnerUserId", oldOwnerUserId)
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// the next page in the list.
func (c *TransfersListCall) PageToken(pageToken string) *TransfersListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Status sets the optional parameter "status": Status of the transfer.
func (c *TransfersListCall) Status(status string) *TransfersListCall {
	c.urlParams_.Set("status", status)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TransfersListCall) Fields(s ...googleapi.Field) *TransfersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TransfersListCall) IfNoneMatch(entityTag string) *TransfersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TransfersListCall) Context(ctx context.Context) *TransfersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TransfersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TransfersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "transfers")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "datatransfer.transfers.list" call.
// Exactly one of *DataTransfersListResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *DataTransfersListResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *TransfersListCall) Do(opts ...googleapi.CallOption) (*DataTransfersListResponse, error) {
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
	ret := &DataTransfersListResponse{
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
	//   "description": "Lists the transfers for a customer by source user, destination user, or status.",
	//   "httpMethod": "GET",
	//   "id": "datatransfer.transfers.list",
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 100.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "500",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "newOwnerUserId": {
	//       "description": "Destination user's profile ID.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "oldOwnerUserId": {
	//       "description": "Source user's profile ID.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify the next page in the list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "status": {
	//       "description": "Status of the transfer.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "transfers",
	//   "response": {
	//     "$ref": "DataTransfersListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.datatransfer",
	//     "https://www.googleapis.com/auth/admin.datatransfer.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *TransfersListCall) Pages(ctx context.Context, f func(*DataTransfersListResponse) error) error {
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

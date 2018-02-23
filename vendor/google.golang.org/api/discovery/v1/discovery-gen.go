// Package discovery provides access to the APIs Discovery Service.
//
// See https://developers.google.com/discovery/
//
// Usage example:
//
//   import "google.golang.org/api/discovery/v1"
//   ...
//   discoveryService, err := discovery.New(oauthHttpClient)
package discovery // import "google.golang.org/api/discovery/v1"

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

const apiId = "discovery:v1"
const apiName = "discovery"
const apiVersion = "v1"
const basePath = "https://www.googleapis.com/discovery/v1/"

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Apis = NewApisService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Apis *ApisService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewApisService(s *Service) *ApisService {
	rs := &ApisService{s: s}
	return rs
}

type ApisService struct {
	s *Service
}

type DirectoryList struct {
	// DiscoveryVersion: Indicate the version of the Discovery API used to
	// generate this doc.
	DiscoveryVersion string `json:"discoveryVersion,omitempty"`

	// Items: The individual directory entries. One entry per api/version
	// pair.
	Items []*DirectoryListItems `json:"items,omitempty"`

	// Kind: The kind for this response.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DiscoveryVersion") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DiscoveryVersion") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DirectoryList) MarshalJSON() ([]byte, error) {
	type noMethod DirectoryList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DirectoryListItems struct {
	// Description: The description of this API.
	Description string `json:"description,omitempty"`

	// DiscoveryLink: A link to the discovery document.
	DiscoveryLink string `json:"discoveryLink,omitempty"`

	// DiscoveryRestUrl: The URL for the discovery REST document.
	DiscoveryRestUrl string `json:"discoveryRestUrl,omitempty"`

	// DocumentationLink: A link to human readable documentation for the
	// API.
	DocumentationLink string `json:"documentationLink,omitempty"`

	// Icons: Links to 16x16 and 32x32 icons representing the API.
	Icons *DirectoryListItemsIcons `json:"icons,omitempty"`

	// Id: The id of this API.
	Id string `json:"id,omitempty"`

	// Kind: The kind for this response.
	Kind string `json:"kind,omitempty"`

	// Labels: Labels for the status of this API, such as labs or
	// deprecated.
	Labels []string `json:"labels,omitempty"`

	// Name: The name of the API.
	Name string `json:"name,omitempty"`

	// Preferred: True if this version is the preferred version to use.
	Preferred bool `json:"preferred,omitempty"`

	// Title: The title of this API.
	Title string `json:"title,omitempty"`

	// Version: The version of the API.
	Version string `json:"version,omitempty"`

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

func (s *DirectoryListItems) MarshalJSON() ([]byte, error) {
	type noMethod DirectoryListItems
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DirectoryListItemsIcons: Links to 16x16 and 32x32 icons representing
// the API.
type DirectoryListItemsIcons struct {
	// X16: The URL of the 16x16 icon.
	X16 string `json:"x16,omitempty"`

	// X32: The URL of the 32x32 icon.
	X32 string `json:"x32,omitempty"`

	// ForceSendFields is a list of field names (e.g. "X16") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "X16") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DirectoryListItemsIcons) MarshalJSON() ([]byte, error) {
	type noMethod DirectoryListItemsIcons
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type JsonSchema struct {
	// Ref: A reference to another schema. The value of this property is the
	// "id" of another schema.
	Ref string `json:"$ref,omitempty"`

	// AdditionalProperties: If this is a schema for an object, this
	// property is the schema for any additional properties with dynamic
	// keys on this object.
	AdditionalProperties *JsonSchema `json:"additionalProperties,omitempty"`

	// Annotations: Additional information about this property.
	Annotations *JsonSchemaAnnotations `json:"annotations,omitempty"`

	// Default: The default value of this property (if one exists).
	Default string `json:"default,omitempty"`

	// Description: A description of this object.
	Description string `json:"description,omitempty"`

	// Enum: Values this parameter may take (if it is an enum).
	Enum []string `json:"enum,omitempty"`

	// EnumDescriptions: The descriptions for the enums. Each position maps
	// to the corresponding value in the "enum" array.
	EnumDescriptions []string `json:"enumDescriptions,omitempty"`

	// Format: An additional regular expression or key that helps constrain
	// the value. For more details see:
	// http://tools.ietf.org/html/draft-zyp-json-schema-03#section-5.23
	Format string `json:"format,omitempty"`

	// Id: Unique identifier for this schema.
	Id string `json:"id,omitempty"`

	// Items: If this is a schema for an array, this property is the schema
	// for each element in the array.
	Items *JsonSchema `json:"items,omitempty"`

	// Location: Whether this parameter goes in the query or the path for
	// REST requests.
	Location string `json:"location,omitempty"`

	// Maximum: The maximum value of this parameter.
	Maximum string `json:"maximum,omitempty"`

	// Minimum: The minimum value of this parameter.
	Minimum string `json:"minimum,omitempty"`

	// Pattern: The regular expression this parameter must conform to. Uses
	// Java 6 regex format:
	// http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html
	Pattern string `json:"pattern,omitempty"`

	// Properties: If this is a schema for an object, list the schema for
	// each property of this object.
	Properties map[string]JsonSchema `json:"properties,omitempty"`

	// ReadOnly: The value is read-only, generated by the service. The value
	// cannot be modified by the client. If the value is included in a POST,
	// PUT, or PATCH request, it is ignored by the service.
	ReadOnly bool `json:"readOnly,omitempty"`

	// Repeated: Whether this parameter may appear multiple times.
	Repeated bool `json:"repeated,omitempty"`

	// Required: Whether the parameter is required.
	Required bool `json:"required,omitempty"`

	// Type: The value type for this schema. A list of values can be found
	// here: http://tools.ietf.org/html/draft-zyp-json-schema-03#section-5.1
	Type string `json:"type,omitempty"`

	// Variant: In a variant data type, the value of one property is used to
	// determine how to interpret the entire entity. Its value must exist in
	// a map of descriminant values to schema names.
	Variant *JsonSchemaVariant `json:"variant,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Ref") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Ref") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JsonSchema) MarshalJSON() ([]byte, error) {
	type noMethod JsonSchema
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// JsonSchemaAnnotations: Additional information about this property.
type JsonSchemaAnnotations struct {
	// Required: A list of methods for which this property is required on
	// requests.
	Required []string `json:"required,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Required") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Required") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JsonSchemaAnnotations) MarshalJSON() ([]byte, error) {
	type noMethod JsonSchemaAnnotations
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// JsonSchemaVariant: In a variant data type, the value of one property
// is used to determine how to interpret the entire entity. Its value
// must exist in a map of descriminant values to schema names.
type JsonSchemaVariant struct {
	// Discriminant: The name of the type discriminant property.
	Discriminant string `json:"discriminant,omitempty"`

	// Map: The map of discriminant value to schema to use for parsing..
	Map []*JsonSchemaVariantMap `json:"map,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Discriminant") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Discriminant") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JsonSchemaVariant) MarshalJSON() ([]byte, error) {
	type noMethod JsonSchemaVariant
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type JsonSchemaVariantMap struct {
	Ref string `json:"$ref,omitempty"`

	TypeValue string `json:"type_value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Ref") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Ref") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *JsonSchemaVariantMap) MarshalJSON() ([]byte, error) {
	type noMethod JsonSchemaVariantMap
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type RestDescription struct {
	// Auth: Authentication information.
	Auth *RestDescriptionAuth `json:"auth,omitempty"`

	// BasePath: [DEPRECATED] The base path for REST requests.
	BasePath string `json:"basePath,omitempty"`

	// BaseUrl: [DEPRECATED] The base URL for REST requests.
	BaseUrl string `json:"baseUrl,omitempty"`

	// BatchPath: The path for REST batch requests.
	BatchPath string `json:"batchPath,omitempty"`

	// CanonicalName: Indicates how the API name should be capitalized and
	// split into various parts. Useful for generating pretty class names.
	CanonicalName string `json:"canonicalName,omitempty"`

	// Description: The description of this API.
	Description string `json:"description,omitempty"`

	// DiscoveryVersion: Indicate the version of the Discovery API used to
	// generate this doc.
	DiscoveryVersion string `json:"discoveryVersion,omitempty"`

	// DocumentationLink: A link to human readable documentation for the
	// API.
	DocumentationLink string `json:"documentationLink,omitempty"`

	// Etag: The ETag for this response.
	Etag string `json:"etag,omitempty"`

	// ExponentialBackoffDefault: Enable exponential backoff for suitable
	// methods in the generated clients.
	ExponentialBackoffDefault bool `json:"exponentialBackoffDefault,omitempty"`

	// Features: A list of supported features for this API.
	Features []string `json:"features,omitempty"`

	// Icons: Links to 16x16 and 32x32 icons representing the API.
	Icons *RestDescriptionIcons `json:"icons,omitempty"`

	// Id: The ID of this API.
	Id string `json:"id,omitempty"`

	// Kind: The kind for this response.
	Kind string `json:"kind,omitempty"`

	// Labels: Labels for the status of this API, such as labs or
	// deprecated.
	Labels []string `json:"labels,omitempty"`

	// Methods: API-level methods for this API.
	Methods map[string]RestMethod `json:"methods,omitempty"`

	// Name: The name of this API.
	Name string `json:"name,omitempty"`

	// OwnerDomain: The domain of the owner of this API. Together with the
	// ownerName and a packagePath values, this can be used to generate a
	// library for this API which would have a unique fully qualified name.
	OwnerDomain string `json:"ownerDomain,omitempty"`

	// OwnerName: The name of the owner of this API. See ownerDomain.
	OwnerName string `json:"ownerName,omitempty"`

	// PackagePath: The package of the owner of this API. See ownerDomain.
	PackagePath string `json:"packagePath,omitempty"`

	// Parameters: Common parameters that apply across all apis.
	Parameters map[string]JsonSchema `json:"parameters,omitempty"`

	// Protocol: The protocol described by this document.
	Protocol string `json:"protocol,omitempty"`

	// Resources: The resources in this API.
	Resources map[string]RestResource `json:"resources,omitempty"`

	// Revision: The version of this API.
	Revision string `json:"revision,omitempty"`

	// RootUrl: The root URL under which all API services live.
	RootUrl string `json:"rootUrl,omitempty"`

	// Schemas: The schemas for this API.
	Schemas map[string]JsonSchema `json:"schemas,omitempty"`

	// ServicePath: The base path for all REST requests.
	ServicePath string `json:"servicePath,omitempty"`

	// Title: The title of this API.
	Title string `json:"title,omitempty"`

	// Version: The version of this API.
	Version string `json:"version,omitempty"`

	VersionModule bool `json:"version_module,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Auth") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Auth") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestDescription) MarshalJSON() ([]byte, error) {
	type noMethod RestDescription
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestDescriptionAuth: Authentication information.
type RestDescriptionAuth struct {
	// Oauth2: OAuth 2.0 authentication information.
	Oauth2 *RestDescriptionAuthOauth2 `json:"oauth2,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Oauth2") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Oauth2") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestDescriptionAuth) MarshalJSON() ([]byte, error) {
	type noMethod RestDescriptionAuth
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestDescriptionAuthOauth2: OAuth 2.0 authentication information.
type RestDescriptionAuthOauth2 struct {
	// Scopes: Available OAuth 2.0 scopes.
	Scopes map[string]RestDescriptionAuthOauth2Scopes `json:"scopes,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Scopes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Scopes") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestDescriptionAuthOauth2) MarshalJSON() ([]byte, error) {
	type noMethod RestDescriptionAuthOauth2
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestDescriptionAuthOauth2Scopes: The scope value.
type RestDescriptionAuthOauth2Scopes struct {
	// Description: Description of scope.
	Description string `json:"description,omitempty"`

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

func (s *RestDescriptionAuthOauth2Scopes) MarshalJSON() ([]byte, error) {
	type noMethod RestDescriptionAuthOauth2Scopes
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestDescriptionIcons: Links to 16x16 and 32x32 icons representing the
// API.
type RestDescriptionIcons struct {
	// X16: The URL of the 16x16 icon.
	X16 string `json:"x16,omitempty"`

	// X32: The URL of the 32x32 icon.
	X32 string `json:"x32,omitempty"`

	// ForceSendFields is a list of field names (e.g. "X16") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "X16") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestDescriptionIcons) MarshalJSON() ([]byte, error) {
	type noMethod RestDescriptionIcons
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type RestMethod struct {
	// Description: Description of this method.
	Description string `json:"description,omitempty"`

	// EtagRequired: Whether this method requires an ETag to be specified.
	// The ETag is sent as an HTTP If-Match or If-None-Match header.
	EtagRequired bool `json:"etagRequired,omitempty"`

	// HttpMethod: HTTP method used by this method.
	HttpMethod string `json:"httpMethod,omitempty"`

	// Id: A unique ID for this method. This property can be used to match
	// methods between different versions of Discovery.
	Id string `json:"id,omitempty"`

	// MediaUpload: Media upload parameters.
	MediaUpload *RestMethodMediaUpload `json:"mediaUpload,omitempty"`

	// ParameterOrder: Ordered list of required parameters, serves as a hint
	// to clients on how to structure their method signatures. The array is
	// ordered such that the "most-significant" parameter appears first.
	ParameterOrder []string `json:"parameterOrder,omitempty"`

	// Parameters: Details for all parameters in this method.
	Parameters map[string]JsonSchema `json:"parameters,omitempty"`

	// Path: The URI path of this REST method. Should be used in conjunction
	// with the basePath property at the api-level.
	Path string `json:"path,omitempty"`

	// Request: The schema for the request.
	Request *RestMethodRequest `json:"request,omitempty"`

	// Response: The schema for the response.
	Response *RestMethodResponse `json:"response,omitempty"`

	// Scopes: OAuth 2.0 scopes applicable to this method.
	Scopes []string `json:"scopes,omitempty"`

	// SupportsMediaDownload: Whether this method supports media downloads.
	SupportsMediaDownload bool `json:"supportsMediaDownload,omitempty"`

	// SupportsMediaUpload: Whether this method supports media uploads.
	SupportsMediaUpload bool `json:"supportsMediaUpload,omitempty"`

	// SupportsSubscription: Whether this method supports subscriptions.
	SupportsSubscription bool `json:"supportsSubscription,omitempty"`

	// UseMediaDownloadService: Indicates that downloads from this method
	// should use the download service URL (i.e. "/download"). Only applies
	// if the method supports media download.
	UseMediaDownloadService bool `json:"useMediaDownloadService,omitempty"`

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

func (s *RestMethod) MarshalJSON() ([]byte, error) {
	type noMethod RestMethod
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestMethodMediaUpload: Media upload parameters.
type RestMethodMediaUpload struct {
	// Accept: MIME Media Ranges for acceptable media uploads to this
	// method.
	Accept []string `json:"accept,omitempty"`

	// MaxSize: Maximum size of a media upload, such as "1MB", "2GB" or
	// "3TB".
	MaxSize string `json:"maxSize,omitempty"`

	// Protocols: Supported upload protocols.
	Protocols *RestMethodMediaUploadProtocols `json:"protocols,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Accept") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Accept") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestMethodMediaUpload) MarshalJSON() ([]byte, error) {
	type noMethod RestMethodMediaUpload
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestMethodMediaUploadProtocols: Supported upload protocols.
type RestMethodMediaUploadProtocols struct {
	// Resumable: Supports the Resumable Media Upload protocol.
	Resumable *RestMethodMediaUploadProtocolsResumable `json:"resumable,omitempty"`

	// Simple: Supports uploading as a single HTTP request.
	Simple *RestMethodMediaUploadProtocolsSimple `json:"simple,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Resumable") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Resumable") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestMethodMediaUploadProtocols) MarshalJSON() ([]byte, error) {
	type noMethod RestMethodMediaUploadProtocols
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestMethodMediaUploadProtocolsResumable: Supports the Resumable Media
// Upload protocol.
type RestMethodMediaUploadProtocolsResumable struct {
	// Multipart: True if this endpoint supports uploading multipart media.
	//
	// Default: true
	Multipart *bool `json:"multipart,omitempty"`

	// Path: The URI path to be used for upload. Should be used in
	// conjunction with the basePath property at the api-level.
	Path string `json:"path,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Multipart") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Multipart") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestMethodMediaUploadProtocolsResumable) MarshalJSON() ([]byte, error) {
	type noMethod RestMethodMediaUploadProtocolsResumable
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestMethodMediaUploadProtocolsSimple: Supports uploading as a single
// HTTP request.
type RestMethodMediaUploadProtocolsSimple struct {
	// Multipart: True if this endpoint supports upload multipart media.
	//
	// Default: true
	Multipart *bool `json:"multipart,omitempty"`

	// Path: The URI path to be used for upload. Should be used in
	// conjunction with the basePath property at the api-level.
	Path string `json:"path,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Multipart") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Multipart") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestMethodMediaUploadProtocolsSimple) MarshalJSON() ([]byte, error) {
	type noMethod RestMethodMediaUploadProtocolsSimple
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestMethodRequest: The schema for the request.
type RestMethodRequest struct {
	// Ref: Schema ID for the request schema.
	Ref string `json:"$ref,omitempty"`

	// ParameterName: parameter name.
	ParameterName string `json:"parameterName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Ref") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Ref") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestMethodRequest) MarshalJSON() ([]byte, error) {
	type noMethod RestMethodRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RestMethodResponse: The schema for the response.
type RestMethodResponse struct {
	// Ref: Schema ID for the response schema.
	Ref string `json:"$ref,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Ref") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Ref") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestMethodResponse) MarshalJSON() ([]byte, error) {
	type noMethod RestMethodResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type RestResource struct {
	// Methods: Methods on this resource.
	Methods map[string]RestMethod `json:"methods,omitempty"`

	// Resources: Sub-resources on this resource.
	Resources map[string]RestResource `json:"resources,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Methods") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Methods") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RestResource) MarshalJSON() ([]byte, error) {
	type noMethod RestResource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "discovery.apis.getRest":

type ApisGetRestCall struct {
	s            *Service
	api          string
	version      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetRest: Retrieve the description of a particular version of an api.
func (r *ApisService) GetRest(api string, version string) *ApisGetRestCall {
	c := &ApisGetRestCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.api = api
	c.version = version
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ApisGetRestCall) Fields(s ...googleapi.Field) *ApisGetRestCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ApisGetRestCall) IfNoneMatch(entityTag string) *ApisGetRestCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ApisGetRestCall) Context(ctx context.Context) *ApisGetRestCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ApisGetRestCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ApisGetRestCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "apis/{api}/{version}/rest")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"api":     c.api,
		"version": c.version,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "discovery.apis.getRest" call.
// Exactly one of *RestDescription or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RestDescription.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ApisGetRestCall) Do(opts ...googleapi.CallOption) (*RestDescription, error) {
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
	ret := &RestDescription{
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
	//   "description": "Retrieve the description of a particular version of an api.",
	//   "httpMethod": "GET",
	//   "id": "discovery.apis.getRest",
	//   "parameterOrder": [
	//     "api",
	//     "version"
	//   ],
	//   "parameters": {
	//     "api": {
	//       "description": "The name of the API.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "version": {
	//       "description": "The version of the API.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "apis/{api}/{version}/rest",
	//   "response": {
	//     "$ref": "RestDescription"
	//   }
	// }

}

// method id "discovery.apis.list":

type ApisListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve the list of APIs supported at this endpoint.
func (r *ApisService) List() *ApisListCall {
	c := &ApisListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Name sets the optional parameter "name": Only include APIs with the
// given name.
func (c *ApisListCall) Name(name string) *ApisListCall {
	c.urlParams_.Set("name", name)
	return c
}

// Preferred sets the optional parameter "preferred": Return only the
// preferred version of an API.
func (c *ApisListCall) Preferred(preferred bool) *ApisListCall {
	c.urlParams_.Set("preferred", fmt.Sprint(preferred))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ApisListCall) Fields(s ...googleapi.Field) *ApisListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ApisListCall) IfNoneMatch(entityTag string) *ApisListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ApisListCall) Context(ctx context.Context) *ApisListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ApisListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ApisListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "apis")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "discovery.apis.list" call.
// Exactly one of *DirectoryList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *DirectoryList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ApisListCall) Do(opts ...googleapi.CallOption) (*DirectoryList, error) {
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
	ret := &DirectoryList{
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
	//   "description": "Retrieve the list of APIs supported at this endpoint.",
	//   "httpMethod": "GET",
	//   "id": "discovery.apis.list",
	//   "parameters": {
	//     "name": {
	//       "description": "Only include APIs with the given name.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "preferred": {
	//       "default": "false",
	//       "description": "Return only the preferred version of an API.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "apis",
	//   "response": {
	//     "$ref": "DirectoryList"
	//   }
	// }

}

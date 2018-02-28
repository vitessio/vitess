// Package dns provides access to the Google Cloud DNS API.
//
// See https://developers.google.com/cloud-dns
//
// Usage example:
//
//   import "google.golang.org/api/dns/v2beta1"
//   ...
//   dnsService, err := dns.New(oauthHttpClient)
package dns // import "google.golang.org/api/dns/v2beta1"

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

const apiId = "dns:v2beta1"
const apiName = "dns"
const apiVersion = "v2beta1"
const basePath = "https://www.googleapis.com/dns/v2beta1/projects/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// View your data across Google Cloud Platform services
	CloudPlatformReadOnlyScope = "https://www.googleapis.com/auth/cloud-platform.read-only"

	// View your DNS records hosted by Google Cloud DNS
	NdevClouddnsReadonlyScope = "https://www.googleapis.com/auth/ndev.clouddns.readonly"

	// View and manage your DNS records hosted by Google Cloud DNS
	NdevClouddnsReadwriteScope = "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Changes = NewChangesService(s)
	s.DnsKeys = NewDnsKeysService(s)
	s.ManagedZoneOperations = NewManagedZoneOperationsService(s)
	s.ManagedZones = NewManagedZonesService(s)
	s.Projects = NewProjectsService(s)
	s.ResourceRecordSets = NewResourceRecordSetsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Changes *ChangesService

	DnsKeys *DnsKeysService

	ManagedZoneOperations *ManagedZoneOperationsService

	ManagedZones *ManagedZonesService

	Projects *ProjectsService

	ResourceRecordSets *ResourceRecordSetsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewChangesService(s *Service) *ChangesService {
	rs := &ChangesService{s: s}
	return rs
}

type ChangesService struct {
	s *Service
}

func NewDnsKeysService(s *Service) *DnsKeysService {
	rs := &DnsKeysService{s: s}
	return rs
}

type DnsKeysService struct {
	s *Service
}

func NewManagedZoneOperationsService(s *Service) *ManagedZoneOperationsService {
	rs := &ManagedZoneOperationsService{s: s}
	return rs
}

type ManagedZoneOperationsService struct {
	s *Service
}

func NewManagedZonesService(s *Service) *ManagedZonesService {
	rs := &ManagedZonesService{s: s}
	return rs
}

type ManagedZonesService struct {
	s *Service
}

func NewProjectsService(s *Service) *ProjectsService {
	rs := &ProjectsService{s: s}
	return rs
}

type ProjectsService struct {
	s *Service
}

func NewResourceRecordSetsService(s *Service) *ResourceRecordSetsService {
	rs := &ResourceRecordSetsService{s: s}
	return rs
}

type ResourceRecordSetsService struct {
	s *Service
}

// Change: An atomic update to a collection of ResourceRecordSets.
type Change struct {
	// Additions: Which ResourceRecordSets to add?
	Additions []*ResourceRecordSet `json:"additions,omitempty"`

	// Deletions: Which ResourceRecordSets to remove? Must match existing
	// data exactly.
	Deletions []*ResourceRecordSet `json:"deletions,omitempty"`

	// Id: Unique identifier for the resource; defined by the server (output
	// only).
	Id string `json:"id,omitempty"`

	// IsServing: If the DNS queries for the zone will be served.
	IsServing bool `json:"isServing,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#change".
	Kind string `json:"kind,omitempty"`

	// StartTime: The time that this operation was started by the server
	// (output only). This is in RFC3339 text format.
	StartTime string `json:"startTime,omitempty"`

	// Status: Status of the operation (output only).
	//
	// Possible values:
	//   "DONE"
	//   "PENDING"
	Status string `json:"status,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Additions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Additions") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Change) MarshalJSON() ([]byte, error) {
	type noMethod Change
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ChangesListResponse: The response to a request to enumerate Changes
// to a ResourceRecordSets collection.
type ChangesListResponse struct {
	// Changes: The requested changes.
	Changes []*Change `json:"changes,omitempty"`

	Header *ResponseHeader `json:"header,omitempty"`

	// Kind: Type of resource.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The presence of this field indicates that there exist
	// more results following your last page of results in pagination order.
	// To fetch them, make another list request using this value as your
	// pagination token.
	//
	// In this way you can retrieve the complete contents of even very large
	// collections one page at a time. However, if the contents of the
	// collection change between the first and last paginated list request,
	// the set of all elements returned will be an inconsistent view of the
	// collection. There is no way to retrieve a "snapshot" of collections
	// larger than the maximum page size.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Changes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Changes") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ChangesListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ChangesListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DnsKey: A DNSSEC key pair.
type DnsKey struct {
	// Algorithm: String mnemonic specifying the DNSSEC algorithm of this
	// key. Immutable after creation time.
	//
	// Possible values:
	//   "ECDSAP256SHA256"
	//   "ECDSAP384SHA384"
	//   "RSASHA1"
	//   "RSASHA256"
	//   "RSASHA512"
	Algorithm string `json:"algorithm,omitempty"`

	// CreationTime: The time that this resource was created in the control
	// plane. This is in RFC3339 text format. Output only.
	CreationTime string `json:"creationTime,omitempty"`

	// Description: A mutable string of at most 1024 characters associated
	// with this resource for the user's convenience. Has no effect on the
	// resource's function.
	Description string `json:"description,omitempty"`

	// Digests: Cryptographic hashes of the DNSKEY resource record
	// associated with this DnsKey. These digests are needed to construct a
	// DS record that points at this DNS key. Output only.
	Digests []*DnsKeyDigest `json:"digests,omitempty"`

	// Id: Unique identifier for the resource; defined by the server (output
	// only).
	Id string `json:"id,omitempty"`

	// IsActive: Active keys will be used to sign subsequent changes to the
	// ManagedZone. Inactive keys will still be present as DNSKEY Resource
	// Records for the use of resolvers validating existing signatures.
	IsActive bool `json:"isActive,omitempty"`

	// KeyLength: Length of the key in bits. Specified at creation time then
	// immutable.
	KeyLength int64 `json:"keyLength,omitempty"`

	// KeyTag: The key tag is a non-cryptographic hash of the a DNSKEY
	// resource record associated with this DnsKey. The key tag can be used
	// to identify a DNSKEY more quickly (but it is not a unique
	// identifier). In particular, the key tag is used in a parent zone's DS
	// record to point at the DNSKEY in this child ManagedZone. The key tag
	// is a number in the range [0, 65535] and the algorithm to calculate it
	// is specified in RFC4034 Appendix B. Output only.
	KeyTag int64 `json:"keyTag,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#dnsKey".
	Kind string `json:"kind,omitempty"`

	// PublicKey: Base64 encoded public half of this key. Output only.
	PublicKey string `json:"publicKey,omitempty"`

	// Type: One of "KEY_SIGNING" or "ZONE_SIGNING". Keys of type
	// KEY_SIGNING have the Secure Entry Point flag set and, when active,
	// will be used to sign only resource record sets of type DNSKEY.
	// Otherwise, the Secure Entry Point flag will be cleared and this key
	// will be used to sign only resource record sets of other types.
	// Immutable after creation time.
	//
	// Possible values:
	//   "KEY_SIGNING"
	//   "ZONE_SIGNING"
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *DnsKey) MarshalJSON() ([]byte, error) {
	type noMethod DnsKey
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DnsKeyDigest struct {
	// Digest: The base-16 encoded bytes of this digest. Suitable for use in
	// a DS resource record.
	Digest string `json:"digest,omitempty"`

	// Type: Specifies the algorithm used to calculate this digest.
	//
	// Possible values:
	//   "SHA1"
	//   "SHA256"
	//   "SHA384"
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Digest") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Digest") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DnsKeyDigest) MarshalJSON() ([]byte, error) {
	type noMethod DnsKeyDigest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DnsKeySpec: Parameters for DnsKey key generation. Used for generating
// initial keys for a new ManagedZone and as default when adding a new
// DnsKey.
type DnsKeySpec struct {
	// Algorithm: String mnemonic specifying the DNSSEC algorithm of this
	// key.
	//
	// Possible values:
	//   "ECDSAP256SHA256"
	//   "ECDSAP384SHA384"
	//   "RSASHA1"
	//   "RSASHA256"
	//   "RSASHA512"
	Algorithm string `json:"algorithm,omitempty"`

	// KeyLength: Length of the keys in bits.
	KeyLength int64 `json:"keyLength,omitempty"`

	// KeyType: One of "KEY_SIGNING" or "ZONE_SIGNING". Keys of type
	// KEY_SIGNING have the Secure Entry Point flag set and, when active,
	// will be used to sign only resource record sets of type DNSKEY.
	// Otherwise, the Secure Entry Point flag will be cleared and this key
	// will be used to sign only resource record sets of other types.
	//
	// Possible values:
	//   "KEY_SIGNING"
	//   "ZONE_SIGNING"
	KeyType string `json:"keyType,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#dnsKeySpec".
	Kind string `json:"kind,omitempty"`

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

func (s *DnsKeySpec) MarshalJSON() ([]byte, error) {
	type noMethod DnsKeySpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DnsKeysListResponse: The response to a request to enumerate DnsKeys
// in a ManagedZone.
type DnsKeysListResponse struct {
	// DnsKeys: The requested resources.
	DnsKeys []*DnsKey `json:"dnsKeys,omitempty"`

	Header *ResponseHeader `json:"header,omitempty"`

	// Kind: Type of resource.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The presence of this field indicates that there exist
	// more results following your last page of results in pagination order.
	// To fetch them, make another list request using this value as your
	// pagination token.
	//
	// In this way you can retrieve the complete contents of even very large
	// collections one page at a time. However, if the contents of the
	// collection change between the first and last paginated list request,
	// the set of all elements returned will be an inconsistent view of the
	// collection. There is no way to retrieve a "snapshot" of collections
	// larger than the maximum page size.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DnsKeys") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DnsKeys") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DnsKeysListResponse) MarshalJSON() ([]byte, error) {
	type noMethod DnsKeysListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ManagedZone: A zone is a subtree of the DNS namespace under one
// administrative responsibility. A ManagedZone is a resource that
// represents a DNS zone hosted by the Cloud DNS service.
type ManagedZone struct {
	// CreationTime: The time that this resource was created on the server.
	// This is in RFC3339 text format. Output only.
	CreationTime string `json:"creationTime,omitempty"`

	// Description: A mutable string of at most 1024 characters associated
	// with this resource for the user's convenience. Has no effect on the
	// managed zone's function.
	Description string `json:"description,omitempty"`

	// DnsName: The DNS name of this managed zone, for instance
	// "example.com.".
	DnsName string `json:"dnsName,omitempty"`

	// DnssecConfig: DNSSEC configuration.
	DnssecConfig *ManagedZoneDnsSecConfig `json:"dnssecConfig,omitempty"`

	// Id: Unique identifier for the resource; defined by the server (output
	// only)
	Id uint64 `json:"id,omitempty,string"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#managedZone".
	Kind string `json:"kind,omitempty"`

	// Name: User assigned name for this resource. Must be unique within the
	// project. The name must be 1-63 characters long, must begin with a
	// letter, end with a letter or digit, and only contain lowercase
	// letters, digits or dashes.
	Name string `json:"name,omitempty"`

	// NameServerSet: Optionally specifies the NameServerSet for this
	// ManagedZone. A NameServerSet is a set of DNS name servers that all
	// host the same ManagedZones. Most users will leave this field unset.
	NameServerSet string `json:"nameServerSet,omitempty"`

	// NameServers: Delegate your managed_zone to these virtual name
	// servers; defined by the server (output only)
	NameServers []string `json:"nameServers,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CreationTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreationTime") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ManagedZone) MarshalJSON() ([]byte, error) {
	type noMethod ManagedZone
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ManagedZoneDnsSecConfig struct {
	// DefaultKeySpecs: Specifies parameters that will be used for
	// generating initial DnsKeys for this ManagedZone. Output only while
	// state is not OFF.
	DefaultKeySpecs []*DnsKeySpec `json:"defaultKeySpecs,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#managedZoneDnsSecConfig".
	Kind string `json:"kind,omitempty"`

	// NonExistence: Specifies the mechanism used to provide authenticated
	// denial-of-existence responses. Output only while state is not OFF.
	//
	// Possible values:
	//   "NSEC"
	//   "NSEC3"
	NonExistence string `json:"nonExistence,omitempty"`

	// State: Specifies whether DNSSEC is enabled, and what mode it is in.
	//
	// Possible values:
	//   "OFF"
	//   "ON"
	//   "TRANSFER"
	State string `json:"state,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DefaultKeySpecs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DefaultKeySpecs") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ManagedZoneDnsSecConfig) MarshalJSON() ([]byte, error) {
	type noMethod ManagedZoneDnsSecConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ManagedZoneOperationsListResponse struct {
	Header *ResponseHeader `json:"header,omitempty"`

	// Kind: Type of resource.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The presence of this field indicates that there exist
	// more results following your last page of results in pagination order.
	// To fetch them, make another list request using this value as your
	// page token.
	//
	// In this way you can retrieve the complete contents of even very large
	// collections one page at a time. However, if the contents of the
	// collection change between the first and last paginated list request,
	// the set of all elements returned will be an inconsistent view of the
	// collection. There is no way to retrieve a consistent snapshot of a
	// collection larger than the maximum page size.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Operations: The operation resources.
	Operations []*Operation `json:"operations,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *ManagedZoneOperationsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ManagedZoneOperationsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ManagedZonesDeleteResponse struct {
	Header *ResponseHeader `json:"header,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *ManagedZonesDeleteResponse) MarshalJSON() ([]byte, error) {
	type noMethod ManagedZonesDeleteResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ManagedZonesListResponse struct {
	Header *ResponseHeader `json:"header,omitempty"`

	// Kind: Type of resource.
	Kind string `json:"kind,omitempty"`

	// ManagedZones: The managed zone resources.
	ManagedZones []*ManagedZone `json:"managedZones,omitempty"`

	// NextPageToken: The presence of this field indicates that there exist
	// more results following your last page of results in pagination order.
	// To fetch them, make another list request using this value as your
	// page token.
	//
	// In this way you can retrieve the complete contents of even very large
	// collections one page at a time. However, if the contents of the
	// collection change between the first and last paginated list request,
	// the set of all elements returned will be an inconsistent view of the
	// collection. There is no way to retrieve a consistent snapshot of a
	// collection larger than the maximum page size.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *ManagedZonesListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ManagedZonesListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Operation: An operation represents a successful mutation performed on
// a Cloud DNS resource. Operations provide: - An audit log of server
// resource mutations. - A way to recover/retry API calls in the case
// where the response is never received by the caller. Use the caller
// specified client_operation_id.
type Operation struct {
	// DnsKeyContext: Only populated if the operation targeted a DnsKey
	// (output only).
	DnsKeyContext *OperationDnsKeyContext `json:"dnsKeyContext,omitempty"`

	// Id: Unique identifier for the resource. This is the
	// client_operation_id if the client specified it when the mutation was
	// initiated, otherwise, it is generated by the server. The name must be
	// 1-63 characters long and match the regular expression [-a-z0-9]?
	// (output only)
	Id string `json:"id,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#operation".
	Kind string `json:"kind,omitempty"`

	// StartTime: The time that this operation was started by the server.
	// This is in RFC3339 text format (output only).
	StartTime string `json:"startTime,omitempty"`

	// Status: Status of the operation. Can be one of the following:
	// "PENDING" or "DONE" (output only).
	//
	// Possible values:
	//   "DONE"
	//   "PENDING"
	Status string `json:"status,omitempty"`

	// Type: Type of the operation. Operations include insert, update, and
	// delete (output only).
	Type string `json:"type,omitempty"`

	// User: User who requested the operation, for example:
	// user@example.com. cloud-dns-system for operations automatically done
	// by the system. (output only)
	User string `json:"user,omitempty"`

	// ZoneContext: Only populated if the operation targeted a ManagedZone
	// (output only).
	ZoneContext *OperationManagedZoneContext `json:"zoneContext,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DnsKeyContext") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DnsKeyContext") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Operation) MarshalJSON() ([]byte, error) {
	type noMethod Operation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type OperationDnsKeyContext struct {
	// NewValue: The post-operation DnsKey resource.
	NewValue *DnsKey `json:"newValue,omitempty"`

	// OldValue: The pre-operation DnsKey resource.
	OldValue *DnsKey `json:"oldValue,omitempty"`

	// ForceSendFields is a list of field names (e.g. "NewValue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NewValue") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OperationDnsKeyContext) MarshalJSON() ([]byte, error) {
	type noMethod OperationDnsKeyContext
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type OperationManagedZoneContext struct {
	// NewValue: The post-operation ManagedZone resource.
	NewValue *ManagedZone `json:"newValue,omitempty"`

	// OldValue: The pre-operation ManagedZone resource.
	OldValue *ManagedZone `json:"oldValue,omitempty"`

	// ForceSendFields is a list of field names (e.g. "NewValue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NewValue") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OperationManagedZoneContext) MarshalJSON() ([]byte, error) {
	type noMethod OperationManagedZoneContext
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Project: A project resource. The project is a top level container for
// resources including Cloud DNS ManagedZones. Projects can be created
// only in the APIs console.
type Project struct {
	// Id: User assigned unique identifier for the resource (output only).
	Id string `json:"id,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#project".
	Kind string `json:"kind,omitempty"`

	// Number: Unique numeric identifier for the resource; defined by the
	// server (output only).
	Number uint64 `json:"number,omitempty,string"`

	// Quota: Quotas assigned to this project (output only).
	Quota *Quota `json:"quota,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *Project) MarshalJSON() ([]byte, error) {
	type noMethod Project
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Quota: Limits associated with a Project.
type Quota struct {
	// DnsKeysPerManagedZone: Maximum allowed number of DnsKeys per
	// ManagedZone.
	DnsKeysPerManagedZone int64 `json:"dnsKeysPerManagedZone,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#quota".
	Kind string `json:"kind,omitempty"`

	// ManagedZones: Maximum allowed number of managed zones in the project.
	ManagedZones int64 `json:"managedZones,omitempty"`

	// ResourceRecordsPerRrset: Maximum allowed number of ResourceRecords
	// per ResourceRecordSet.
	ResourceRecordsPerRrset int64 `json:"resourceRecordsPerRrset,omitempty"`

	// RrsetAdditionsPerChange: Maximum allowed number of ResourceRecordSets
	// to add per ChangesCreateRequest.
	RrsetAdditionsPerChange int64 `json:"rrsetAdditionsPerChange,omitempty"`

	// RrsetDeletionsPerChange: Maximum allowed number of ResourceRecordSets
	// to delete per ChangesCreateRequest.
	RrsetDeletionsPerChange int64 `json:"rrsetDeletionsPerChange,omitempty"`

	// RrsetsPerManagedZone: Maximum allowed number of ResourceRecordSets
	// per zone in the project.
	RrsetsPerManagedZone int64 `json:"rrsetsPerManagedZone,omitempty"`

	// TotalRrdataSizePerChange: Maximum allowed size for total rrdata in
	// one ChangesCreateRequest in bytes.
	TotalRrdataSizePerChange int64 `json:"totalRrdataSizePerChange,omitempty"`

	// WhitelistedKeySpecs: DNSSEC algorithm and key length types that can
	// be used for DnsKeys.
	WhitelistedKeySpecs []*DnsKeySpec `json:"whitelistedKeySpecs,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "DnsKeysPerManagedZone") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DnsKeysPerManagedZone") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Quota) MarshalJSON() ([]byte, error) {
	type noMethod Quota
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResourceRecordSet: A unit of data that will be returned by the DNS
// servers.
type ResourceRecordSet struct {
	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "dns#resourceRecordSet".
	Kind string `json:"kind,omitempty"`

	// Name: For example, www.example.com.
	Name string `json:"name,omitempty"`

	// Rrdatas: As defined in RFC 1035 (section 5) and RFC 1034 (section
	// 3.6.1).
	Rrdatas []string `json:"rrdatas,omitempty"`

	// SignatureRrdatas: As defined in RFC 4034 (section 3.2).
	SignatureRrdatas []string `json:"signatureRrdatas,omitempty"`

	// Ttl: Number of seconds that this ResourceRecordSet can be cached by
	// resolvers.
	Ttl int64 `json:"ttl,omitempty"`

	// Type: The identifier of a supported record type, for example, A,
	// AAAA, MX, TXT, and so on.
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

func (s *ResourceRecordSet) MarshalJSON() ([]byte, error) {
	type noMethod ResourceRecordSet
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ResourceRecordSetsListResponse struct {
	Header *ResponseHeader `json:"header,omitempty"`

	// Kind: Type of resource.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The presence of this field indicates that there exist
	// more results following your last page of results in pagination order.
	// To fetch them, make another list request using this value as your
	// pagination token.
	//
	// In this way you can retrieve the complete contents of even very large
	// collections one page at a time. However, if the contents of the
	// collection change between the first and last paginated list request,
	// the set of all elements returned will be an inconsistent view of the
	// collection. There is no way to retrieve a consistent snapshot of a
	// collection larger than the maximum page size.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Rrsets: The resource record set resources.
	Rrsets []*ResourceRecordSet `json:"rrsets,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *ResourceRecordSetsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ResourceRecordSetsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResponseHeader: Elements common to every response.
type ResponseHeader struct {
	// OperationId: For mutating operation requests that completed
	// successfully. This is the client_operation_id if the client specified
	// it, otherwise it is generated by the server (output only).
	OperationId string `json:"operationId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OperationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OperationId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResponseHeader) MarshalJSON() ([]byte, error) {
	type noMethod ResponseHeader
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "dns.changes.create":

type ChangesCreateCall struct {
	s           *Service
	project     string
	managedZone string
	change      *Change
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Create: Atomically update the ResourceRecordSet collection.
func (r *ChangesService) Create(project string, managedZone string, change *Change) *ChangesCreateCall {
	c := &ChangesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	c.change = change
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ChangesCreateCall) ClientOperationId(clientOperationId string) *ChangesCreateCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChangesCreateCall) Fields(s ...googleapi.Field) *ChangesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChangesCreateCall) Context(ctx context.Context) *ChangesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChangesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChangesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.change)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/changes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.changes.create" call.
// Exactly one of *Change or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Change.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ChangesCreateCall) Do(opts ...googleapi.CallOption) (*Change, error) {
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
	ret := &Change{
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
	//   "description": "Atomically update the ResourceRecordSet collection.",
	//   "httpMethod": "POST",
	//   "id": "dns.changes.create",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/changes",
	//   "request": {
	//     "$ref": "Change"
	//   },
	//   "response": {
	//     "$ref": "Change"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.changes.get":

type ChangesGetCall struct {
	s            *Service
	project      string
	managedZone  string
	changeId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Fetch the representation of an existing Change.
func (r *ChangesService) Get(project string, managedZone string, changeId string) *ChangesGetCall {
	c := &ChangesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	c.changeId = changeId
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ChangesGetCall) ClientOperationId(clientOperationId string) *ChangesGetCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChangesGetCall) Fields(s ...googleapi.Field) *ChangesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ChangesGetCall) IfNoneMatch(entityTag string) *ChangesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChangesGetCall) Context(ctx context.Context) *ChangesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChangesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChangesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/changes/{changeId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
		"changeId":    c.changeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.changes.get" call.
// Exactly one of *Change or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Change.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ChangesGetCall) Do(opts ...googleapi.CallOption) (*Change, error) {
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
	ret := &Change{
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
	//   "description": "Fetch the representation of an existing Change.",
	//   "httpMethod": "GET",
	//   "id": "dns.changes.get",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone",
	//     "changeId"
	//   ],
	//   "parameters": {
	//     "changeId": {
	//       "description": "The identifier of the requested change, from a previous ResourceRecordSetsChangeResponse.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/changes/{changeId}",
	//   "response": {
	//     "$ref": "Change"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.changes.list":

type ChangesListCall struct {
	s            *Service
	project      string
	managedZone  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Enumerate Changes to a ResourceRecordSet collection.
func (r *ChangesService) List(project string, managedZone string) *ChangesListCall {
	c := &ChangesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to be returned. If unspecified, the server will decide how
// many results to return.
func (c *ChangesListCall) MaxResults(maxResults int64) *ChangesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": A tag returned by
// a previous list request that was truncated. Use this parameter to
// continue a previous list request.
func (c *ChangesListCall) PageToken(pageToken string) *ChangesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// SortBy sets the optional parameter "sortBy": Sorting criterion. The
// only supported value is change sequence.
//
// Possible values:
//   "CHANGE_SEQUENCE" (default)
func (c *ChangesListCall) SortBy(sortBy string) *ChangesListCall {
	c.urlParams_.Set("sortBy", sortBy)
	return c
}

// SortOrder sets the optional parameter "sortOrder": Sorting order
// direction: 'ascending' or 'descending'.
func (c *ChangesListCall) SortOrder(sortOrder string) *ChangesListCall {
	c.urlParams_.Set("sortOrder", sortOrder)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChangesListCall) Fields(s ...googleapi.Field) *ChangesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ChangesListCall) IfNoneMatch(entityTag string) *ChangesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChangesListCall) Context(ctx context.Context) *ChangesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChangesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChangesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/changes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.changes.list" call.
// Exactly one of *ChangesListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ChangesListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ChangesListCall) Do(opts ...googleapi.CallOption) (*ChangesListResponse, error) {
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
	ret := &ChangesListResponse{
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
	//   "description": "Enumerate Changes to a ResourceRecordSet collection.",
	//   "httpMethod": "GET",
	//   "id": "dns.changes.list",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Optional. Maximum number of results to be returned. If unspecified, the server will decide how many results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A tag returned by a previous list request that was truncated. Use this parameter to continue a previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "sortBy": {
	//       "default": "CHANGE_SEQUENCE",
	//       "description": "Sorting criterion. The only supported value is change sequence.",
	//       "enum": [
	//         "CHANGE_SEQUENCE"
	//       ],
	//       "enumDescriptions": [
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sortOrder": {
	//       "description": "Sorting order direction: 'ascending' or 'descending'.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/changes",
	//   "response": {
	//     "$ref": "ChangesListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ChangesListCall) Pages(ctx context.Context, f func(*ChangesListResponse) error) error {
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

// method id "dns.dnsKeys.get":

type DnsKeysGetCall struct {
	s            *Service
	project      string
	managedZone  string
	dnsKeyId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Fetch the representation of an existing DnsKey.
func (r *DnsKeysService) Get(project string, managedZone string, dnsKeyId string) *DnsKeysGetCall {
	c := &DnsKeysGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	c.dnsKeyId = dnsKeyId
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *DnsKeysGetCall) ClientOperationId(clientOperationId string) *DnsKeysGetCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// DigestType sets the optional parameter "digestType": An optional
// comma-separated list of digest types to compute and display for key
// signing keys. If omitted, the recommended digest type will be
// computed and displayed.
func (c *DnsKeysGetCall) DigestType(digestType string) *DnsKeysGetCall {
	c.urlParams_.Set("digestType", digestType)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DnsKeysGetCall) Fields(s ...googleapi.Field) *DnsKeysGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DnsKeysGetCall) IfNoneMatch(entityTag string) *DnsKeysGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DnsKeysGetCall) Context(ctx context.Context) *DnsKeysGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DnsKeysGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DnsKeysGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/dnsKeys/{dnsKeyId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
		"dnsKeyId":    c.dnsKeyId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.dnsKeys.get" call.
// Exactly one of *DnsKey or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *DnsKey.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *DnsKeysGetCall) Do(opts ...googleapi.CallOption) (*DnsKey, error) {
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
	ret := &DnsKey{
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
	//   "description": "Fetch the representation of an existing DnsKey.",
	//   "httpMethod": "GET",
	//   "id": "dns.dnsKeys.get",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone",
	//     "dnsKeyId"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "digestType": {
	//       "description": "An optional comma-separated list of digest types to compute and display for key signing keys. If omitted, the recommended digest type will be computed and displayed.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "dnsKeyId": {
	//       "description": "The identifier of the requested DnsKey.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/dnsKeys/{dnsKeyId}",
	//   "response": {
	//     "$ref": "DnsKey"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.dnsKeys.list":

type DnsKeysListCall struct {
	s            *Service
	project      string
	managedZone  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Enumerate DnsKeys to a ResourceRecordSet collection.
func (r *DnsKeysService) List(project string, managedZone string) *DnsKeysListCall {
	c := &DnsKeysListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	return c
}

// DigestType sets the optional parameter "digestType": An optional
// comma-separated list of digest types to compute and display for key
// signing keys. If omitted, the recommended digest type will be
// computed and displayed.
func (c *DnsKeysListCall) DigestType(digestType string) *DnsKeysListCall {
	c.urlParams_.Set("digestType", digestType)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to be returned. If unspecified, the server will decide how
// many results to return.
func (c *DnsKeysListCall) MaxResults(maxResults int64) *DnsKeysListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": A tag returned by
// a previous list request that was truncated. Use this parameter to
// continue a previous list request.
func (c *DnsKeysListCall) PageToken(pageToken string) *DnsKeysListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DnsKeysListCall) Fields(s ...googleapi.Field) *DnsKeysListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DnsKeysListCall) IfNoneMatch(entityTag string) *DnsKeysListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DnsKeysListCall) Context(ctx context.Context) *DnsKeysListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DnsKeysListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DnsKeysListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/dnsKeys")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.dnsKeys.list" call.
// Exactly one of *DnsKeysListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *DnsKeysListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *DnsKeysListCall) Do(opts ...googleapi.CallOption) (*DnsKeysListResponse, error) {
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
	ret := &DnsKeysListResponse{
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
	//   "description": "Enumerate DnsKeys to a ResourceRecordSet collection.",
	//   "httpMethod": "GET",
	//   "id": "dns.dnsKeys.list",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "digestType": {
	//       "description": "An optional comma-separated list of digest types to compute and display for key signing keys. If omitted, the recommended digest type will be computed and displayed.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Optional. Maximum number of results to be returned. If unspecified, the server will decide how many results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A tag returned by a previous list request that was truncated. Use this parameter to continue a previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/dnsKeys",
	//   "response": {
	//     "$ref": "DnsKeysListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *DnsKeysListCall) Pages(ctx context.Context, f func(*DnsKeysListResponse) error) error {
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

// method id "dns.managedZoneOperations.get":

type ManagedZoneOperationsGetCall struct {
	s            *Service
	project      string
	managedZone  string
	operation    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Fetch the representation of an existing Operation.
func (r *ManagedZoneOperationsService) Get(project string, managedZone string, operation string) *ManagedZoneOperationsGetCall {
	c := &ManagedZoneOperationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	c.operation = operation
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ManagedZoneOperationsGetCall) ClientOperationId(clientOperationId string) *ManagedZoneOperationsGetCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZoneOperationsGetCall) Fields(s ...googleapi.Field) *ManagedZoneOperationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ManagedZoneOperationsGetCall) IfNoneMatch(entityTag string) *ManagedZoneOperationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZoneOperationsGetCall) Context(ctx context.Context) *ManagedZoneOperationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZoneOperationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZoneOperationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/operations/{operation}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
		"operation":   c.operation,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZoneOperations.get" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ManagedZoneOperationsGetCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Fetch the representation of an existing Operation.",
	//   "httpMethod": "GET",
	//   "id": "dns.managedZoneOperations.get",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone",
	//     "operation"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "operation": {
	//       "description": "Identifies the operation addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/operations/{operation}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.managedZoneOperations.list":

type ManagedZoneOperationsListCall struct {
	s            *Service
	project      string
	managedZone  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Enumerate Operations for the given ManagedZone.
func (r *ManagedZoneOperationsService) List(project string, managedZone string) *ManagedZoneOperationsListCall {
	c := &ManagedZoneOperationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to be returned. If unspecified, the server will decide how
// many results to return.
func (c *ManagedZoneOperationsListCall) MaxResults(maxResults int64) *ManagedZoneOperationsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": A tag returned by
// a previous list request that was truncated. Use this parameter to
// continue a previous list request.
func (c *ManagedZoneOperationsListCall) PageToken(pageToken string) *ManagedZoneOperationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// SortBy sets the optional parameter "sortBy": Sorting criterion. The
// only supported values are START_TIME and ID.
//
// Possible values:
//   "ID"
//   "START_TIME" (default)
func (c *ManagedZoneOperationsListCall) SortBy(sortBy string) *ManagedZoneOperationsListCall {
	c.urlParams_.Set("sortBy", sortBy)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZoneOperationsListCall) Fields(s ...googleapi.Field) *ManagedZoneOperationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ManagedZoneOperationsListCall) IfNoneMatch(entityTag string) *ManagedZoneOperationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZoneOperationsListCall) Context(ctx context.Context) *ManagedZoneOperationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZoneOperationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZoneOperationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/operations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZoneOperations.list" call.
// Exactly one of *ManagedZoneOperationsListResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ManagedZoneOperationsListResponse.ServerResponse.Header or
// (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *ManagedZoneOperationsListCall) Do(opts ...googleapi.CallOption) (*ManagedZoneOperationsListResponse, error) {
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
	ret := &ManagedZoneOperationsListResponse{
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
	//   "description": "Enumerate Operations for the given ManagedZone.",
	//   "httpMethod": "GET",
	//   "id": "dns.managedZoneOperations.list",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Optional. Maximum number of results to be returned. If unspecified, the server will decide how many results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A tag returned by a previous list request that was truncated. Use this parameter to continue a previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "sortBy": {
	//       "default": "START_TIME",
	//       "description": "Sorting criterion. The only supported values are START_TIME and ID.",
	//       "enum": [
	//         "ID",
	//         "START_TIME"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/operations",
	//   "response": {
	//     "$ref": "ManagedZoneOperationsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ManagedZoneOperationsListCall) Pages(ctx context.Context, f func(*ManagedZoneOperationsListResponse) error) error {
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

// method id "dns.managedZones.create":

type ManagedZonesCreateCall struct {
	s           *Service
	project     string
	managedzone *ManagedZone
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Create: Create a new ManagedZone.
func (r *ManagedZonesService) Create(project string, managedzone *ManagedZone) *ManagedZonesCreateCall {
	c := &ManagedZonesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedzone = managedzone
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ManagedZonesCreateCall) ClientOperationId(clientOperationId string) *ManagedZonesCreateCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZonesCreateCall) Fields(s ...googleapi.Field) *ManagedZonesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZonesCreateCall) Context(ctx context.Context) *ManagedZonesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZonesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZonesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.managedzone)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project": c.project,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZones.create" call.
// Exactly one of *ManagedZone or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ManagedZone.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ManagedZonesCreateCall) Do(opts ...googleapi.CallOption) (*ManagedZone, error) {
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
	ret := &ManagedZone{
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
	//   "description": "Create a new ManagedZone.",
	//   "httpMethod": "POST",
	//   "id": "dns.managedZones.create",
	//   "parameterOrder": [
	//     "project"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones",
	//   "request": {
	//     "$ref": "ManagedZone"
	//   },
	//   "response": {
	//     "$ref": "ManagedZone"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.managedZones.delete":

type ManagedZonesDeleteCall struct {
	s           *Service
	project     string
	managedZone string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Delete: Delete a previously created ManagedZone.
func (r *ManagedZonesService) Delete(project string, managedZone string) *ManagedZonesDeleteCall {
	c := &ManagedZonesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ManagedZonesDeleteCall) ClientOperationId(clientOperationId string) *ManagedZonesDeleteCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZonesDeleteCall) Fields(s ...googleapi.Field) *ManagedZonesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZonesDeleteCall) Context(ctx context.Context) *ManagedZonesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZonesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZonesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZones.delete" call.
// Exactly one of *ManagedZonesDeleteResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ManagedZonesDeleteResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ManagedZonesDeleteCall) Do(opts ...googleapi.CallOption) (*ManagedZonesDeleteResponse, error) {
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
	ret := &ManagedZonesDeleteResponse{
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
	//   "description": "Delete a previously created ManagedZone.",
	//   "httpMethod": "DELETE",
	//   "id": "dns.managedZones.delete",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}",
	//   "response": {
	//     "$ref": "ManagedZonesDeleteResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.managedZones.get":

type ManagedZonesGetCall struct {
	s            *Service
	project      string
	managedZone  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Fetch the representation of an existing ManagedZone.
func (r *ManagedZonesService) Get(project string, managedZone string) *ManagedZonesGetCall {
	c := &ManagedZonesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ManagedZonesGetCall) ClientOperationId(clientOperationId string) *ManagedZonesGetCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZonesGetCall) Fields(s ...googleapi.Field) *ManagedZonesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ManagedZonesGetCall) IfNoneMatch(entityTag string) *ManagedZonesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZonesGetCall) Context(ctx context.Context) *ManagedZonesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZonesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZonesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZones.get" call.
// Exactly one of *ManagedZone or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ManagedZone.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ManagedZonesGetCall) Do(opts ...googleapi.CallOption) (*ManagedZone, error) {
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
	ret := &ManagedZone{
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
	//   "description": "Fetch the representation of an existing ManagedZone.",
	//   "httpMethod": "GET",
	//   "id": "dns.managedZones.get",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}",
	//   "response": {
	//     "$ref": "ManagedZone"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.managedZones.list":

type ManagedZonesListCall struct {
	s            *Service
	project      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Enumerate ManagedZones that have been created but not yet
// deleted.
func (r *ManagedZonesService) List(project string) *ManagedZonesListCall {
	c := &ManagedZonesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	return c
}

// DnsName sets the optional parameter "dnsName": Restricts the list to
// return only zones with this domain name.
func (c *ManagedZonesListCall) DnsName(dnsName string) *ManagedZonesListCall {
	c.urlParams_.Set("dnsName", dnsName)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to be returned. If unspecified, the server will decide how
// many results to return.
func (c *ManagedZonesListCall) MaxResults(maxResults int64) *ManagedZonesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": A tag returned by
// a previous list request that was truncated. Use this parameter to
// continue a previous list request.
func (c *ManagedZonesListCall) PageToken(pageToken string) *ManagedZonesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZonesListCall) Fields(s ...googleapi.Field) *ManagedZonesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ManagedZonesListCall) IfNoneMatch(entityTag string) *ManagedZonesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZonesListCall) Context(ctx context.Context) *ManagedZonesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZonesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZonesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project": c.project,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZones.list" call.
// Exactly one of *ManagedZonesListResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ManagedZonesListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ManagedZonesListCall) Do(opts ...googleapi.CallOption) (*ManagedZonesListResponse, error) {
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
	ret := &ManagedZonesListResponse{
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
	//   "description": "Enumerate ManagedZones that have been created but not yet deleted.",
	//   "httpMethod": "GET",
	//   "id": "dns.managedZones.list",
	//   "parameterOrder": [
	//     "project"
	//   ],
	//   "parameters": {
	//     "dnsName": {
	//       "description": "Restricts the list to return only zones with this domain name.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Optional. Maximum number of results to be returned. If unspecified, the server will decide how many results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A tag returned by a previous list request that was truncated. Use this parameter to continue a previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones",
	//   "response": {
	//     "$ref": "ManagedZonesListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ManagedZonesListCall) Pages(ctx context.Context, f func(*ManagedZonesListResponse) error) error {
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

// method id "dns.managedZones.patch":

type ManagedZonesPatchCall struct {
	s           *Service
	project     string
	managedZone string
	managedzone *ManagedZone
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Patch: Update an existing ManagedZone. This method supports patch
// semantics.
func (r *ManagedZonesService) Patch(project string, managedZone string, managedzone *ManagedZone) *ManagedZonesPatchCall {
	c := &ManagedZonesPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	c.managedzone = managedzone
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ManagedZonesPatchCall) ClientOperationId(clientOperationId string) *ManagedZonesPatchCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZonesPatchCall) Fields(s ...googleapi.Field) *ManagedZonesPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZonesPatchCall) Context(ctx context.Context) *ManagedZonesPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZonesPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZonesPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.managedzone)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZones.patch" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ManagedZonesPatchCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Update an existing ManagedZone. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "dns.managedZones.patch",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}",
	//   "request": {
	//     "$ref": "ManagedZone"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.managedZones.update":

type ManagedZonesUpdateCall struct {
	s           *Service
	project     string
	managedZone string
	managedzone *ManagedZone
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Update: Update an existing ManagedZone.
func (r *ManagedZonesService) Update(project string, managedZone string, managedzone *ManagedZone) *ManagedZonesUpdateCall {
	c := &ManagedZonesUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	c.managedzone = managedzone
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ManagedZonesUpdateCall) ClientOperationId(clientOperationId string) *ManagedZonesUpdateCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ManagedZonesUpdateCall) Fields(s ...googleapi.Field) *ManagedZonesUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ManagedZonesUpdateCall) Context(ctx context.Context) *ManagedZonesUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ManagedZonesUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ManagedZonesUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.managedzone)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.managedZones.update" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ManagedZonesUpdateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
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
	ret := &Operation{
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
	//   "description": "Update an existing ManagedZone.",
	//   "httpMethod": "PUT",
	//   "id": "dns.managedZones.update",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}",
	//   "request": {
	//     "$ref": "ManagedZone"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.projects.get":

type ProjectsGetCall struct {
	s            *Service
	project      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Fetch the representation of an existing Project.
func (r *ProjectsService) Get(project string) *ProjectsGetCall {
	c := &ProjectsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	return c
}

// ClientOperationId sets the optional parameter "clientOperationId":
// For mutating operation requests only. An optional identifier
// specified by the client. Must be unique for operation resources in
// the Operations collection.
func (c *ProjectsGetCall) ClientOperationId(clientOperationId string) *ProjectsGetCall {
	c.urlParams_.Set("clientOperationId", clientOperationId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsGetCall) Fields(s ...googleapi.Field) *ProjectsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsGetCall) IfNoneMatch(entityTag string) *ProjectsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsGetCall) Context(ctx context.Context) *ProjectsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project": c.project,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.projects.get" call.
// Exactly one of *Project or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Project.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProjectsGetCall) Do(opts ...googleapi.CallOption) (*Project, error) {
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
	ret := &Project{
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
	//   "description": "Fetch the representation of an existing Project.",
	//   "httpMethod": "GET",
	//   "id": "dns.projects.get",
	//   "parameterOrder": [
	//     "project"
	//   ],
	//   "parameters": {
	//     "clientOperationId": {
	//       "description": "For mutating operation requests only. An optional identifier specified by the client. Must be unique for operation resources in the Operations collection.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}",
	//   "response": {
	//     "$ref": "Project"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// method id "dns.resourceRecordSets.list":

type ResourceRecordSetsListCall struct {
	s            *Service
	project      string
	managedZone  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Enumerate ResourceRecordSets that have been created but not yet
// deleted.
func (r *ResourceRecordSetsService) List(project string, managedZone string) *ResourceRecordSetsListCall {
	c := &ResourceRecordSetsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.project = project
	c.managedZone = managedZone
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to be returned. If unspecified, the server will decide how
// many results to return.
func (c *ResourceRecordSetsListCall) MaxResults(maxResults int64) *ResourceRecordSetsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// Name sets the optional parameter "name": Restricts the list to return
// only records with this fully qualified domain name.
func (c *ResourceRecordSetsListCall) Name(name string) *ResourceRecordSetsListCall {
	c.urlParams_.Set("name", name)
	return c
}

// PageToken sets the optional parameter "pageToken": A tag returned by
// a previous list request that was truncated. Use this parameter to
// continue a previous list request.
func (c *ResourceRecordSetsListCall) PageToken(pageToken string) *ResourceRecordSetsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Type sets the optional parameter "type": Restricts the list to return
// only records of this type. If present, the "name" parameter must also
// be present.
func (c *ResourceRecordSetsListCall) Type(type_ string) *ResourceRecordSetsListCall {
	c.urlParams_.Set("type", type_)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourceRecordSetsListCall) Fields(s ...googleapi.Field) *ResourceRecordSetsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ResourceRecordSetsListCall) IfNoneMatch(entityTag string) *ResourceRecordSetsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourceRecordSetsListCall) Context(ctx context.Context) *ResourceRecordSetsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourceRecordSetsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourceRecordSetsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{project}/managedZones/{managedZone}/rrsets")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"project":     c.project,
		"managedZone": c.managedZone,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "dns.resourceRecordSets.list" call.
// Exactly one of *ResourceRecordSetsListResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ResourceRecordSetsListResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResourceRecordSetsListCall) Do(opts ...googleapi.CallOption) (*ResourceRecordSetsListResponse, error) {
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
	ret := &ResourceRecordSetsListResponse{
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
	//   "description": "Enumerate ResourceRecordSets that have been created but not yet deleted.",
	//   "httpMethod": "GET",
	//   "id": "dns.resourceRecordSets.list",
	//   "parameterOrder": [
	//     "project",
	//     "managedZone"
	//   ],
	//   "parameters": {
	//     "managedZone": {
	//       "description": "Identifies the managed zone addressed by this request. Can be the managed zone name or id.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Optional. Maximum number of results to be returned. If unspecified, the server will decide how many results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "name": {
	//       "description": "Restricts the list to return only records with this fully qualified domain name.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Optional. A tag returned by a previous list request that was truncated. Use this parameter to continue a previous list request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "project": {
	//       "description": "Identifies the project addressed by this request.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "type": {
	//       "description": "Restricts the list to return only records of this type. If present, the \"name\" parameter must also be present.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "{project}/managedZones/{managedZone}/rrsets",
	//   "response": {
	//     "$ref": "ResourceRecordSetsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform",
	//     "https://www.googleapis.com/auth/cloud-platform.read-only",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readonly",
	//     "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ResourceRecordSetsListCall) Pages(ctx context.Context, f func(*ResourceRecordSetsListResponse) error) error {
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

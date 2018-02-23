// Package proximitybeacon provides access to the Google Proximity Beacon API.
//
// See https://developers.google.com/beacons/proximity/
//
// Usage example:
//
//   import "google.golang.org/api/proximitybeacon/v1beta1"
//   ...
//   proximitybeaconService, err := proximitybeacon.New(oauthHttpClient)
package proximitybeacon // import "google.golang.org/api/proximitybeacon/v1beta1"

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

const apiId = "proximitybeacon:v1beta1"
const apiName = "proximitybeacon"
const apiVersion = "v1beta1"
const basePath = "https://proximitybeacon.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and modify your beacons
	UserlocationBeaconRegistryScope = "https://www.googleapis.com/auth/userlocation.beacon.registry"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Beaconinfo = NewBeaconinfoService(s)
	s.Beacons = NewBeaconsService(s)
	s.Namespaces = NewNamespacesService(s)
	s.V1beta1 = NewV1beta1Service(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Beaconinfo *BeaconinfoService

	Beacons *BeaconsService

	Namespaces *NamespacesService

	V1beta1 *V1beta1Service
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewBeaconinfoService(s *Service) *BeaconinfoService {
	rs := &BeaconinfoService{s: s}
	return rs
}

type BeaconinfoService struct {
	s *Service
}

func NewBeaconsService(s *Service) *BeaconsService {
	rs := &BeaconsService{s: s}
	rs.Attachments = NewBeaconsAttachmentsService(s)
	rs.Diagnostics = NewBeaconsDiagnosticsService(s)
	return rs
}

type BeaconsService struct {
	s *Service

	Attachments *BeaconsAttachmentsService

	Diagnostics *BeaconsDiagnosticsService
}

func NewBeaconsAttachmentsService(s *Service) *BeaconsAttachmentsService {
	rs := &BeaconsAttachmentsService{s: s}
	return rs
}

type BeaconsAttachmentsService struct {
	s *Service
}

func NewBeaconsDiagnosticsService(s *Service) *BeaconsDiagnosticsService {
	rs := &BeaconsDiagnosticsService{s: s}
	return rs
}

type BeaconsDiagnosticsService struct {
	s *Service
}

func NewNamespacesService(s *Service) *NamespacesService {
	rs := &NamespacesService{s: s}
	return rs
}

type NamespacesService struct {
	s *Service
}

func NewV1beta1Service(s *Service) *V1beta1Service {
	rs := &V1beta1Service{s: s}
	return rs
}

type V1beta1Service struct {
	s *Service
}

// AdvertisedId: Defines a unique identifier of a beacon as broadcast by
// the device.
type AdvertisedId struct {
	// Id: The actual beacon identifier, as broadcast by the beacon
	// hardware. Must be
	// [base64](http://tools.ietf.org/html/rfc4648#section-4) encoded in
	// HTTP requests, and will be so encoded (with padding) in responses.
	// The base64 encoding should be of the binary byte-stream and not any
	// textual (such as hex) representation thereof. Required.
	Id string `json:"id,omitempty"`

	// Type: Specifies the identifier type. Required.
	//
	// Possible values:
	//   "TYPE_UNSPECIFIED"
	//   "EDDYSTONE"
	//   "IBEACON"
	//   "ALTBEACON"
	//   "EDDYSTONE_EID"
	Type string `json:"type,omitempty"`

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

func (s *AdvertisedId) MarshalJSON() ([]byte, error) {
	type noMethod AdvertisedId
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AttachmentInfo: A subset of attachment information served via the
// `beaconinfo.getforobserved` method, used when your users encounter
// your beacons.
type AttachmentInfo struct {
	// Data: An opaque data container for client-provided data.
	Data string `json:"data,omitempty"`

	// NamespacedType: Specifies what kind of attachment this is. Tells a
	// client how to interpret the `data` field. Format is namespace/type,
	// for example scrupulous-wombat-12345/welcome-message
	NamespacedType string `json:"namespacedType,omitempty"`

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

func (s *AttachmentInfo) MarshalJSON() ([]byte, error) {
	type noMethod AttachmentInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Beacon: Details of a beacon device.
type Beacon struct {
	// AdvertisedId: The identifier of a beacon as advertised by it. This
	// field must be populated when registering. It may be empty when
	// updating a beacon record because it is ignored in updates. When
	// registering a beacon that broadcasts Eddystone-EID, this field should
	// contain a "stable" Eddystone-UID that identifies the beacon and links
	// it to its attachments. The stable Eddystone-UID is only used for
	// administering the beacon.
	AdvertisedId *AdvertisedId `json:"advertisedId,omitempty"`

	// BeaconName: Resource name of this beacon. A beacon name has the
	// format "beacons/N!beaconId" where the beaconId is the base16 ID
	// broadcast by the beacon and N is a code for the beacon's type.
	// Possible values are `3` for Eddystone, `1` for iBeacon, or `5` for
	// AltBeacon. This field must be left empty when registering. After
	// reading a beacon, clients can use the name for future operations.
	BeaconName string `json:"beaconName,omitempty"`

	// Description: Free text used to identify and describe the beacon.
	// Maximum length 140 characters. Optional.
	Description string `json:"description,omitempty"`

	// EphemeralIdRegistration: Write-only registration parameters for
	// beacons using Eddystone-EID (remotely resolved ephemeral ID) format.
	// This information will not be populated in API responses. When
	// submitting this data, the `advertised_id` field must contain an ID of
	// type Eddystone-UID. Any other ID type will result in an error.
	EphemeralIdRegistration *EphemeralIdRegistration `json:"ephemeralIdRegistration,omitempty"`

	// ExpectedStability: Expected location stability. This is set when the
	// beacon is registered or updated, not automatically detected in any
	// way. Optional.
	//
	// Possible values:
	//   "STABILITY_UNSPECIFIED"
	//   "STABLE"
	//   "PORTABLE"
	//   "MOBILE"
	//   "ROVING"
	ExpectedStability string `json:"expectedStability,omitempty"`

	// IndoorLevel: The indoor level information for this beacon, if known.
	// As returned by the Google Maps API. Optional.
	IndoorLevel *IndoorLevel `json:"indoorLevel,omitempty"`

	// LatLng: The location of the beacon, expressed as a latitude and
	// longitude pair. This location is given when the beacon is registered
	// or updated. It does not necessarily indicate the actual current
	// location of the beacon. Optional.
	LatLng *LatLng `json:"latLng,omitempty"`

	// PlaceId: The [Google Places API](/places/place-id) Place ID of the
	// place where the beacon is deployed. This is given when the beacon is
	// registered or updated, not automatically detected in any way.
	// Optional.
	PlaceId string `json:"placeId,omitempty"`

	// Properties: Properties of the beacon device, for example battery type
	// or firmware version. Optional.
	Properties map[string]string `json:"properties,omitempty"`

	// ProvisioningKey: Some beacons may require a user to provide an
	// authorization key before changing any of its configuration (e.g.
	// broadcast frames, transmit power). This field provides a place to
	// store and control access to that key. This field is populated in
	// responses to `GET /v1beta1/beacons/3!beaconId` from users with write
	// access to the given beacon. That is to say: If the user is authorized
	// to write the beacon's confidential data in the service, the service
	// considers them authorized to configure the beacon. Note that this key
	// grants nothing on the service, only on the beacon itself.
	ProvisioningKey string `json:"provisioningKey,omitempty"`

	// Status: Current status of the beacon. Required.
	//
	// Possible values:
	//   "STATUS_UNSPECIFIED"
	//   "ACTIVE"
	//   "DECOMMISSIONED"
	//   "INACTIVE"
	Status string `json:"status,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AdvertisedId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdvertisedId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Beacon) MarshalJSON() ([]byte, error) {
	type noMethod Beacon
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BeaconAttachment: Project-specific data associated with a beacon.
type BeaconAttachment struct {
	// AttachmentName: Resource name of this attachment. Attachment names
	// have the format: beacons/beacon_id/attachments/attachment_id. Leave
	// this empty on creation.
	AttachmentName string `json:"attachmentName,omitempty"`

	// Data: An opaque data container for client-provided data. Must be
	// [base64](http://tools.ietf.org/html/rfc4648#section-4) encoded in
	// HTTP requests, and will be so encoded (with padding) in responses.
	// Required.
	Data string `json:"data,omitempty"`

	// NamespacedType: Specifies what kind of attachment this is. Tells a
	// client how to interpret the `data` field. Format is namespace/type.
	// Namespace provides type separation between clients. Type describes
	// the type of `data`, for use by the client when parsing the `data`
	// field. Required.
	NamespacedType string `json:"namespacedType,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AttachmentName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AttachmentName") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *BeaconAttachment) MarshalJSON() ([]byte, error) {
	type noMethod BeaconAttachment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BeaconInfo: A subset of beacon information served via the
// `beaconinfo.getforobserved` method, which you call when users of your
// app encounter your beacons.
type BeaconInfo struct {
	// AdvertisedId: The ID advertised by the beacon.
	AdvertisedId *AdvertisedId `json:"advertisedId,omitempty"`

	// Attachments: Attachments matching the type(s) requested. May be empty
	// if no attachment types were requested, or if none matched.
	Attachments []*AttachmentInfo `json:"attachments,omitempty"`

	// BeaconName: The name under which the beacon is registered.
	BeaconName string `json:"beaconName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AdvertisedId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdvertisedId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BeaconInfo) MarshalJSON() ([]byte, error) {
	type noMethod BeaconInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Date: Represents a whole calendar date, e.g. date of birth. The time
// of day and time zone are either specified elsewhere or are not
// significant. The date is relative to the Proleptic Gregorian
// Calendar. The day may be 0 to represent a year and month where the
// day is not significant, e.g. credit card expiration date. The year
// may be 0 to represent a month and day independent of year, e.g.
// anniversary date. Related types are google.type.TimeOfDay and
// `google.protobuf.Timestamp`.
type Date struct {
	// Day: Day of month. Must be from 1 to 31 and valid for the year and
	// month, or 0 if specifying a year/month where the day is not
	// significant.
	Day int64 `json:"day,omitempty"`

	// Month: Month of year. Must be from 1 to 12.
	Month int64 `json:"month,omitempty"`

	// Year: Year of date. Must be from 1 to 9999, or 0 if specifying a date
	// without a year.
	Year int64 `json:"year,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Day") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Day") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Date) MarshalJSON() ([]byte, error) {
	type noMethod Date
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeleteAttachmentsResponse: Response for a request to delete
// attachments.
type DeleteAttachmentsResponse struct {
	// NumDeleted: The number of attachments that were deleted.
	NumDeleted int64 `json:"numDeleted,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "NumDeleted") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NumDeleted") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteAttachmentsResponse) MarshalJSON() ([]byte, error) {
	type noMethod DeleteAttachmentsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Diagnostics: Diagnostics for a single beacon.
type Diagnostics struct {
	// Alerts: An unordered list of Alerts that the beacon has.
	//
	// Possible values:
	//   "ALERT_UNSPECIFIED" - Invalid value. Should never appear.
	//   "WRONG_LOCATION" - The beacon has been reported in a location
	// different than its registered location. This may indicate that the
	// beacon has been moved. This signal is not 100% accurate, but
	// indicates that further investigation is worth while.
	//   "LOW_BATTERY" - The battery level for the beacon is low enough
	// that, given the beacon's current use, its battery will run out with
	// in the next 60 days. This indicates that the battery should be
	// replaced soon.
	Alerts []string `json:"alerts,omitempty"`

	// BeaconName: Resource name of the beacon. For Eddystone-EID beacons,
	// this may be the beacon's current EID, or the beacon's "stable"
	// Eddystone-UID.
	BeaconName string `json:"beaconName,omitempty"`

	// EstimatedLowBatteryDate: The date when the battery is expected to be
	// low. If the value is missing then there is no estimate for when the
	// battery will be low. This value is only an estimate, not an exact
	// date.
	EstimatedLowBatteryDate *Date `json:"estimatedLowBatteryDate,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alerts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alerts") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Diagnostics) MarshalJSON() ([]byte, error) {
	type noMethod Diagnostics
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Empty: A generic empty message that you can re-use to avoid defining
// duplicated empty messages in your APIs. A typical example is to use
// it as the request or the response type of an API method. For
// instance: service Foo { rpc Bar(google.protobuf.Empty) returns
// (google.protobuf.Empty); } The JSON representation for `Empty` is
// empty JSON object `{}`.
type Empty struct {
	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`
}

// EphemeralIdRegistration: Write-only registration parameters for
// beacons using Eddystone-EID format. Two ways of securely registering
// an Eddystone-EID beacon with the service are supported: 1. Perform an
// ECDH key exchange via this API, including a previous call to `GET
// /v1beta1/eidparams`. In this case the fields `beacon_ecdh_public_key`
// and `service_ecdh_public_key` should be populated and
// `beacon_identity_key` should not be populated. This method ensures
// that only the two parties in the ECDH key exchange can compute the
// identity key, which becomes a secret between them. 2. Derive or
// obtain the beacon's identity key via other secure means (perhaps an
// ECDH key exchange between the beacon and a mobile device or any other
// secure method), and then submit the resulting identity key to the
// service. In this case `beacon_identity_key` field should be
// populated, and neither of `beacon_ecdh_public_key` nor
// `service_ecdh_public_key` fields should be. The security of this
// method depends on how securely the parties involved (in particular
// the bluetooth client) handle the identity key, and obviously on how
// securely the identity key was generated. See [the Eddystone
// specification](https://github.com/google/eddystone/tree/master/eddysto
// ne-eid) at GitHub.
type EphemeralIdRegistration struct {
	// BeaconEcdhPublicKey: The beacon's public key used for the Elliptic
	// curve Diffie-Hellman key exchange. When this field is populated,
	// `service_ecdh_public_key` must also be populated, and
	// `beacon_identity_key` must not be.
	BeaconEcdhPublicKey string `json:"beaconEcdhPublicKey,omitempty"`

	// BeaconIdentityKey: The private key of the beacon. If this field is
	// populated, `beacon_ecdh_public_key` and `service_ecdh_public_key`
	// must not be populated.
	BeaconIdentityKey string `json:"beaconIdentityKey,omitempty"`

	// InitialClockValue: The initial clock value of the beacon. The
	// beacon's clock must have begun counting at this value immediately
	// prior to transmitting this value to the resolving service.
	// Significant delay in transmitting this value to the service risks
	// registration or resolution failures. If a value is not provided, the
	// default is zero.
	InitialClockValue uint64 `json:"initialClockValue,omitempty,string"`

	// InitialEid: An initial ephemeral ID calculated using the clock value
	// submitted as `initial_clock_value`, and the secret key generated by
	// the Diffie-Hellman key exchange using `service_ecdh_public_key` and
	// `service_ecdh_public_key`. This initial EID value will be used by the
	// service to confirm that the key exchange process was successful.
	InitialEid string `json:"initialEid,omitempty"`

	// RotationPeriodExponent: Indicates the nominal period between each
	// rotation of the beacon's ephemeral ID. "Nominal" because the beacon
	// should randomize the actual interval. See [the spec at
	// github](https://github.com/google/eddystone/tree/master/eddystone-eid)
	//  for details. This value corresponds to a power-of-two scaler on the
	// beacon's clock: when the scaler value is K, the beacon will begin
	// broadcasting a new ephemeral ID on average every 2^K seconds.
	RotationPeriodExponent int64 `json:"rotationPeriodExponent,omitempty"`

	// ServiceEcdhPublicKey: The service's public key used for the Elliptic
	// curve Diffie-Hellman key exchange. When this field is populated,
	// `beacon_ecdh_public_key` must also be populated, and
	// `beacon_identity_key` must not be.
	ServiceEcdhPublicKey string `json:"serviceEcdhPublicKey,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BeaconEcdhPublicKey")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BeaconEcdhPublicKey") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *EphemeralIdRegistration) MarshalJSON() ([]byte, error) {
	type noMethod EphemeralIdRegistration
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EphemeralIdRegistrationParams: Information a client needs to
// provision and register beacons that broadcast Eddystone-EID format
// beacon IDs, using Elliptic curve Diffie-Hellman key exchange. See
// [the Eddystone
// specification](https://github.com/google/eddystone/tree/master/eddysto
// ne-eid) at GitHub.
type EphemeralIdRegistrationParams struct {
	// MaxRotationPeriodExponent: Indicates the maximum rotation period
	// supported by the service. See
	// EddystoneEidRegistration.rotation_period_exponent
	MaxRotationPeriodExponent int64 `json:"maxRotationPeriodExponent,omitempty"`

	// MinRotationPeriodExponent: Indicates the minimum rotation period
	// supported by the service. See
	// EddystoneEidRegistration.rotation_period_exponent
	MinRotationPeriodExponent int64 `json:"minRotationPeriodExponent,omitempty"`

	// ServiceEcdhPublicKey: The beacon service's public key for use by a
	// beacon to derive its Identity Key using Elliptic Curve Diffie-Hellman
	// key exchange.
	ServiceEcdhPublicKey string `json:"serviceEcdhPublicKey,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g.
	// "MaxRotationPeriodExponent") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "MaxRotationPeriodExponent") to include in API requests with the JSON
	// null value. By default, fields with empty values are omitted from API
	// requests. However, any field with an empty value appearing in
	// NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EphemeralIdRegistrationParams) MarshalJSON() ([]byte, error) {
	type noMethod EphemeralIdRegistrationParams
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetInfoForObservedBeaconsRequest: Request for beacon and attachment
// information about beacons that a mobile client has encountered "in
// the wild".
type GetInfoForObservedBeaconsRequest struct {
	// NamespacedTypes: Specifies what kind of attachments to include in the
	// response. When given, the response will include only attachments of
	// the given types. When empty, no attachments will be returned. Must be
	// in the format namespace/type. Accepts `*` to specify all types in all
	// namespaces. Optional.
	NamespacedTypes []string `json:"namespacedTypes,omitempty"`

	// Observations: The beacons that the client has encountered. At least
	// one must be given.
	Observations []*Observation `json:"observations,omitempty"`

	// ForceSendFields is a list of field names (e.g. "NamespacedTypes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NamespacedTypes") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *GetInfoForObservedBeaconsRequest) MarshalJSON() ([]byte, error) {
	type noMethod GetInfoForObservedBeaconsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetInfoForObservedBeaconsResponse: Information about the requested
// beacons, optionally including attachment data.
type GetInfoForObservedBeaconsResponse struct {
	// Beacons: Public information about beacons. May be empty if the
	// request matched no beacons.
	Beacons []*BeaconInfo `json:"beacons,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Beacons") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Beacons") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetInfoForObservedBeaconsResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetInfoForObservedBeaconsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IndoorLevel: Indoor level, a human-readable string as returned by
// Google Maps APIs, useful to indicate which floor of a building a
// beacon is located on.
type IndoorLevel struct {
	// Name: The name of this level.
	Name string `json:"name,omitempty"`

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

func (s *IndoorLevel) MarshalJSON() ([]byte, error) {
	type noMethod IndoorLevel
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LatLng: An object representing a latitude/longitude pair. This is
// expressed as a pair of doubles representing degrees latitude and
// degrees longitude. Unless specified otherwise, this must conform to
// the WGS84 standard. Values must be within normalized ranges. Example
// of normalization code in Python: def NormalizeLongitude(longitude):
// """Wraps decimal degrees longitude to [-180.0, 180.0].""" q, r =
// divmod(longitude, 360.0) if r > 180.0 or (r == 180.0 and q <= -1.0):
// return r - 360.0 return r def NormalizeLatLng(latitude, longitude):
// """Wraps decimal degrees latitude and longitude to [-90.0, 90.0] and
// [-180.0, 180.0], respectively.""" r = latitude % 360.0 if r = 270.0:
// return r - 360, NormalizeLongitude(longitude) else: return 180 - r,
// NormalizeLongitude(longitude + 180.0) assert 180.0 ==
// NormalizeLongitude(180.0) assert -180.0 == NormalizeLongitude(-180.0)
// assert -179.0 == NormalizeLongitude(181.0) assert (0.0, 0.0) ==
// NormalizeLatLng(360.0, 0.0) assert (0.0, 0.0) ==
// NormalizeLatLng(-360.0, 0.0) assert (85.0, 180.0) ==
// NormalizeLatLng(95.0, 0.0) assert (-85.0, -170.0) ==
// NormalizeLatLng(-95.0, 10.0) assert (90.0, 10.0) ==
// NormalizeLatLng(90.0, 10.0) assert (-90.0, -10.0) ==
// NormalizeLatLng(-90.0, -10.0) assert (0.0, -170.0) ==
// NormalizeLatLng(-180.0, 10.0) assert (0.0, -170.0) ==
// NormalizeLatLng(180.0, 10.0) assert (-90.0, 10.0) ==
// NormalizeLatLng(270.0, 10.0) assert (90.0, 10.0) ==
// NormalizeLatLng(-270.0, 10.0)
type LatLng struct {
	// Latitude: The latitude in degrees. It must be in the range [-90.0,
	// +90.0].
	Latitude float64 `json:"latitude,omitempty"`

	// Longitude: The longitude in degrees. It must be in the range [-180.0,
	// +180.0].
	Longitude float64 `json:"longitude,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Latitude") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Latitude") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LatLng) MarshalJSON() ([]byte, error) {
	type noMethod LatLng
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListBeaconAttachmentsResponse: Response to ListBeaconAttachments that
// contains the requested attachments.
type ListBeaconAttachmentsResponse struct {
	// Attachments: The attachments that corresponded to the request params.
	Attachments []*BeaconAttachment `json:"attachments,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Attachments") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Attachments") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListBeaconAttachmentsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListBeaconAttachmentsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListBeaconsResponse: Response that contains list beacon results and
// pagination help.
type ListBeaconsResponse struct {
	// Beacons: The beacons that matched the search criteria.
	Beacons []*Beacon `json:"beacons,omitempty"`

	// NextPageToken: An opaque pagination token that the client may provide
	// in their next request to retrieve the next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalCount: Estimate of the total number of beacons matched by the
	// query. Higher values may be less accurate.
	TotalCount int64 `json:"totalCount,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Beacons") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Beacons") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListBeaconsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListBeaconsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListDiagnosticsResponse: Response that contains the requested
// diagnostics.
type ListDiagnosticsResponse struct {
	// Diagnostics: The diagnostics matching the given request.
	Diagnostics []*Diagnostics `json:"diagnostics,omitempty"`

	// NextPageToken: Token that can be used for pagination. Returned only
	// if the request matches more beacons than can be returned in this
	// response.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Diagnostics") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Diagnostics") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListDiagnosticsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListDiagnosticsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListNamespacesResponse: Response to ListNamespacesRequest that
// contains all the project's namespaces.
type ListNamespacesResponse struct {
	// Namespaces: The attachments that corresponded to the request params.
	Namespaces []*Namespace `json:"namespaces,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Namespaces") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Namespaces") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListNamespacesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListNamespacesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Namespace: An attachment namespace defines read and write access for
// all the attachments created under it. Each namespace is globally
// unique, and owned by one project which is the only project that can
// create attachments under it.
type Namespace struct {
	// NamespaceName: Resource name of this namespace. Namespaces names have
	// the format: namespaces/namespace.
	NamespaceName string `json:"namespaceName,omitempty"`

	// ServingVisibility: Specifies what clients may receive attachments
	// under this namespace via `beaconinfo.getforobserved`.
	//
	// Possible values:
	//   "VISIBILITY_UNSPECIFIED"
	//   "UNLISTED"
	//   "PUBLIC"
	ServingVisibility string `json:"servingVisibility,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "NamespaceName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "NamespaceName") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Namespace) MarshalJSON() ([]byte, error) {
	type noMethod Namespace
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Observation: Represents one beacon observed once.
type Observation struct {
	// AdvertisedId: The ID advertised by the beacon the client has
	// encountered. Clients may submit an Eddystone-EID `advertised_id`. If
	// the client is not authorized to resolve the given Eddystone-EID, no
	// data will be returned for that beacon. Required.
	AdvertisedId *AdvertisedId `json:"advertisedId,omitempty"`

	// Telemetry: The array of telemetry bytes received from the beacon. The
	// server is responsible for parsing it. This field may frequently be
	// empty, as with a beacon that transmits telemetry only occasionally.
	Telemetry string `json:"telemetry,omitempty"`

	// TimestampMs: Time when the beacon was observed.
	TimestampMs string `json:"timestampMs,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AdvertisedId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdvertisedId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Observation) MarshalJSON() ([]byte, error) {
	type noMethod Observation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "proximitybeacon.beaconinfo.getforobserved":

type BeaconinfoGetforobservedCall struct {
	s                                *Service
	getinfoforobservedbeaconsrequest *GetInfoForObservedBeaconsRequest
	urlParams_                       gensupport.URLParams
	ctx_                             context.Context
	header_                          http.Header
}

// Getforobserved: Given one or more beacon observations, returns any
// beacon information and attachments accessible to your application.
// Authorize by using the [API
// key](https://developers.google.com/beacons/proximity/how-tos/authorizi
// ng#APIKey) for the application.
func (r *BeaconinfoService) Getforobserved(getinfoforobservedbeaconsrequest *GetInfoForObservedBeaconsRequest) *BeaconinfoGetforobservedCall {
	c := &BeaconinfoGetforobservedCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.getinfoforobservedbeaconsrequest = getinfoforobservedbeaconsrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconinfoGetforobservedCall) Fields(s ...googleapi.Field) *BeaconinfoGetforobservedCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconinfoGetforobservedCall) Context(ctx context.Context) *BeaconinfoGetforobservedCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconinfoGetforobservedCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconinfoGetforobservedCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.getinfoforobservedbeaconsrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/beaconinfo:getforobserved")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beaconinfo.getforobserved" call.
// Exactly one of *GetInfoForObservedBeaconsResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GetInfoForObservedBeaconsResponse.ServerResponse.Header or
// (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *BeaconinfoGetforobservedCall) Do(opts ...googleapi.CallOption) (*GetInfoForObservedBeaconsResponse, error) {
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
	ret := &GetInfoForObservedBeaconsResponse{
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
	//   "description": "Given one or more beacon observations, returns any beacon information and attachments accessible to your application. Authorize by using the [API key](https://developers.google.com/beacons/proximity/how-tos/authorizing#APIKey) for the application.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beaconinfo.getforobserved",
	//   "path": "v1beta1/beaconinfo:getforobserved",
	//   "request": {
	//     "$ref": "GetInfoForObservedBeaconsRequest"
	//   },
	//   "response": {
	//     "$ref": "GetInfoForObservedBeaconsResponse"
	//   }
	// }

}

// method id "proximitybeacon.beacons.activate":

type BeaconsActivateCall struct {
	s          *Service
	beaconName string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Activate: Activates a beacon. A beacon that is active will return
// information and attachment data when queried via
// `beaconinfo.getforobserved`. Calling this method on an already active
// beacon will do nothing (but will return a successful response code).
// Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsService) Activate(beaconName string) *BeaconsActivateCall {
	c := &BeaconsActivateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the beacon to activate. If the project id is not specified then the
// project making the request is used. The project id must match the
// project that owns the beacon.
func (c *BeaconsActivateCall) ProjectId(projectId string) *BeaconsActivateCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsActivateCall) Fields(s ...googleapi.Field) *BeaconsActivateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsActivateCall) Context(ctx context.Context) *BeaconsActivateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsActivateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsActivateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}:activate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.activate" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsActivateCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Activates a beacon. A beacon that is active will return information and attachment data when queried via `beaconinfo.getforobserved`. Calling this method on an already active beacon will do nothing (but will return a successful response code). Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beacons.activate",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Beacon that should be activated. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID or the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the beacon to activate. If the project id is not specified then the project making the request is used. The project id must match the project that owns the beacon. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}:activate",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.deactivate":

type BeaconsDeactivateCall struct {
	s          *Service
	beaconName string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Deactivate: Deactivates a beacon. Once deactivated, the API will not
// return information nor attachment data for the beacon when queried
// via `beaconinfo.getforobserved`. Calling this method on an already
// inactive beacon will do nothing (but will return a successful
// response code). Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsService) Deactivate(beaconName string) *BeaconsDeactivateCall {
	c := &BeaconsDeactivateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the beacon to deactivate. If the project id is not specified then the
// project making the request is used. The project id must match the
// project that owns the beacon.
func (c *BeaconsDeactivateCall) ProjectId(projectId string) *BeaconsDeactivateCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsDeactivateCall) Fields(s ...googleapi.Field) *BeaconsDeactivateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsDeactivateCall) Context(ctx context.Context) *BeaconsDeactivateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsDeactivateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsDeactivateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}:deactivate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.deactivate" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsDeactivateCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Deactivates a beacon. Once deactivated, the API will not return information nor attachment data for the beacon when queried via `beaconinfo.getforobserved`. Calling this method on an already inactive beacon will do nothing (but will return a successful response code). Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beacons.deactivate",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Beacon that should be deactivated. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID or the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the beacon to deactivate. If the project id is not specified then the project making the request is used. The project id must match the project that owns the beacon. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}:deactivate",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.decommission":

type BeaconsDecommissionCall struct {
	s          *Service
	beaconName string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Decommission: Decommissions the specified beacon in the service. This
// beacon will no longer be returned from `beaconinfo.getforobserved`.
// This operation is permanent -- you will not be able to re-register a
// beacon with this ID again. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsService) Decommission(beaconName string) *BeaconsDecommissionCall {
	c := &BeaconsDecommissionCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the beacon to decommission. If the project id is not specified then
// the project making the request is used. The project id must match the
// project that owns the beacon.
func (c *BeaconsDecommissionCall) ProjectId(projectId string) *BeaconsDecommissionCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsDecommissionCall) Fields(s ...googleapi.Field) *BeaconsDecommissionCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsDecommissionCall) Context(ctx context.Context) *BeaconsDecommissionCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsDecommissionCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsDecommissionCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}:decommission")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.decommission" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsDecommissionCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Decommissions the specified beacon in the service. This beacon will no longer be returned from `beaconinfo.getforobserved`. This operation is permanent -- you will not be able to re-register a beacon with this ID again. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beacons.decommission",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Beacon that should be decommissioned. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID of the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the beacon to decommission. If the project id is not specified then the project making the request is used. The project id must match the project that owns the beacon. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}:decommission",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.get":

type BeaconsGetCall struct {
	s            *Service
	beaconName   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns detailed information about the specified beacon.
// Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **viewer**, **Is owner** or **Can edit**
// permissions in the Google Developers Console project. Requests may
// supply an Eddystone-EID beacon name in the form: `beacons/4!beaconId`
// where the `beaconId` is the base16 ephemeral ID broadcast by the
// beacon. The returned `Beacon` object will contain the beacon's stable
// Eddystone-UID. Clients not authorized to resolve the beacon's
// ephemeral Eddystone-EID broadcast will receive an error.
func (r *BeaconsService) Get(beaconName string) *BeaconsGetCall {
	c := &BeaconsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the beacon to request. If the project id is not specified then the
// project making the request is used. The project id must match the
// project that owns the beacon.
func (c *BeaconsGetCall) ProjectId(projectId string) *BeaconsGetCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsGetCall) Fields(s ...googleapi.Field) *BeaconsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BeaconsGetCall) IfNoneMatch(entityTag string) *BeaconsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsGetCall) Context(ctx context.Context) *BeaconsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.get" call.
// Exactly one of *Beacon or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Beacon.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsGetCall) Do(opts ...googleapi.CallOption) (*Beacon, error) {
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
	ret := &Beacon{
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
	//   "description": "Returns detailed information about the specified beacon. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **viewer**, **Is owner** or **Can edit** permissions in the Google Developers Console project. Requests may supply an Eddystone-EID beacon name in the form: `beacons/4!beaconId` where the `beaconId` is the base16 ephemeral ID broadcast by the beacon. The returned `Beacon` object will contain the beacon's stable Eddystone-UID. Clients not authorized to resolve the beacon's ephemeral Eddystone-EID broadcast will receive an error.",
	//   "httpMethod": "GET",
	//   "id": "proximitybeacon.beacons.get",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Resource name of this beacon. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID or the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the beacon to request. If the project id is not specified then the project making the request is used. The project id must match the project that owns the beacon. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}",
	//   "response": {
	//     "$ref": "Beacon"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.list":

type BeaconsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Searches the beacon registry for beacons that match the given
// search criteria. Only those beacons that the client has permission to
// list will be returned. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **viewer**, **Is owner** or **Can edit**
// permissions in the Google Developers Console project.
func (r *BeaconsService) List() *BeaconsListCall {
	c := &BeaconsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// PageSize sets the optional parameter "pageSize": The maximum number
// of records to return for this request, up to a server-defined upper
// limit.
func (c *BeaconsListCall) PageSize(pageSize int64) *BeaconsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": A pagination token
// obtained from a previous request to list beacons.
func (c *BeaconsListCall) PageToken(pageToken string) *BeaconsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ProjectId sets the optional parameter "projectId": The project id to
// list beacons under. If not present then the project credential that
// made the request is used as the project.
func (c *BeaconsListCall) ProjectId(projectId string) *BeaconsListCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Q sets the optional parameter "q": Filter query string that supports
// the following field filters: * `description:"" For example:
// `description:"Room 3" Returns beacons whose description matches
// tokens in the string "Room 3" (not necessarily that exact string).
// The string must be double-quoted. * `status:` For example:
// `status:active` Returns beacons whose status matches the given value.
// Values must be one of the Beacon.Status enum values (case
// insensitive). Accepts multiple filters which will be combined with OR
// logic. * `stability:` For example: `stability:mobile` Returns beacons
// whose expected stability matches the given value. Values must be one
// of the Beacon.Stability enum values (case insensitive). Accepts
// multiple filters which will be combined with OR logic. *
// `place_id:"" For example: `place_id:"ChIJVSZzVR8FdkgRXGmmm6SslKw="
// Returns beacons explicitly registered at the given place, expressed
// as a Place ID obtained from [Google Places API](/places/place-id).
// Does not match places inside the given place. Does not consider the
// beacon's actual location (which may be different from its registered
// place). Accepts multiple filters that will be combined with OR logic.
// The place ID must be double-quoted. * `registration_time[|=]` For
// example: `registration_time>=1433116800` Returns beacons whose
// registration time matches the given filter. Supports the operators: ,
// =. Timestamp must be expressed as an integer number of seconds since
// midnight January 1, 1970 UTC. Accepts at most two filters that will
// be combined with AND logic, to support "between" semantics. If more
// than two are supplied, the latter ones are ignored. * `lat: lng:
// radius:` For example: `lat:51.1232343 lng:-1.093852 radius:1000`
// Returns beacons whose registered location is within the given circle.
// When any of these fields are given, all are required. Latitude and
// longitude must be decimal degrees between -90.0 and 90.0 and between
// -180.0 and 180.0 respectively. Radius must be an integer number of
// meters between 10 and 1,000,000 (1000 km). * `property:"=" For
// example: `property:"battery-type=CR2032" Returns beacons which have
// a property of the given name and value. Supports multiple filters
// which will be combined with OR logic. The entire name=value string
// must be double-quoted as one string. * `attachment_type:"" For
// example: `attachment_type:"my-namespace/my-type" Returns beacons
// having at least one attachment of the given namespaced type. Supports
// "any within this namespace" via the partial wildcard syntax:
// "my-namespace/*". Supports multiple filters which will be combined
// with OR logic. The string must be double-quoted. Multiple filters on
// the same field are combined with OR logic (except registration_time
// which is combined with AND logic). Multiple filters on different
// fields are combined with AND logic. Filters should be separated by
// spaces. As with any HTTP query string parameter, the whole filter
// expression must be URL-encoded. Example REST request: `GET
// /v1beta1/beacons?q=status:active%20lat:51.123%20lng:-1.095%20radius:10
// 00`
func (c *BeaconsListCall) Q(q string) *BeaconsListCall {
	c.urlParams_.Set("q", q)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsListCall) Fields(s ...googleapi.Field) *BeaconsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BeaconsListCall) IfNoneMatch(entityTag string) *BeaconsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsListCall) Context(ctx context.Context) *BeaconsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/beacons")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.list" call.
// Exactly one of *ListBeaconsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListBeaconsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BeaconsListCall) Do(opts ...googleapi.CallOption) (*ListBeaconsResponse, error) {
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
	ret := &ListBeaconsResponse{
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
	//   "description": "Searches the beacon registry for beacons that match the given search criteria. Only those beacons that the client has permission to list will be returned. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **viewer**, **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "GET",
	//   "id": "proximitybeacon.beacons.list",
	//   "parameters": {
	//     "pageSize": {
	//       "description": "The maximum number of records to return for this request, up to a server-defined upper limit.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "A pagination token obtained from a previous request to list beacons.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id to list beacons under. If not present then the project credential that made the request is used as the project. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Filter query string that supports the following field filters: * `description:\"\"` For example: `description:\"Room 3\"` Returns beacons whose description matches tokens in the string \"Room 3\" (not necessarily that exact string). The string must be double-quoted. * `status:` For example: `status:active` Returns beacons whose status matches the given value. Values must be one of the Beacon.Status enum values (case insensitive). Accepts multiple filters which will be combined with OR logic. * `stability:` For example: `stability:mobile` Returns beacons whose expected stability matches the given value. Values must be one of the Beacon.Stability enum values (case insensitive). Accepts multiple filters which will be combined with OR logic. * `place_id:\"\"` For example: `place_id:\"ChIJVSZzVR8FdkgRXGmmm6SslKw=\"` Returns beacons explicitly registered at the given place, expressed as a Place ID obtained from [Google Places API](/places/place-id). Does not match places inside the given place. Does not consider the beacon's actual location (which may be different from its registered place). Accepts multiple filters that will be combined with OR logic. The place ID must be double-quoted. * `registration_time[|=]` For example: `registration_time\u003e=1433116800` Returns beacons whose registration time matches the given filter. Supports the operators: , =. Timestamp must be expressed as an integer number of seconds since midnight January 1, 1970 UTC. Accepts at most two filters that will be combined with AND logic, to support \"between\" semantics. If more than two are supplied, the latter ones are ignored. * `lat: lng: radius:` For example: `lat:51.1232343 lng:-1.093852 radius:1000` Returns beacons whose registered location is within the given circle. When any of these fields are given, all are required. Latitude and longitude must be decimal degrees between -90.0 and 90.0 and between -180.0 and 180.0 respectively. Radius must be an integer number of meters between 10 and 1,000,000 (1000 km). * `property:\"=\"` For example: `property:\"battery-type=CR2032\"` Returns beacons which have a property of the given name and value. Supports multiple filters which will be combined with OR logic. The entire name=value string must be double-quoted as one string. * `attachment_type:\"\"` For example: `attachment_type:\"my-namespace/my-type\"` Returns beacons having at least one attachment of the given namespaced type. Supports \"any within this namespace\" via the partial wildcard syntax: \"my-namespace/*\". Supports multiple filters which will be combined with OR logic. The string must be double-quoted. Multiple filters on the same field are combined with OR logic (except registration_time which is combined with AND logic). Multiple filters on different fields are combined with AND logic. Filters should be separated by spaces. As with any HTTP query string parameter, the whole filter expression must be URL-encoded. Example REST request: `GET /v1beta1/beacons?q=status:active%20lat:51.123%20lng:-1.095%20radius:1000`",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/beacons",
	//   "response": {
	//     "$ref": "ListBeaconsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *BeaconsListCall) Pages(ctx context.Context, f func(*ListBeaconsResponse) error) error {
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

// method id "proximitybeacon.beacons.register":

type BeaconsRegisterCall struct {
	s          *Service
	beacon     *Beacon
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Register: Registers a previously unregistered beacon given its
// `advertisedId`. These IDs are unique within the system. An ID can be
// registered only once. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsService) Register(beacon *Beacon) *BeaconsRegisterCall {
	c := &BeaconsRegisterCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beacon = beacon
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the project the beacon will be registered to. If the project id is
// not specified then the project making the request is used.
func (c *BeaconsRegisterCall) ProjectId(projectId string) *BeaconsRegisterCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsRegisterCall) Fields(s ...googleapi.Field) *BeaconsRegisterCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsRegisterCall) Context(ctx context.Context) *BeaconsRegisterCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsRegisterCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsRegisterCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.beacon)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/beacons:register")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.register" call.
// Exactly one of *Beacon or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Beacon.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsRegisterCall) Do(opts ...googleapi.CallOption) (*Beacon, error) {
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
	ret := &Beacon{
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
	//   "description": "Registers a previously unregistered beacon given its `advertisedId`. These IDs are unique within the system. An ID can be registered only once. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beacons.register",
	//   "parameters": {
	//     "projectId": {
	//       "description": "The project id of the project the beacon will be registered to. If the project id is not specified then the project making the request is used. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/beacons:register",
	//   "request": {
	//     "$ref": "Beacon"
	//   },
	//   "response": {
	//     "$ref": "Beacon"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.update":

type BeaconsUpdateCall struct {
	s          *Service
	beaconName string
	beacon     *Beacon
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates the information about the specified beacon. **Any
// field that you do not populate in the submitted beacon will be
// permanently erased**, so you should follow the "read, modify, write"
// pattern to avoid inadvertently destroying data. Changes to the beacon
// status via this method will be silently ignored. To update beacon
// status, use the separate methods on this API for activation,
// deactivation, and decommissioning. Authenticate using an [OAuth
// access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsService) Update(beaconName string, beacon *Beacon) *BeaconsUpdateCall {
	c := &BeaconsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	c.beacon = beacon
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the beacon to update. If the project id is not specified then the
// project making the request is used. The project id must match the
// project that owns the beacon.
func (c *BeaconsUpdateCall) ProjectId(projectId string) *BeaconsUpdateCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsUpdateCall) Fields(s ...googleapi.Field) *BeaconsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsUpdateCall) Context(ctx context.Context) *BeaconsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.beacon)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.update" call.
// Exactly one of *Beacon or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Beacon.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsUpdateCall) Do(opts ...googleapi.CallOption) (*Beacon, error) {
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
	ret := &Beacon{
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
	//   "description": "Updates the information about the specified beacon. **Any field that you do not populate in the submitted beacon will be permanently erased**, so you should follow the \"read, modify, write\" pattern to avoid inadvertently destroying data. Changes to the beacon status via this method will be silently ignored. To update beacon status, use the separate methods on this API for activation, deactivation, and decommissioning. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "PUT",
	//   "id": "proximitybeacon.beacons.update",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Resource name of this beacon. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone, `1` for iBeacon, or `5` for AltBeacon. This field must be left empty when registering. After reading a beacon, clients can use the name for future operations.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the beacon to update. If the project id is not specified then the project making the request is used. The project id must match the project that owns the beacon. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}",
	//   "request": {
	//     "$ref": "Beacon"
	//   },
	//   "response": {
	//     "$ref": "Beacon"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.attachments.batchDelete":

type BeaconsAttachmentsBatchDeleteCall struct {
	s          *Service
	beaconName string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// BatchDelete: Deletes multiple attachments on a given beacon. This
// operation is permanent and cannot be undone. You can optionally
// specify `namespacedType` to choose which attachments should be
// deleted. If you do not specify `namespacedType`, all your attachments
// on the given beacon will be deleted. You also may explicitly specify
// `*/*` to delete all. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsAttachmentsService) BatchDelete(beaconName string) *BeaconsAttachmentsBatchDeleteCall {
	c := &BeaconsAttachmentsBatchDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// NamespacedType sets the optional parameter "namespacedType":
// Specifies the namespace and type of attachments to delete in
// `namespace/type` format. Accepts `*/*` to specify "all types in all
// namespaces".
func (c *BeaconsAttachmentsBatchDeleteCall) NamespacedType(namespacedType string) *BeaconsAttachmentsBatchDeleteCall {
	c.urlParams_.Set("namespacedType", namespacedType)
	return c
}

// ProjectId sets the optional parameter "projectId": The project id to
// delete beacon attachments under. This field can be used when "*" is
// specified to mean all attachment namespaces. Projects may have
// multiple attachments with multiple namespaces. If "*" is specified
// and the projectId string is empty, then the project making the
// request is used.
func (c *BeaconsAttachmentsBatchDeleteCall) ProjectId(projectId string) *BeaconsAttachmentsBatchDeleteCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsAttachmentsBatchDeleteCall) Fields(s ...googleapi.Field) *BeaconsAttachmentsBatchDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsAttachmentsBatchDeleteCall) Context(ctx context.Context) *BeaconsAttachmentsBatchDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsAttachmentsBatchDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsAttachmentsBatchDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}/attachments:batchDelete")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.attachments.batchDelete" call.
// Exactly one of *DeleteAttachmentsResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *DeleteAttachmentsResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BeaconsAttachmentsBatchDeleteCall) Do(opts ...googleapi.CallOption) (*DeleteAttachmentsResponse, error) {
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
	ret := &DeleteAttachmentsResponse{
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
	//   "description": "Deletes multiple attachments on a given beacon. This operation is permanent and cannot be undone. You can optionally specify `namespacedType` to choose which attachments should be deleted. If you do not specify `namespacedType`, all your attachments on the given beacon will be deleted. You also may explicitly specify `*/*` to delete all. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beacons.attachments.batchDelete",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "The beacon whose attachments should be deleted. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID or the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "namespacedType": {
	//       "description": "Specifies the namespace and type of attachments to delete in `namespace/type` format. Accepts `*/*` to specify \"all types in all namespaces\". Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id to delete beacon attachments under. This field can be used when \"*\" is specified to mean all attachment namespaces. Projects may have multiple attachments with multiple namespaces. If \"*\" is specified and the projectId string is empty, then the project making the request is used. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}/attachments:batchDelete",
	//   "response": {
	//     "$ref": "DeleteAttachmentsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.attachments.create":

type BeaconsAttachmentsCreateCall struct {
	s                *Service
	beaconName       string
	beaconattachment *BeaconAttachment
	urlParams_       gensupport.URLParams
	ctx_             context.Context
	header_          http.Header
}

// Create: Associates the given data with the specified beacon.
// Attachment data must contain two parts:
// - A namespaced type.
// - The actual attachment data itself.  The namespaced type consists of
// two parts, the namespace and the type. The namespace must be one of
// the values returned by the `namespaces` endpoint, while the type can
// be a string of any characters except for the forward slash (`/`) up
// to 100 characters in length. Attachment data can be up to 1024 bytes
// long. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsAttachmentsService) Create(beaconName string, beaconattachment *BeaconAttachment) *BeaconsAttachmentsCreateCall {
	c := &BeaconsAttachmentsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	c.beaconattachment = beaconattachment
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the project the attachment will belong to. If the project id is not
// specified then the project making the request is used.
func (c *BeaconsAttachmentsCreateCall) ProjectId(projectId string) *BeaconsAttachmentsCreateCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsAttachmentsCreateCall) Fields(s ...googleapi.Field) *BeaconsAttachmentsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsAttachmentsCreateCall) Context(ctx context.Context) *BeaconsAttachmentsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsAttachmentsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsAttachmentsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.beaconattachment)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}/attachments")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.attachments.create" call.
// Exactly one of *BeaconAttachment or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *BeaconAttachment.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BeaconsAttachmentsCreateCall) Do(opts ...googleapi.CallOption) (*BeaconAttachment, error) {
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
	ret := &BeaconAttachment{
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
	//   "description": "Associates the given data with the specified beacon. Attachment data must contain two parts:  \n- A namespaced type. \n- The actual attachment data itself.  The namespaced type consists of two parts, the namespace and the type. The namespace must be one of the values returned by the `namespaces` endpoint, while the type can be a string of any characters except for the forward slash (`/`) up to 100 characters in length. Attachment data can be up to 1024 bytes long. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "POST",
	//   "id": "proximitybeacon.beacons.attachments.create",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Beacon on which the attachment should be created. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID or the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the project the attachment will belong to. If the project id is not specified then the project making the request is used. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}/attachments",
	//   "request": {
	//     "$ref": "BeaconAttachment"
	//   },
	//   "response": {
	//     "$ref": "BeaconAttachment"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.attachments.delete":

type BeaconsAttachmentsDeleteCall struct {
	s              *Service
	attachmentName string
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Delete: Deletes the specified attachment for the given beacon. Each
// attachment has a unique attachment name (`attachmentName`) which is
// returned when you fetch the attachment data via this API. You specify
// this with the delete request to control which attachment is removed.
// This operation cannot be undone. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **Is owner** or **Can edit** permissions in the
// Google Developers Console project.
func (r *BeaconsAttachmentsService) Delete(attachmentName string) *BeaconsAttachmentsDeleteCall {
	c := &BeaconsAttachmentsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.attachmentName = attachmentName
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the attachment to delete. If not provided, the project that is making
// the request is used.
func (c *BeaconsAttachmentsDeleteCall) ProjectId(projectId string) *BeaconsAttachmentsDeleteCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsAttachmentsDeleteCall) Fields(s ...googleapi.Field) *BeaconsAttachmentsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsAttachmentsDeleteCall) Context(ctx context.Context) *BeaconsAttachmentsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsAttachmentsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsAttachmentsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+attachmentName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"attachmentName": c.attachmentName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.attachments.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BeaconsAttachmentsDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	ret := &Empty{
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
	//   "description": "Deletes the specified attachment for the given beacon. Each attachment has a unique attachment name (`attachmentName`) which is returned when you fetch the attachment data via this API. You specify this with the delete request to control which attachment is removed. This operation cannot be undone. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "DELETE",
	//   "id": "proximitybeacon.beacons.attachments.delete",
	//   "parameterOrder": [
	//     "attachmentName"
	//   ],
	//   "parameters": {
	//     "attachmentName": {
	//       "description": "The attachment name (`attachmentName`) of the attachment to remove. For example: `beacons/3!893737abc9/attachments/c5e937-af0-494-959-ec49d12738`. For Eddystone-EID beacons, the beacon ID portion (`3!893737abc9`) may be the beacon's current EID, or its \"stable\" Eddystone-UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*/attachments/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the attachment to delete. If not provided, the project that is making the request is used. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+attachmentName}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.attachments.list":

type BeaconsAttachmentsListCall struct {
	s            *Service
	beaconName   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns the attachments for the specified beacon that match the
// specified namespaced-type pattern. To control which namespaced types
// are returned, you add the `namespacedType` query parameter to the
// request. You must either use `*/*`, to return all attachments, or the
// namespace must be one of the ones returned from the `namespaces`
// endpoint. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **viewer**, **Is owner** or **Can edit**
// permissions in the Google Developers Console project.
func (r *BeaconsAttachmentsService) List(beaconName string) *BeaconsAttachmentsListCall {
	c := &BeaconsAttachmentsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// NamespacedType sets the optional parameter "namespacedType":
// Specifies the namespace and type of attachment to include in response
// in namespace/type format. Accepts `*/*` to specify "all types in all
// namespaces".
func (c *BeaconsAttachmentsListCall) NamespacedType(namespacedType string) *BeaconsAttachmentsListCall {
	c.urlParams_.Set("namespacedType", namespacedType)
	return c
}

// ProjectId sets the optional parameter "projectId": The project id to
// list beacon attachments under. This field can be used when "*" is
// specified to mean all attachment namespaces. Projects may have
// multiple attachments with multiple namespaces. If "*" is specified
// and the projectId string is empty, then the project making the
// request is used.
func (c *BeaconsAttachmentsListCall) ProjectId(projectId string) *BeaconsAttachmentsListCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsAttachmentsListCall) Fields(s ...googleapi.Field) *BeaconsAttachmentsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BeaconsAttachmentsListCall) IfNoneMatch(entityTag string) *BeaconsAttachmentsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsAttachmentsListCall) Context(ctx context.Context) *BeaconsAttachmentsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsAttachmentsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsAttachmentsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}/attachments")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.attachments.list" call.
// Exactly one of *ListBeaconAttachmentsResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ListBeaconAttachmentsResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BeaconsAttachmentsListCall) Do(opts ...googleapi.CallOption) (*ListBeaconAttachmentsResponse, error) {
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
	ret := &ListBeaconAttachmentsResponse{
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
	//   "description": "Returns the attachments for the specified beacon that match the specified namespaced-type pattern. To control which namespaced types are returned, you add the `namespacedType` query parameter to the request. You must either use `*/*`, to return all attachments, or the namespace must be one of the ones returned from the `namespaces` endpoint. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **viewer**, **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "GET",
	//   "id": "proximitybeacon.beacons.attachments.list",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "beaconName": {
	//       "description": "Beacon whose attachments should be fetched. A beacon name has the format \"beacons/N!beaconId\" where the beaconId is the base16 ID broadcast by the beacon and N is a code for the beacon's type. Possible values are `3` for Eddystone-UID, `4` for Eddystone-EID, `1` for iBeacon, or `5` for AltBeacon. For Eddystone-EID beacons, you may use either the current EID or the beacon's \"stable\" UID. Required.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "namespacedType": {
	//       "description": "Specifies the namespace and type of attachment to include in response in namespace/type format. Accepts `*/*` to specify \"all types in all namespaces\".",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id to list beacon attachments under. This field can be used when \"*\" is specified to mean all attachment namespaces. Projects may have multiple attachments with multiple namespaces. If \"*\" is specified and the projectId string is empty, then the project making the request is used. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}/attachments",
	//   "response": {
	//     "$ref": "ListBeaconAttachmentsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.beacons.diagnostics.list":

type BeaconsDiagnosticsListCall struct {
	s            *Service
	beaconName   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List the diagnostics for a single beacon. You can also list
// diagnostics for all the beacons owned by your Google Developers
// Console project by using the beacon name `beacons/-`. Authenticate
// using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **viewer**, **Is owner** or **Can edit**
// permissions in the Google Developers Console project.
func (r *BeaconsDiagnosticsService) List(beaconName string) *BeaconsDiagnosticsListCall {
	c := &BeaconsDiagnosticsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.beaconName = beaconName
	return c
}

// AlertFilter sets the optional parameter "alertFilter": Requests only
// beacons that have the given alert. For example, to find beacons that
// have low batteries use `alert_filter=LOW_BATTERY`.
//
// Possible values:
//   "ALERT_UNSPECIFIED"
//   "WRONG_LOCATION"
//   "LOW_BATTERY"
func (c *BeaconsDiagnosticsListCall) AlertFilter(alertFilter string) *BeaconsDiagnosticsListCall {
	c.urlParams_.Set("alertFilter", alertFilter)
	return c
}

// PageSize sets the optional parameter "pageSize": Specifies the
// maximum number of results to return. Defaults to 10. Maximum 1000.
func (c *BeaconsDiagnosticsListCall) PageSize(pageSize int64) *BeaconsDiagnosticsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": Requests results
// that occur after the `page_token`, obtained from the response to a
// previous request.
func (c *BeaconsDiagnosticsListCall) PageToken(pageToken string) *BeaconsDiagnosticsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ProjectId sets the optional parameter "projectId": Requests only
// diagnostic records for the given project id. If not set, then the
// project making the request will be used for looking up diagnostic
// records.
func (c *BeaconsDiagnosticsListCall) ProjectId(projectId string) *BeaconsDiagnosticsListCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BeaconsDiagnosticsListCall) Fields(s ...googleapi.Field) *BeaconsDiagnosticsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BeaconsDiagnosticsListCall) IfNoneMatch(entityTag string) *BeaconsDiagnosticsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BeaconsDiagnosticsListCall) Context(ctx context.Context) *BeaconsDiagnosticsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BeaconsDiagnosticsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BeaconsDiagnosticsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+beaconName}/diagnostics")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"beaconName": c.beaconName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.beacons.diagnostics.list" call.
// Exactly one of *ListDiagnosticsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListDiagnosticsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BeaconsDiagnosticsListCall) Do(opts ...googleapi.CallOption) (*ListDiagnosticsResponse, error) {
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
	ret := &ListDiagnosticsResponse{
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
	//   "description": "List the diagnostics for a single beacon. You can also list diagnostics for all the beacons owned by your Google Developers Console project by using the beacon name `beacons/-`. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **viewer**, **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "GET",
	//   "id": "proximitybeacon.beacons.diagnostics.list",
	//   "parameterOrder": [
	//     "beaconName"
	//   ],
	//   "parameters": {
	//     "alertFilter": {
	//       "description": "Requests only beacons that have the given alert. For example, to find beacons that have low batteries use `alert_filter=LOW_BATTERY`.",
	//       "enum": [
	//         "ALERT_UNSPECIFIED",
	//         "WRONG_LOCATION",
	//         "LOW_BATTERY"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "beaconName": {
	//       "description": "Beacon that the diagnostics are for.",
	//       "location": "path",
	//       "pattern": "^beacons/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Specifies the maximum number of results to return. Defaults to 10. Maximum 1000. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Requests results that occur after the `page_token`, obtained from the response to a previous request. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "Requests only diagnostic records for the given project id. If not set, then the project making the request will be used for looking up diagnostic records. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+beaconName}/diagnostics",
	//   "response": {
	//     "$ref": "ListDiagnosticsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *BeaconsDiagnosticsListCall) Pages(ctx context.Context, f func(*ListDiagnosticsResponse) error) error {
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

// method id "proximitybeacon.namespaces.list":

type NamespacesListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists all attachment namespaces owned by your Google Developers
// Console project. Attachment data associated with a beacon must
// include a namespaced type, and the namespace must be owned by your
// project. Authenticate using an [OAuth access
// token](https://developers.google.com/identity/protocols/OAuth2) from
// a signed-in user with **viewer**, **Is owner** or **Can edit**
// permissions in the Google Developers Console project.
func (r *NamespacesService) List() *NamespacesListCall {
	c := &NamespacesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// ProjectId sets the optional parameter "projectId": The project id to
// list namespaces under.
func (c *NamespacesListCall) ProjectId(projectId string) *NamespacesListCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NamespacesListCall) Fields(s ...googleapi.Field) *NamespacesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *NamespacesListCall) IfNoneMatch(entityTag string) *NamespacesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NamespacesListCall) Context(ctx context.Context) *NamespacesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NamespacesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NamespacesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/namespaces")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.namespaces.list" call.
// Exactly one of *ListNamespacesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListNamespacesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *NamespacesListCall) Do(opts ...googleapi.CallOption) (*ListNamespacesResponse, error) {
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
	ret := &ListNamespacesResponse{
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
	//   "description": "Lists all attachment namespaces owned by your Google Developers Console project. Attachment data associated with a beacon must include a namespaced type, and the namespace must be owned by your project. Authenticate using an [OAuth access token](https://developers.google.com/identity/protocols/OAuth2) from a signed-in user with **viewer**, **Is owner** or **Can edit** permissions in the Google Developers Console project.",
	//   "httpMethod": "GET",
	//   "id": "proximitybeacon.namespaces.list",
	//   "parameters": {
	//     "projectId": {
	//       "description": "The project id to list namespaces under. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/namespaces",
	//   "response": {
	//     "$ref": "ListNamespacesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.namespaces.update":

type NamespacesUpdateCall struct {
	s             *Service
	namespaceName string
	namespace     *Namespace
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// Update: Updates the information about the specified namespace. Only
// the namespace visibility can be updated.
func (r *NamespacesService) Update(namespaceName string, namespace *Namespace) *NamespacesUpdateCall {
	c := &NamespacesUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.namespaceName = namespaceName
	c.namespace = namespace
	return c
}

// ProjectId sets the optional parameter "projectId": The project id of
// the namespace to update. If the project id is not specified then the
// project making the request is used. The project id must match the
// project that owns the beacon.
func (c *NamespacesUpdateCall) ProjectId(projectId string) *NamespacesUpdateCall {
	c.urlParams_.Set("projectId", projectId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NamespacesUpdateCall) Fields(s ...googleapi.Field) *NamespacesUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NamespacesUpdateCall) Context(ctx context.Context) *NamespacesUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NamespacesUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NamespacesUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.namespace)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/{+namespaceName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"namespaceName": c.namespaceName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.namespaces.update" call.
// Exactly one of *Namespace or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Namespace.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *NamespacesUpdateCall) Do(opts ...googleapi.CallOption) (*Namespace, error) {
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
	ret := &Namespace{
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
	//   "description": "Updates the information about the specified namespace. Only the namespace visibility can be updated.",
	//   "httpMethod": "PUT",
	//   "id": "proximitybeacon.namespaces.update",
	//   "parameterOrder": [
	//     "namespaceName"
	//   ],
	//   "parameters": {
	//     "namespaceName": {
	//       "description": "Resource name of this namespace. Namespaces names have the format: namespaces/namespace.",
	//       "location": "path",
	//       "pattern": "^namespaces/[^/]*$",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projectId": {
	//       "description": "The project id of the namespace to update. If the project id is not specified then the project making the request is used. The project id must match the project that owns the beacon. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1beta1/{+namespaceName}",
	//   "request": {
	//     "$ref": "Namespace"
	//   },
	//   "response": {
	//     "$ref": "Namespace"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

// method id "proximitybeacon.getEidparams":

type V1beta1GetEidparamsCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetEidparams: Gets the Proximity Beacon API's current public key and
// associated parameters used to initiate the Diffie-Hellman key
// exchange required to register a beacon that broadcasts the
// Eddystone-EID format. This key changes periodically; clients may
// cache it and re-use the same public key to provision and register
// multiple beacons. However, clients should be prepared to refresh this
// key when they encounter an error registering an Eddystone-EID beacon.
func (r *V1beta1Service) GetEidparams() *V1beta1GetEidparamsCall {
	c := &V1beta1GetEidparamsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *V1beta1GetEidparamsCall) Fields(s ...googleapi.Field) *V1beta1GetEidparamsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *V1beta1GetEidparamsCall) IfNoneMatch(entityTag string) *V1beta1GetEidparamsCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *V1beta1GetEidparamsCall) Context(ctx context.Context) *V1beta1GetEidparamsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *V1beta1GetEidparamsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *V1beta1GetEidparamsCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1beta1/eidparams")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "proximitybeacon.getEidparams" call.
// Exactly one of *EphemeralIdRegistrationParams or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *EphemeralIdRegistrationParams.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *V1beta1GetEidparamsCall) Do(opts ...googleapi.CallOption) (*EphemeralIdRegistrationParams, error) {
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
	ret := &EphemeralIdRegistrationParams{
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
	//   "description": "Gets the Proximity Beacon API's current public key and associated parameters used to initiate the Diffie-Hellman key exchange required to register a beacon that broadcasts the Eddystone-EID format. This key changes periodically; clients may cache it and re-use the same public key to provision and register multiple beacons. However, clients should be prepared to refresh this key when they encounter an error registering an Eddystone-EID beacon.",
	//   "httpMethod": "GET",
	//   "id": "proximitybeacon.getEidparams",
	//   "path": "v1beta1/eidparams",
	//   "response": {
	//     "$ref": "EphemeralIdRegistrationParams"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/userlocation.beacon.registry"
	//   ]
	// }

}

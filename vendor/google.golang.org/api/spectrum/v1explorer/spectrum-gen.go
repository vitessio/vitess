// Package spectrum provides access to the Google Spectrum Database API.
//
// See http://developers.google.com/spectrum
//
// Usage example:
//
//   import "google.golang.org/api/spectrum/v1explorer"
//   ...
//   spectrumService, err := spectrum.New(oauthHttpClient)
package spectrum // import "google.golang.org/api/spectrum/v1explorer"

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

const apiId = "spectrum:v1explorer"
const apiName = "spectrum"
const apiVersion = "v1explorer"
const basePath = "https://www.googleapis.com/spectrum/v1explorer/paws/"

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Paws = NewPawsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Paws *PawsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewPawsService(s *Service) *PawsService {
	rs := &PawsService{s: s}
	return rs
}

type PawsService struct {
	s *Service
}

// AntennaCharacteristics: Antenna characteristics provide additional
// information, such as the antenna height, antenna type, etc. Whether
// antenna characteristics must be provided in a request depends on the
// device type and regulatory domain.
type AntennaCharacteristics struct {
	// Height: The antenna height in meters. Whether the antenna height is
	// required depends on the device type and the regulatory domain. Note
	// that the height may be negative.
	Height float64 `json:"height,omitempty"`

	// HeightType: If the height is required, then the height type (AGL for
	// above ground level or AMSL for above mean sea level) is also
	// required. The default is AGL.
	HeightType string `json:"heightType,omitempty"`

	// HeightUncertainty: The height uncertainty in meters. Whether this is
	// required depends on the regulatory domain.
	HeightUncertainty float64 `json:"heightUncertainty,omitempty"`

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

func (s *AntennaCharacteristics) MarshalJSON() ([]byte, error) {
	type noMethod AntennaCharacteristics
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DatabaseSpec: This message contains the name and URI of a database.
type DatabaseSpec struct {
	// Name: The display name for a database.
	Name string `json:"name,omitempty"`

	// Uri: The corresponding URI of the database.
	Uri string `json:"uri,omitempty"`

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

func (s *DatabaseSpec) MarshalJSON() ([]byte, error) {
	type noMethod DatabaseSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DbUpdateSpec: This message is provided by the database to notify
// devices of an upcoming change to the database URI.
type DbUpdateSpec struct {
	// Databases: A required list of one or more databases. A device should
	// update its preconfigured list of databases to replace (only) the
	// database that provided the response with the specified entries.
	Databases []*DatabaseSpec `json:"databases,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Databases") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Databases") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DbUpdateSpec) MarshalJSON() ([]byte, error) {
	type noMethod DbUpdateSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeviceCapabilities: Device capabilities provide additional
// information that may be used by a device to provide additional
// information to the database that may help it to determine available
// spectrum. If the database does not support device capabilities it
// will ignore the parameter altogether.
type DeviceCapabilities struct {
	// FrequencyRanges: An optional list of frequency ranges supported by
	// the device. Each element must contain start and stop frequencies in
	// which the device can operate. Channel identifiers are optional. When
	// specified, the database should not return available spectrum that
	// falls outside these ranges or channel IDs.
	FrequencyRanges []*FrequencyRange `json:"frequencyRanges,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FrequencyRanges") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FrequencyRanges") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DeviceCapabilities) MarshalJSON() ([]byte, error) {
	type noMethod DeviceCapabilities
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeviceDescriptor: The device descriptor contains parameters that
// identify the specific device, such as its manufacturer serial number,
// regulatory-specific identifier (e.g., FCC ID), and any other device
// characteristics required by regulatory domains.
type DeviceDescriptor struct {
	// EtsiEnDeviceCategory: Specifies the ETSI white space device category.
	// Valid values are the strings master and slave. This field is
	// case-insensitive. Consult the ETSI documentation for details about
	// the device types.
	EtsiEnDeviceCategory string `json:"etsiEnDeviceCategory,omitempty"`

	// EtsiEnDeviceEmissionsClass: Specifies the ETSI white space device
	// emissions class. The values are represented by numeric strings, such
	// as 1, 2, etc. Consult the ETSI documentation for details about the
	// device types.
	EtsiEnDeviceEmissionsClass string `json:"etsiEnDeviceEmissionsClass,omitempty"`

	// EtsiEnDeviceType: Specifies the ETSI white space device type. Valid
	// values are single-letter strings, such as A, B, etc. Consult the ETSI
	// documentation for details about the device types.
	EtsiEnDeviceType string `json:"etsiEnDeviceType,omitempty"`

	// EtsiEnTechnologyId: Specifies the ETSI white space device technology
	// identifier. The string value must not exceed 64 characters in length.
	// Consult the ETSI documentation for details about the device types.
	EtsiEnTechnologyId string `json:"etsiEnTechnologyId,omitempty"`

	// FccId: Specifies the device's FCC certification identifier. The value
	// is an identifier string whose length should not exceed 32 characters.
	// Note that, in practice, a valid FCC ID may be limited to 19
	// characters.
	FccId string `json:"fccId,omitempty"`

	// FccTvbdDeviceType: Specifies the TV Band White Space device type, as
	// defined by the FCC. Valid values are FIXED, MODE_1, MODE_2.
	FccTvbdDeviceType string `json:"fccTvbdDeviceType,omitempty"`

	// ManufacturerId: The manufacturer's ID may be required by the
	// regulatory domain. This should represent the name of the device
	// manufacturer, should be consistent across all devices from the same
	// manufacturer, and should be distinct from that of other
	// manufacturers. The string value must not exceed 64 characters in
	// length.
	ManufacturerId string `json:"manufacturerId,omitempty"`

	// ModelId: The device's model ID may be required by the regulatory
	// domain. The string value must not exceed 64 characters in length.
	ModelId string `json:"modelId,omitempty"`

	// RulesetIds: The list of identifiers for rulesets supported by the
	// device. A database may require that the device provide this list
	// before servicing the device requests. If the database does not
	// support any of the rulesets specified in the list, the database may
	// refuse to service the device requests. If present, the list must
	// contain at least one entry.
	//
	// For information about the valid requests, see section 9.2 of the PAWS
	// specification. Currently, FccTvBandWhiteSpace-2010 is the only
	// supported ruleset.
	RulesetIds []string `json:"rulesetIds,omitempty"`

	// SerialNumber: The manufacturer's device serial number; required by
	// the applicable regulatory domain. The length of the value must not
	// exceed 64 characters.
	SerialNumber string `json:"serialNumber,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "EtsiEnDeviceCategory") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EtsiEnDeviceCategory") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DeviceDescriptor) MarshalJSON() ([]byte, error) {
	type noMethod DeviceDescriptor
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeviceOwner: This parameter contains device-owner information
// required as part of device registration. The regulatory domains may
// require additional parameters.
//
// All contact information must be expressed using the structure defined
// by the vCard format specification. Only the contact fields of vCard
// are supported:
// - fn: Full name of an individual
// - org: Name of the organization
// - adr: Address fields
// - tel: Telephone numbers
// - email: Email addresses
//
// Note that the vCard specification defines maximum lengths for each
// field.
type DeviceOwner struct {
	// Operator: The vCard contact information for the device operator is
	// optional, but may be required by specific regulatory domains.
	Operator *Vcard `json:"operator,omitempty"`

	// Owner: The vCard contact information for the individual or business
	// that owns the device is required.
	Owner *Vcard `json:"owner,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Operator") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Operator") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeviceOwner) MarshalJSON() ([]byte, error) {
	type noMethod DeviceOwner
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeviceValidity: The device validity element describes whether a
// particular device is valid to operate in the regulatory domain.
type DeviceValidity struct {
	// DeviceDesc: The descriptor of the device for which the validity check
	// was requested. It will always be present.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// IsValid: The validity status: true if the device is valid for
	// operation, false otherwise. It will always be present.
	IsValid bool `json:"isValid,omitempty"`

	// Reason: If the device identifier is not valid, the database may
	// include a reason. The reason may be in any language. The length of
	// the value should not exceed 128 characters.
	Reason string `json:"reason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceDesc") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceDesc") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeviceValidity) MarshalJSON() ([]byte, error) {
	type noMethod DeviceValidity
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventTime: The start and stop times of an event. This is used to
// indicate the time period for which a spectrum profile is valid.
//
// Both times are expressed using the format, YYYY-MM-DDThh:mm:ssZ, as
// defined in RFC3339. The times must be expressed using UTC.
type EventTime struct {
	// StartTime: The inclusive start of the event. It will be present.
	StartTime string `json:"startTime,omitempty"`

	// StopTime: The exclusive end of the event. It will be present.
	StopTime string `json:"stopTime,omitempty"`

	// ForceSendFields is a list of field names (e.g. "StartTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "StartTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventTime) MarshalJSON() ([]byte, error) {
	type noMethod EventTime
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FrequencyRange: A specific range of frequencies together with the
// associated maximum power level and channel identifier.
type FrequencyRange struct {
	// ChannelId: The database may include a channel identifier, when
	// applicable. When it is included, the device should treat it as
	// informative. The length of the identifier should not exceed 16
	// characters.
	ChannelId string `json:"channelId,omitempty"`

	// MaxPowerDBm: The maximum total power level (EIRP)—computed over the
	// corresponding operating bandwidth—that is permitted within the
	// frequency range. Depending on the context in which the
	// frequency-range element appears, this value may be required. For
	// example, it is required in the available-spectrum response,
	// available-spectrum-batch response, and spectrum-use notification
	// message, but it should not be present (it is not applicable) when the
	// frequency range appears inside a device-capabilities message.
	MaxPowerDBm float64 `json:"maxPowerDBm,omitempty"`

	// StartHz: The required inclusive start of the frequency range (in
	// Hertz).
	StartHz float64 `json:"startHz,omitempty"`

	// StopHz: The required exclusive end of the frequency range (in Hertz).
	StopHz float64 `json:"stopHz,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ChannelId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ChannelId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FrequencyRange) MarshalJSON() ([]byte, error) {
	type noMethod FrequencyRange
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeoLocation: This parameter is used to specify the geolocation of the
// device.
type GeoLocation struct {
	// Confidence: The location confidence level, as an integer percentage,
	// may be required, depending on the regulatory domain. When the
	// parameter is optional and not provided, its value is assumed to be
	// 95. Valid values range from 0 to 99, since, in practice, 100-percent
	// confidence is not achievable. The confidence value is meaningful only
	// when geolocation refers to a point with uncertainty.
	Confidence int64 `json:"confidence,omitempty"`

	// Point: If present, indicates that the geolocation represents a point.
	// Paradoxically, a point is parameterized using an ellipse, where the
	// center represents the location of the point and the distances along
	// the major and minor axes represent the uncertainty. The uncertainty
	// values may be required, depending on the regulatory domain.
	Point *GeoLocationEllipse `json:"point,omitempty"`

	// Region: If present, indicates that the geolocation represents a
	// region. Database support for regions is optional.
	Region *GeoLocationPolygon `json:"region,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Confidence") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Confidence") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeoLocation) MarshalJSON() ([]byte, error) {
	type noMethod GeoLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeoLocationEllipse: A "point" with uncertainty is represented using
// the Ellipse shape.
type GeoLocationEllipse struct {
	// Center: A required geo-spatial point representing the center of the
	// ellipse.
	Center *GeoLocationPoint `json:"center,omitempty"`

	// Orientation: A floating-point number that expresses the orientation
	// of the ellipse, representing the rotation, in degrees, of the
	// semi-major axis from North towards the East. For example, when the
	// uncertainty is greatest along the North-South direction, orientation
	// is 0 degrees; conversely, if the uncertainty is greatest along the
	// East-West direction, orientation is 90 degrees. When orientation is
	// not present, the orientation is assumed to be 0.
	Orientation float64 `json:"orientation,omitempty"`

	// SemiMajorAxis: A floating-point number that expresses the location
	// uncertainty along the major axis of the ellipse. May be required by
	// the regulatory domain. When the uncertainty is optional, the default
	// value is 0.
	SemiMajorAxis float64 `json:"semiMajorAxis,omitempty"`

	// SemiMinorAxis: A floating-point number that expresses the location
	// uncertainty along the minor axis of the ellipse. May be required by
	// the regulatory domain. When the uncertainty is optional, the default
	// value is 0.
	SemiMinorAxis float64 `json:"semiMinorAxis,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Center") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Center") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeoLocationEllipse) MarshalJSON() ([]byte, error) {
	type noMethod GeoLocationEllipse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeoLocationPoint: A single geolocation on the globe.
type GeoLocationPoint struct {
	// Latitude: A required floating-point number that expresses the
	// latitude in degrees using the WGS84 datum. For details on this
	// encoding, see the National Imagery and Mapping Agency's Technical
	// Report TR8350.2.
	Latitude float64 `json:"latitude,omitempty"`

	// Longitude: A required floating-point number that expresses the
	// longitude in degrees using the WGS84 datum. For details on this
	// encoding, see the National Imagery and Mapping Agency's Technical
	// Report TR8350.2.
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

func (s *GeoLocationPoint) MarshalJSON() ([]byte, error) {
	type noMethod GeoLocationPoint
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeoLocationPolygon: A region is represented using the polygonal
// shape.
type GeoLocationPolygon struct {
	// Exterior: When the geolocation describes a region, the exterior field
	// refers to a list of latitude/longitude points that represent the
	// vertices of a polygon. The first and last points must be the same.
	// Thus, a minimum of four points is required. The following polygon
	// restrictions from RFC5491 apply:
	// - A connecting line shall not cross another connecting line of the
	// same polygon.
	// - The vertices must be defined in a counterclockwise order.
	// - The edges of a polygon are defined by the shortest path between two
	// points in space (not a geodesic curve). Consequently, the length
	// between two adjacent vertices should be restricted to a maximum of
	// 130 km.
	// - All vertices are assumed to be at the same altitude.
	// - Polygon shapes should be restricted to a maximum of 15 vertices (16
	// points that include the repeated vertex).
	Exterior []*GeoLocationPoint `json:"exterior,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Exterior") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Exterior") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeoLocationPolygon) MarshalJSON() ([]byte, error) {
	type noMethod GeoLocationPolygon
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeoSpectrumSchedule: The schedule of spectrum profiles available at a
// particular geolocation.
type GeoSpectrumSchedule struct {
	// Location: The geolocation identifies the location at which the
	// spectrum schedule applies. It will always be present.
	Location *GeoLocation `json:"location,omitempty"`

	// SpectrumSchedules: A list of available spectrum profiles and
	// associated times. It will always be present, and at least one
	// schedule must be included (though it may be empty if there is no
	// available spectrum). More than one schedule may be included to
	// represent future changes to the available spectrum.
	SpectrumSchedules []*SpectrumSchedule `json:"spectrumSchedules,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Location") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Location") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeoSpectrumSchedule) MarshalJSON() ([]byte, error) {
	type noMethod GeoSpectrumSchedule
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsGetSpectrumBatchRequest: The request message for a batch
// available spectrum query protocol.
type PawsGetSpectrumBatchRequest struct {
	// Antenna: Depending on device type and regulatory domain, antenna
	// characteristics may be required.
	Antenna *AntennaCharacteristics `json:"antenna,omitempty"`

	// Capabilities: The master device may include its device capabilities
	// to limit the available-spectrum batch response to the spectrum that
	// is compatible with its capabilities. The database should not return
	// spectrum that is incompatible with the specified capabilities.
	Capabilities *DeviceCapabilities `json:"capabilities,omitempty"`

	// DeviceDesc: When the available spectrum request is made on behalf of
	// a specific device (a master or slave device), device descriptor
	// information for the device on whose behalf the request is made is
	// required (in such cases, the requestType parameter must be empty).
	// When a requestType value is specified, device descriptor information
	// may be optional or required according to the rules of the applicable
	// regulatory domain.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// Locations: A geolocation list is required. This allows a device to
	// specify its current location plus additional anticipated locations
	// when allowed by the regulatory domain. At least one location must be
	// included. Geolocation must be given as the location of the radiation
	// center of the device's antenna. If a location specifies a region,
	// rather than a point, the database may return an UNIMPLEMENTED error
	// if it does not support query by region.
	//
	// There is no upper limit on the number of locations included in a
	// available spectrum batch request, but the database may restrict the
	// number of locations it supports by returning a response with fewer
	// locations than specified in the batch request. Note that geolocations
	// must be those of the master device (a device with geolocation
	// capability that makes an available spectrum batch request), whether
	// the master device is making the request on its own behalf or on
	// behalf of a slave device (one without geolocation capability).
	Locations []*GeoLocation `json:"locations,omitempty"`

	// MasterDeviceDesc: When an available spectrum batch request is made by
	// the master device (a device with geolocation capability) on behalf of
	// a slave device (a device without geolocation capability), the rules
	// of the applicable regulatory domain may require the master device to
	// provide its own device descriptor information (in addition to device
	// descriptor information for the slave device in a separate parameter).
	MasterDeviceDesc *DeviceDescriptor `json:"masterDeviceDesc,omitempty"`

	// Owner: Depending on device type and regulatory domain, device owner
	// information may be included in an available spectrum batch request.
	// This allows the device to register and get spectrum-availability
	// information in a single request.
	Owner *DeviceOwner `json:"owner,omitempty"`

	// RequestType: The request type parameter is an optional parameter that
	// can be used to modify an available spectrum batch request, but its
	// use depends on applicable regulatory rules. For example, It may be
	// used to request generic slave device parameters without having to
	// specify the device descriptor for a specific device. When the
	// requestType parameter is missing, the request is for a specific
	// device (master or slave), and the device descriptor parameter for the
	// device on whose behalf the batch request is made is required.
	RequestType string `json:"requestType,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Antenna") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Antenna") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PawsGetSpectrumBatchRequest) MarshalJSON() ([]byte, error) {
	type noMethod PawsGetSpectrumBatchRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsGetSpectrumBatchResponse: The response message for the batch
// available spectrum query contains a schedule of available spectrum
// for the device at multiple locations.
type PawsGetSpectrumBatchResponse struct {
	// DatabaseChange: A database may include the databaseChange parameter
	// to notify a device of a change to its database URI, providing one or
	// more alternate database URIs. The device should use this information
	// to update its list of pre-configured databases by (only) replacing
	// its entry for the responding database with the list of alternate
	// URIs.
	DatabaseChange *DbUpdateSpec `json:"databaseChange,omitempty"`

	// DeviceDesc: The database must return in its available spectrum
	// response the device descriptor information it received in the master
	// device's available spectrum batch request.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// GeoSpectrumSchedules: The available spectrum batch response must
	// contain a geo-spectrum schedule list, The list may be empty if
	// spectrum is not available. The database may return more than one
	// geo-spectrum schedule to represent future changes to the available
	// spectrum. How far in advance a schedule may be provided depends upon
	// the applicable regulatory domain. The database may return available
	// spectrum for fewer geolocations than requested. The device must not
	// make assumptions about the order of the entries in the list, and must
	// use the geolocation value in each geo-spectrum schedule entry to
	// match available spectrum to a location.
	GeoSpectrumSchedules []*GeoSpectrumSchedule `json:"geoSpectrumSchedules,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "spectrum#pawsGetSpectrumBatchResponse".
	Kind string `json:"kind,omitempty"`

	// MaxContiguousBwHz: The database may return a constraint on the
	// allowed maximum contiguous bandwidth (in Hertz). A regulatory domain
	// may require the database to return this parameter. When this
	// parameter is present in the response, the device must apply this
	// constraint to its spectrum-selection logic to ensure that no single
	// block of spectrum has bandwidth that exceeds this value.
	MaxContiguousBwHz float64 `json:"maxContiguousBwHz,omitempty"`

	// MaxTotalBwHz: The database may return a constraint on the allowed
	// maximum total bandwidth (in Hertz), which does not need to be
	// contiguous. A regulatory domain may require the database to return
	// this parameter. When this parameter is present in the available
	// spectrum batch response, the device must apply this constraint to its
	// spectrum-selection logic to ensure that total bandwidth does not
	// exceed this value.
	MaxTotalBwHz float64 `json:"maxTotalBwHz,omitempty"`

	// NeedsSpectrumReport: For regulatory domains that require a
	// spectrum-usage report from devices, the database must return true for
	// this parameter if the geo-spectrum schedules list is not empty;
	// otherwise, the database should either return false or omit this
	// parameter. If this parameter is present and its value is true, the
	// device must send a spectrum use notify message to the database;
	// otherwise, the device should not send the notification.
	NeedsSpectrumReport bool `json:"needsSpectrumReport,omitempty"`

	// RulesetInfo: The database should return ruleset information, which
	// identifies the applicable regulatory authority and ruleset for the
	// available spectrum batch response. If included, the device must use
	// the corresponding ruleset to interpret the response. Values provided
	// in the returned ruleset information, such as maxLocationChange, take
	// precedence over any conflicting values provided in the ruleset
	// information returned in a prior initialization response sent by the
	// database to the device.
	RulesetInfo *RulesetInfo `json:"rulesetInfo,omitempty"`

	// Timestamp: The database includes a timestamp of the form,
	// YYYY-MM-DDThh:mm:ssZ (Internet timestamp format per RFC3339), in its
	// available spectrum batch response. The timestamp should be used by
	// the device as a reference for the start and stop times specified in
	// the response spectrum schedules.
	Timestamp string `json:"timestamp,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DatabaseChange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DatabaseChange") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *PawsGetSpectrumBatchResponse) MarshalJSON() ([]byte, error) {
	type noMethod PawsGetSpectrumBatchResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsGetSpectrumRequest: The request message for the available
// spectrum query protocol which must include the device's geolocation.
type PawsGetSpectrumRequest struct {
	// Antenna: Depending on device type and regulatory domain, the
	// characteristics of the antenna may be required.
	Antenna *AntennaCharacteristics `json:"antenna,omitempty"`

	// Capabilities: The master device may include its device capabilities
	// to limit the available-spectrum response to the spectrum that is
	// compatible with its capabilities. The database should not return
	// spectrum that is incompatible with the specified capabilities.
	Capabilities *DeviceCapabilities `json:"capabilities,omitempty"`

	// DeviceDesc: When the available spectrum request is made on behalf of
	// a specific device (a master or slave device), device descriptor
	// information for that device is required (in such cases, the
	// requestType parameter must be empty). When a requestType value is
	// specified, device descriptor information may be optional or required
	// according to the rules of the applicable regulatory domain.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// Location: The geolocation of the master device (a device with
	// geolocation capability that makes an available spectrum request) is
	// required whether the master device is making the request on its own
	// behalf or on behalf of a slave device (one without geolocation
	// capability). The location must be the location of the radiation
	// center of the master device's antenna. To support mobile devices, a
	// regulatory domain may allow the anticipated position of the master
	// device to be given instead. If the location specifies a region,
	// rather than a point, the database may return an UNIMPLEMENTED error
	// code if it does not support query by region.
	Location *GeoLocation `json:"location,omitempty"`

	// MasterDeviceDesc: When an available spectrum request is made by the
	// master device (a device with geolocation capability) on behalf of a
	// slave device (a device without geolocation capability), the rules of
	// the applicable regulatory domain may require the master device to
	// provide its own device descriptor information (in addition to device
	// descriptor information for the slave device, which is provided in a
	// separate parameter).
	MasterDeviceDesc *DeviceDescriptor `json:"masterDeviceDesc,omitempty"`

	// Owner: Depending on device type and regulatory domain, device owner
	// information may be included in an available spectrum request. This
	// allows the device to register and get spectrum-availability
	// information in a single request.
	Owner *DeviceOwner `json:"owner,omitempty"`

	// RequestType: The request type parameter is an optional parameter that
	// can be used to modify an available spectrum request, but its use
	// depends on applicable regulatory rules. It may be used, for example,
	// to request generic slave device parameters without having to specify
	// the device descriptor for a specific device. When the requestType
	// parameter is missing, the request is for a specific device (master or
	// slave), and the deviceDesc parameter for the device on whose behalf
	// the request is made is required.
	RequestType string `json:"requestType,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Antenna") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Antenna") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PawsGetSpectrumRequest) MarshalJSON() ([]byte, error) {
	type noMethod PawsGetSpectrumRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsGetSpectrumResponse: The response message for the available
// spectrum query which contains a schedule of available spectrum for
// the device.
type PawsGetSpectrumResponse struct {
	// DatabaseChange: A database may include the databaseChange parameter
	// to notify a device of a change to its database URI, providing one or
	// more alternate database URIs. The device should use this information
	// to update its list of pre-configured databases by (only) replacing
	// its entry for the responding database with the list of alternate
	// URIs.
	DatabaseChange *DbUpdateSpec `json:"databaseChange,omitempty"`

	// DeviceDesc: The database must return, in its available spectrum
	// response, the device descriptor information it received in the master
	// device's available spectrum request.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "spectrum#pawsGetSpectrumResponse".
	Kind string `json:"kind,omitempty"`

	// MaxContiguousBwHz: The database may return a constraint on the
	// allowed maximum contiguous bandwidth (in Hertz). A regulatory domain
	// may require the database to return this parameter. When this
	// parameter is present in the response, the device must apply this
	// constraint to its spectrum-selection logic to ensure that no single
	// block of spectrum has bandwidth that exceeds this value.
	MaxContiguousBwHz float64 `json:"maxContiguousBwHz,omitempty"`

	// MaxTotalBwHz: The database may return a constraint on the allowed
	// maximum total bandwidth (in Hertz), which need not be contiguous. A
	// regulatory domain may require the database to return this parameter.
	// When this parameter is present in the available spectrum response,
	// the device must apply this constraint to its spectrum-selection logic
	// to ensure that total bandwidth does not exceed this value.
	MaxTotalBwHz float64 `json:"maxTotalBwHz,omitempty"`

	// NeedsSpectrumReport: For regulatory domains that require a
	// spectrum-usage report from devices, the database must return true for
	// this parameter if the spectrum schedule list is not empty; otherwise,
	// the database will either return false or omit this parameter. If this
	// parameter is present and its value is true, the device must send a
	// spectrum use notify message to the database; otherwise, the device
	// must not send the notification.
	NeedsSpectrumReport bool `json:"needsSpectrumReport,omitempty"`

	// RulesetInfo: The database should return ruleset information, which
	// identifies the applicable regulatory authority and ruleset for the
	// available spectrum response. If included, the device must use the
	// corresponding ruleset to interpret the response. Values provided in
	// the returned ruleset information, such as maxLocationChange, take
	// precedence over any conflicting values provided in the ruleset
	// information returned in a prior initialization response sent by the
	// database to the device.
	RulesetInfo *RulesetInfo `json:"rulesetInfo,omitempty"`

	// SpectrumSchedules: The available spectrum response must contain a
	// spectrum schedule list. The list may be empty if spectrum is not
	// available. The database may return more than one spectrum schedule to
	// represent future changes to the available spectrum. How far in
	// advance a schedule may be provided depends on the applicable
	// regulatory domain.
	SpectrumSchedules []*SpectrumSchedule `json:"spectrumSchedules,omitempty"`

	// Timestamp: The database includes a timestamp of the form
	// YYYY-MM-DDThh:mm:ssZ (Internet timestamp format per RFC3339) in its
	// available spectrum response. The timestamp should be used by the
	// device as a reference for the start and stop times specified in the
	// response spectrum schedules.
	Timestamp string `json:"timestamp,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DatabaseChange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DatabaseChange") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *PawsGetSpectrumResponse) MarshalJSON() ([]byte, error) {
	type noMethod PawsGetSpectrumResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsInitRequest: The initialization request message allows the master
// device to initiate exchange of capabilities with the database.
type PawsInitRequest struct {
	// DeviceDesc: The DeviceDescriptor parameter is required. If the
	// database does not support the device or any of the rulesets specified
	// in the device descriptor, it must return an UNSUPPORTED error code in
	// the error response.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// Location: A device's geolocation is required.
	Location *GeoLocation `json:"location,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceDesc") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceDesc") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PawsInitRequest) MarshalJSON() ([]byte, error) {
	type noMethod PawsInitRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsInitResponse: The initialization response message communicates
// database parameters to the requesting device.
type PawsInitResponse struct {
	// DatabaseChange: A database may include the databaseChange parameter
	// to notify a device of a change to its database URI, providing one or
	// more alternate database URIs. The device should use this information
	// to update its list of pre-configured databases by (only) replacing
	// its entry for the responding database with the list of alternate
	// URIs.
	DatabaseChange *DbUpdateSpec `json:"databaseChange,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "spectrum#pawsInitResponse".
	Kind string `json:"kind,omitempty"`

	// RulesetInfo: The rulesetInfo parameter must be included in the
	// response. This parameter specifies the regulatory domain and
	// parameters applicable to that domain. The database must include the
	// authority field, which defines the regulatory domain for the location
	// specified in the INIT_REQ message.
	RulesetInfo *RulesetInfo `json:"rulesetInfo,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DatabaseChange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DatabaseChange") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *PawsInitResponse) MarshalJSON() ([]byte, error) {
	type noMethod PawsInitResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsNotifySpectrumUseRequest: The spectrum-use notification message
// which must contain the geolocation of the Device and parameters
// required by the regulatory domain.
type PawsNotifySpectrumUseRequest struct {
	// DeviceDesc: Device descriptor information is required in the
	// spectrum-use notification message.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// Location: The geolocation of the master device (the device that is
	// sending the spectrum-use notification) to the database is required in
	// the spectrum-use notification message.
	Location *GeoLocation `json:"location,omitempty"`

	// Spectra: A spectrum list is required in the spectrum-use
	// notification. The list specifies the spectrum that the device expects
	// to use, which includes frequency ranges and maximum power levels. The
	// list may be empty if the device decides not to use any of spectrum.
	// For consistency, the psdBandwidthHz value should match that from one
	// of the spectrum elements in the corresponding available spectrum
	// response previously sent to the device by the database. Note that
	// maximum power levels in the spectrum element must be expressed as
	// power spectral density over the specified psdBandwidthHz value. The
	// actual bandwidth to be used (as computed from the start and stop
	// frequencies) may be different from the psdBandwidthHz value. As an
	// example, when regulatory rules express maximum power spectral density
	// in terms of maximum power over any 100 kHz band, then the
	// psdBandwidthHz value should be set to 100 kHz, even though the actual
	// bandwidth used can be 20 kHz.
	Spectra []*SpectrumMessage `json:"spectra,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceDesc") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceDesc") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PawsNotifySpectrumUseRequest) MarshalJSON() ([]byte, error) {
	type noMethod PawsNotifySpectrumUseRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsNotifySpectrumUseResponse: An empty response to the notification.
type PawsNotifySpectrumUseResponse struct {
	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "spectrum#pawsNotifySpectrumUseResponse".
	Kind string `json:"kind,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *PawsNotifySpectrumUseResponse) MarshalJSON() ([]byte, error) {
	type noMethod PawsNotifySpectrumUseResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsRegisterRequest: The registration request message contains the
// required registration parameters.
type PawsRegisterRequest struct {
	// Antenna: Antenna characteristics, including its height and height
	// type.
	Antenna *AntennaCharacteristics `json:"antenna,omitempty"`

	// DeviceDesc: A DeviceDescriptor is required.
	DeviceDesc *DeviceDescriptor `json:"deviceDesc,omitempty"`

	// DeviceOwner: Device owner information is required.
	DeviceOwner *DeviceOwner `json:"deviceOwner,omitempty"`

	// Location: A device's geolocation is required.
	Location *GeoLocation `json:"location,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Antenna") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Antenna") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PawsRegisterRequest) MarshalJSON() ([]byte, error) {
	type noMethod PawsRegisterRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsRegisterResponse: The registration response message simply
// acknowledges receipt of the request and is otherwise empty.
type PawsRegisterResponse struct {
	// DatabaseChange: A database may include the databaseChange parameter
	// to notify a device of a change to its database URI, providing one or
	// more alternate database URIs. The device should use this information
	// to update its list of pre-configured databases by (only) replacing
	// its entry for the responding database with the list of alternate
	// URIs.
	DatabaseChange *DbUpdateSpec `json:"databaseChange,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "spectrum#pawsRegisterResponse".
	Kind string `json:"kind,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DatabaseChange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DatabaseChange") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *PawsRegisterResponse) MarshalJSON() ([]byte, error) {
	type noMethod PawsRegisterResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsVerifyDeviceRequest: The device validation request message.
type PawsVerifyDeviceRequest struct {
	// DeviceDescs: A list of device descriptors, which specifies the slave
	// devices to be validated, is required.
	DeviceDescs []*DeviceDescriptor `json:"deviceDescs,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceDescs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceDescs") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PawsVerifyDeviceRequest) MarshalJSON() ([]byte, error) {
	type noMethod PawsVerifyDeviceRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PawsVerifyDeviceResponse: The device validation response message.
type PawsVerifyDeviceResponse struct {
	// DatabaseChange: A database may include the databaseChange parameter
	// to notify a device of a change to its database URI, providing one or
	// more alternate database URIs. The device should use this information
	// to update its list of pre-configured databases by (only) replacing
	// its entry for the responding database with the list of alternate
	// URIs.
	DatabaseChange *DbUpdateSpec `json:"databaseChange,omitempty"`

	// DeviceValidities: A device validities list is required in the device
	// validation response to report whether each slave device listed in a
	// previous device validation request is valid. The number of entries
	// must match the number of device descriptors listed in the previous
	// device validation request.
	DeviceValidities []*DeviceValidity `json:"deviceValidities,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "spectrum#pawsVerifyDeviceResponse".
	Kind string `json:"kind,omitempty"`

	// Type: The message type (e.g., INIT_REQ, AVAIL_SPECTRUM_REQ,
	// ...).
	//
	// Required field.
	Type string `json:"type,omitempty"`

	// Version: The PAWS version. Must be exactly 1.0.
	//
	// Required field.
	Version string `json:"version,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DatabaseChange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DatabaseChange") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *PawsVerifyDeviceResponse) MarshalJSON() ([]byte, error) {
	type noMethod PawsVerifyDeviceResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RulesetInfo: This contains parameters for the ruleset of a regulatory
// domain that is communicated using the initialization and
// available-spectrum processes.
type RulesetInfo struct {
	// Authority: The regulatory domain to which the ruleset belongs is
	// required. It must be a 2-letter country code. The device should use
	// this to determine additional device behavior required by the
	// associated regulatory domain.
	Authority string `json:"authority,omitempty"`

	// MaxLocationChange: The maximum location change in meters is required
	// in the initialization response, but optional otherwise. When the
	// device changes location by more than this specified distance, it must
	// contact the database to get the available spectrum for the new
	// location. If the device is using spectrum that is no longer
	// available, it must immediately cease use of the spectrum under rules
	// for database-managed spectrum. If this value is provided within the
	// context of an available-spectrum response, it takes precedence over
	// the value within the initialization response.
	MaxLocationChange float64 `json:"maxLocationChange,omitempty"`

	// MaxPollingSecs: The maximum duration, in seconds, between requests
	// for available spectrum. It is required in the initialization
	// response, but optional otherwise. The device must contact the
	// database to get available spectrum no less frequently than this
	// duration. If the new spectrum information indicates that the device
	// is using spectrum that is no longer available, it must immediately
	// cease use of those frequencies under rules for database-managed
	// spectrum. If this value is provided within the context of an
	// available-spectrum response, it takes precedence over the value
	// within the initialization response.
	MaxPollingSecs int64 `json:"maxPollingSecs,omitempty"`

	// RulesetIds: The identifiers of the rulesets supported for the
	// device's location. The database should include at least one
	// applicable ruleset in the initialization response. The device may use
	// the ruleset identifiers to determine parameters to include in
	// subsequent requests. Within the context of the available-spectrum
	// responses, the database should include the identifier of the ruleset
	// that it used to determine the available-spectrum response. If
	// included, the device must use the specified ruleset to interpret the
	// response. If the device does not support the indicated ruleset, it
	// must not operate in the spectrum governed by the ruleset.
	RulesetIds []string `json:"rulesetIds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Authority") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Authority") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RulesetInfo) MarshalJSON() ([]byte, error) {
	type noMethod RulesetInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SpectrumMessage: Available spectrum can be logically characterized by
// a list of frequency ranges and permissible power levels for each
// range.
type SpectrumMessage struct {
	// Bandwidth: The bandwidth (in Hertz) for which permissible power
	// levels are specified. For example, FCC regulation would require only
	// one spectrum specification at 6MHz bandwidth, but Ofcom regulation
	// would require two specifications, at 0.1MHz and 8MHz. This parameter
	// may be empty if there is no available spectrum. It will be present
	// otherwise.
	Bandwidth float64 `json:"bandwidth,omitempty"`

	// FrequencyRanges: The list of frequency ranges and permissible power
	// levels. The list may be empty if there is no available spectrum,
	// otherwise it will be present.
	FrequencyRanges []*FrequencyRange `json:"frequencyRanges,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Bandwidth") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Bandwidth") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SpectrumMessage) MarshalJSON() ([]byte, error) {
	type noMethod SpectrumMessage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SpectrumSchedule: The spectrum schedule element combines an event
// time with spectrum profile to define a time period in which the
// profile is valid.
type SpectrumSchedule struct {
	// EventTime: The event time expresses when the spectrum profile is
	// valid. It will always be present.
	EventTime *EventTime `json:"eventTime,omitempty"`

	// Spectra: A list of spectrum messages representing the usable profile.
	// It will always be present, but may be empty when there is no
	// available spectrum.
	Spectra []*SpectrumMessage `json:"spectra,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EventTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EventTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SpectrumSchedule) MarshalJSON() ([]byte, error) {
	type noMethod SpectrumSchedule
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Vcard: A vCard-in-JSON message that contains only the fields needed
// for PAWS:
// - fn: Full name of an individual
// - org: Name of the organization
// - adr: Address fields
// - tel: Telephone numbers
// - email: Email addresses
type Vcard struct {
	// Adr: The street address of the entity.
	Adr *VcardAddress `json:"adr,omitempty"`

	// Email: An email address that can be used to reach the contact.
	Email *VcardTypedText `json:"email,omitempty"`

	// Fn: The full name of the contact person. For example: John A. Smith.
	Fn string `json:"fn,omitempty"`

	// Org: The organization associated with the registering entity.
	Org *VcardTypedText `json:"org,omitempty"`

	// Tel: A telephone number that can be used to call the contact.
	Tel *VcardTelephone `json:"tel,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Adr") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Adr") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Vcard) MarshalJSON() ([]byte, error) {
	type noMethod Vcard
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VcardAddress: The structure used to represent a street address.
type VcardAddress struct {
	// Code: The postal code associated with the address. For example:
	// 94423.
	Code string `json:"code,omitempty"`

	// Country: The country name. For example: US.
	Country string `json:"country,omitempty"`

	// Locality: The city or local equivalent portion of the address. For
	// example: San Jose.
	Locality string `json:"locality,omitempty"`

	// Pobox: An optional post office box number.
	Pobox string `json:"pobox,omitempty"`

	// Region: The state or local equivalent portion of the address. For
	// example: CA.
	Region string `json:"region,omitempty"`

	// Street: The street number and name. For example: 123 Any St.
	Street string `json:"street,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Code") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Code") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VcardAddress) MarshalJSON() ([]byte, error) {
	type noMethod VcardAddress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VcardTelephone: The structure used to represent a telephone number.
type VcardTelephone struct {
	// Uri: A nested telephone URI of the form: tel:+1-123-456-7890.
	Uri string `json:"uri,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Uri") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Uri") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VcardTelephone) MarshalJSON() ([]byte, error) {
	type noMethod VcardTelephone
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VcardTypedText: The structure used to represent an organization and
// an email address.
type VcardTypedText struct {
	// Text: The text string associated with this item. For example, for an
	// org field: ACME, inc. For an email field: smith@example.com.
	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Text") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Text") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VcardTypedText) MarshalJSON() ([]byte, error) {
	type noMethod VcardTypedText
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "spectrum.paws.getSpectrum":

type PawsGetSpectrumCall struct {
	s                      *Service
	pawsgetspectrumrequest *PawsGetSpectrumRequest
	urlParams_             gensupport.URLParams
	ctx_                   context.Context
	header_                http.Header
}

// GetSpectrum: Requests information about the available spectrum for a
// device at a location. Requests from a fixed-mode device must include
// owner information so the device can be registered with the database.
func (r *PawsService) GetSpectrum(pawsgetspectrumrequest *PawsGetSpectrumRequest) *PawsGetSpectrumCall {
	c := &PawsGetSpectrumCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.pawsgetspectrumrequest = pawsgetspectrumrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PawsGetSpectrumCall) Fields(s ...googleapi.Field) *PawsGetSpectrumCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PawsGetSpectrumCall) Context(ctx context.Context) *PawsGetSpectrumCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PawsGetSpectrumCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PawsGetSpectrumCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pawsgetspectrumrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "getSpectrum")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "spectrum.paws.getSpectrum" call.
// Exactly one of *PawsGetSpectrumResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PawsGetSpectrumResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PawsGetSpectrumCall) Do(opts ...googleapi.CallOption) (*PawsGetSpectrumResponse, error) {
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
	ret := &PawsGetSpectrumResponse{
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
	//   "description": "Requests information about the available spectrum for a device at a location. Requests from a fixed-mode device must include owner information so the device can be registered with the database.",
	//   "httpMethod": "POST",
	//   "id": "spectrum.paws.getSpectrum",
	//   "path": "getSpectrum",
	//   "request": {
	//     "$ref": "PawsGetSpectrumRequest"
	//   },
	//   "response": {
	//     "$ref": "PawsGetSpectrumResponse"
	//   }
	// }

}

// method id "spectrum.paws.getSpectrumBatch":

type PawsGetSpectrumBatchCall struct {
	s                           *Service
	pawsgetspectrumbatchrequest *PawsGetSpectrumBatchRequest
	urlParams_                  gensupport.URLParams
	ctx_                        context.Context
	header_                     http.Header
}

// GetSpectrumBatch: The Google Spectrum Database does not support batch
// requests, so this method always yields an UNIMPLEMENTED error.
func (r *PawsService) GetSpectrumBatch(pawsgetspectrumbatchrequest *PawsGetSpectrumBatchRequest) *PawsGetSpectrumBatchCall {
	c := &PawsGetSpectrumBatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.pawsgetspectrumbatchrequest = pawsgetspectrumbatchrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PawsGetSpectrumBatchCall) Fields(s ...googleapi.Field) *PawsGetSpectrumBatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PawsGetSpectrumBatchCall) Context(ctx context.Context) *PawsGetSpectrumBatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PawsGetSpectrumBatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PawsGetSpectrumBatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pawsgetspectrumbatchrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "getSpectrumBatch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "spectrum.paws.getSpectrumBatch" call.
// Exactly one of *PawsGetSpectrumBatchResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *PawsGetSpectrumBatchResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PawsGetSpectrumBatchCall) Do(opts ...googleapi.CallOption) (*PawsGetSpectrumBatchResponse, error) {
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
	ret := &PawsGetSpectrumBatchResponse{
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
	//   "description": "The Google Spectrum Database does not support batch requests, so this method always yields an UNIMPLEMENTED error.",
	//   "httpMethod": "POST",
	//   "id": "spectrum.paws.getSpectrumBatch",
	//   "path": "getSpectrumBatch",
	//   "request": {
	//     "$ref": "PawsGetSpectrumBatchRequest"
	//   },
	//   "response": {
	//     "$ref": "PawsGetSpectrumBatchResponse"
	//   }
	// }

}

// method id "spectrum.paws.init":

type PawsInitCall struct {
	s               *Service
	pawsinitrequest *PawsInitRequest
	urlParams_      gensupport.URLParams
	ctx_            context.Context
	header_         http.Header
}

// Init: Initializes the connection between a white space device and the
// database.
func (r *PawsService) Init(pawsinitrequest *PawsInitRequest) *PawsInitCall {
	c := &PawsInitCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.pawsinitrequest = pawsinitrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PawsInitCall) Fields(s ...googleapi.Field) *PawsInitCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PawsInitCall) Context(ctx context.Context) *PawsInitCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PawsInitCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PawsInitCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pawsinitrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "init")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "spectrum.paws.init" call.
// Exactly one of *PawsInitResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PawsInitResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PawsInitCall) Do(opts ...googleapi.CallOption) (*PawsInitResponse, error) {
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
	ret := &PawsInitResponse{
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
	//   "description": "Initializes the connection between a white space device and the database.",
	//   "httpMethod": "POST",
	//   "id": "spectrum.paws.init",
	//   "path": "init",
	//   "request": {
	//     "$ref": "PawsInitRequest"
	//   },
	//   "response": {
	//     "$ref": "PawsInitResponse"
	//   }
	// }

}

// method id "spectrum.paws.notifySpectrumUse":

type PawsNotifySpectrumUseCall struct {
	s                            *Service
	pawsnotifyspectrumuserequest *PawsNotifySpectrumUseRequest
	urlParams_                   gensupport.URLParams
	ctx_                         context.Context
	header_                      http.Header
}

// NotifySpectrumUse: Notifies the database that the device has selected
// certain frequency ranges for transmission. Only to be invoked when
// required by the regulator. The Google Spectrum Database does not
// operate in domains that require notification, so this always yields
// an UNIMPLEMENTED error.
func (r *PawsService) NotifySpectrumUse(pawsnotifyspectrumuserequest *PawsNotifySpectrumUseRequest) *PawsNotifySpectrumUseCall {
	c := &PawsNotifySpectrumUseCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.pawsnotifyspectrumuserequest = pawsnotifyspectrumuserequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PawsNotifySpectrumUseCall) Fields(s ...googleapi.Field) *PawsNotifySpectrumUseCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PawsNotifySpectrumUseCall) Context(ctx context.Context) *PawsNotifySpectrumUseCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PawsNotifySpectrumUseCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PawsNotifySpectrumUseCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pawsnotifyspectrumuserequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "notifySpectrumUse")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "spectrum.paws.notifySpectrumUse" call.
// Exactly one of *PawsNotifySpectrumUseResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *PawsNotifySpectrumUseResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PawsNotifySpectrumUseCall) Do(opts ...googleapi.CallOption) (*PawsNotifySpectrumUseResponse, error) {
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
	ret := &PawsNotifySpectrumUseResponse{
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
	//   "description": "Notifies the database that the device has selected certain frequency ranges for transmission. Only to be invoked when required by the regulator. The Google Spectrum Database does not operate in domains that require notification, so this always yields an UNIMPLEMENTED error.",
	//   "httpMethod": "POST",
	//   "id": "spectrum.paws.notifySpectrumUse",
	//   "path": "notifySpectrumUse",
	//   "request": {
	//     "$ref": "PawsNotifySpectrumUseRequest"
	//   },
	//   "response": {
	//     "$ref": "PawsNotifySpectrumUseResponse"
	//   }
	// }

}

// method id "spectrum.paws.register":

type PawsRegisterCall struct {
	s                   *Service
	pawsregisterrequest *PawsRegisterRequest
	urlParams_          gensupport.URLParams
	ctx_                context.Context
	header_             http.Header
}

// Register: The Google Spectrum Database implements registration in the
// getSpectrum method. As such this always returns an UNIMPLEMENTED
// error.
func (r *PawsService) Register(pawsregisterrequest *PawsRegisterRequest) *PawsRegisterCall {
	c := &PawsRegisterCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.pawsregisterrequest = pawsregisterrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PawsRegisterCall) Fields(s ...googleapi.Field) *PawsRegisterCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PawsRegisterCall) Context(ctx context.Context) *PawsRegisterCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PawsRegisterCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PawsRegisterCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pawsregisterrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "register")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "spectrum.paws.register" call.
// Exactly one of *PawsRegisterResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PawsRegisterResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PawsRegisterCall) Do(opts ...googleapi.CallOption) (*PawsRegisterResponse, error) {
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
	ret := &PawsRegisterResponse{
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
	//   "description": "The Google Spectrum Database implements registration in the getSpectrum method. As such this always returns an UNIMPLEMENTED error.",
	//   "httpMethod": "POST",
	//   "id": "spectrum.paws.register",
	//   "path": "register",
	//   "request": {
	//     "$ref": "PawsRegisterRequest"
	//   },
	//   "response": {
	//     "$ref": "PawsRegisterResponse"
	//   }
	// }

}

// method id "spectrum.paws.verifyDevice":

type PawsVerifyDeviceCall struct {
	s                       *Service
	pawsverifydevicerequest *PawsVerifyDeviceRequest
	urlParams_              gensupport.URLParams
	ctx_                    context.Context
	header_                 http.Header
}

// VerifyDevice: Validates a device for white space use in accordance
// with regulatory rules. The Google Spectrum Database does not support
// master/slave configurations, so this always yields an UNIMPLEMENTED
// error.
func (r *PawsService) VerifyDevice(pawsverifydevicerequest *PawsVerifyDeviceRequest) *PawsVerifyDeviceCall {
	c := &PawsVerifyDeviceCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.pawsverifydevicerequest = pawsverifydevicerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PawsVerifyDeviceCall) Fields(s ...googleapi.Field) *PawsVerifyDeviceCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PawsVerifyDeviceCall) Context(ctx context.Context) *PawsVerifyDeviceCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PawsVerifyDeviceCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PawsVerifyDeviceCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pawsverifydevicerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "verifyDevice")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "spectrum.paws.verifyDevice" call.
// Exactly one of *PawsVerifyDeviceResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *PawsVerifyDeviceResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PawsVerifyDeviceCall) Do(opts ...googleapi.CallOption) (*PawsVerifyDeviceResponse, error) {
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
	ret := &PawsVerifyDeviceResponse{
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
	//   "description": "Validates a device for white space use in accordance with regulatory rules. The Google Spectrum Database does not support master/slave configurations, so this always yields an UNIMPLEMENTED error.",
	//   "httpMethod": "POST",
	//   "id": "spectrum.paws.verifyDevice",
	//   "path": "verifyDevice",
	//   "request": {
	//     "$ref": "PawsVerifyDeviceRequest"
	//   },
	//   "response": {
	//     "$ref": "PawsVerifyDeviceResponse"
	//   }
	// }

}

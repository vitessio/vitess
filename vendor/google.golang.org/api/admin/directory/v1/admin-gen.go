// Package admin provides access to the Admin Directory API.
//
// See https://developers.google.com/admin-sdk/directory/
//
// Usage example:
//
//   import "google.golang.org/api/admin/directory/v1"
//   ...
//   adminService, err := admin.New(oauthHttpClient)
package admin // import "google.golang.org/api/admin/directory/v1"

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

const apiId = "admin:directory_v1"
const apiName = "admin"
const apiVersion = "directory_v1"
const basePath = "https://www.googleapis.com/admin/directory/v1/"

// OAuth2 scopes used by this API.
const (
	// View and manage customer related information
	AdminDirectoryCustomerScope = "https://www.googleapis.com/auth/admin.directory.customer"

	// View customer related information
	AdminDirectoryCustomerReadonlyScope = "https://www.googleapis.com/auth/admin.directory.customer.readonly"

	// View and manage your Chrome OS devices' metadata
	AdminDirectoryDeviceChromeosScope = "https://www.googleapis.com/auth/admin.directory.device.chromeos"

	// View your Chrome OS devices' metadata
	AdminDirectoryDeviceChromeosReadonlyScope = "https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly"

	// View and manage your mobile devices' metadata
	AdminDirectoryDeviceMobileScope = "https://www.googleapis.com/auth/admin.directory.device.mobile"

	// Manage your mobile devices by performing administrative tasks
	AdminDirectoryDeviceMobileActionScope = "https://www.googleapis.com/auth/admin.directory.device.mobile.action"

	// View your mobile devices' metadata
	AdminDirectoryDeviceMobileReadonlyScope = "https://www.googleapis.com/auth/admin.directory.device.mobile.readonly"

	// View and manage the provisioning of domains for your customers
	AdminDirectoryDomainScope = "https://www.googleapis.com/auth/admin.directory.domain"

	// View domains related to your customers
	AdminDirectoryDomainReadonlyScope = "https://www.googleapis.com/auth/admin.directory.domain.readonly"

	// View and manage the provisioning of groups on your domain
	AdminDirectoryGroupScope = "https://www.googleapis.com/auth/admin.directory.group"

	// View and manage group subscriptions on your domain
	AdminDirectoryGroupMemberScope = "https://www.googleapis.com/auth/admin.directory.group.member"

	// View group subscriptions on your domain
	AdminDirectoryGroupMemberReadonlyScope = "https://www.googleapis.com/auth/admin.directory.group.member.readonly"

	// View groups on your domain
	AdminDirectoryGroupReadonlyScope = "https://www.googleapis.com/auth/admin.directory.group.readonly"

	// View and manage notifications received on your domain
	AdminDirectoryNotificationsScope = "https://www.googleapis.com/auth/admin.directory.notifications"

	// View and manage organization units on your domain
	AdminDirectoryOrgunitScope = "https://www.googleapis.com/auth/admin.directory.orgunit"

	// View organization units on your domain
	AdminDirectoryOrgunitReadonlyScope = "https://www.googleapis.com/auth/admin.directory.orgunit.readonly"

	// View and manage the provisioning of calendar resources on your domain
	AdminDirectoryResourceCalendarScope = "https://www.googleapis.com/auth/admin.directory.resource.calendar"

	// View calendar resources on your domain
	AdminDirectoryResourceCalendarReadonlyScope = "https://www.googleapis.com/auth/admin.directory.resource.calendar.readonly"

	// Manage delegated admin roles for your domain
	AdminDirectoryRolemanagementScope = "https://www.googleapis.com/auth/admin.directory.rolemanagement"

	// View delegated admin roles for your domain
	AdminDirectoryRolemanagementReadonlyScope = "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly"

	// View and manage the provisioning of users on your domain
	AdminDirectoryUserScope = "https://www.googleapis.com/auth/admin.directory.user"

	// View and manage user aliases on your domain
	AdminDirectoryUserAliasScope = "https://www.googleapis.com/auth/admin.directory.user.alias"

	// View user aliases on your domain
	AdminDirectoryUserAliasReadonlyScope = "https://www.googleapis.com/auth/admin.directory.user.alias.readonly"

	// View users on your domain
	AdminDirectoryUserReadonlyScope = "https://www.googleapis.com/auth/admin.directory.user.readonly"

	// Manage data access permissions for users on your domain
	AdminDirectoryUserSecurityScope = "https://www.googleapis.com/auth/admin.directory.user.security"

	// View and manage the provisioning of user schemas on your domain
	AdminDirectoryUserschemaScope = "https://www.googleapis.com/auth/admin.directory.userschema"

	// View user schemas on your domain
	AdminDirectoryUserschemaReadonlyScope = "https://www.googleapis.com/auth/admin.directory.userschema.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Asps = NewAspsService(s)
	s.Channels = NewChannelsService(s)
	s.Chromeosdevices = NewChromeosdevicesService(s)
	s.Customers = NewCustomersService(s)
	s.DomainAliases = NewDomainAliasesService(s)
	s.Domains = NewDomainsService(s)
	s.Groups = NewGroupsService(s)
	s.Members = NewMembersService(s)
	s.Mobiledevices = NewMobiledevicesService(s)
	s.Notifications = NewNotificationsService(s)
	s.Orgunits = NewOrgunitsService(s)
	s.Privileges = NewPrivilegesService(s)
	s.Resources = NewResourcesService(s)
	s.RoleAssignments = NewRoleAssignmentsService(s)
	s.Roles = NewRolesService(s)
	s.Schemas = NewSchemasService(s)
	s.Tokens = NewTokensService(s)
	s.Users = NewUsersService(s)
	s.VerificationCodes = NewVerificationCodesService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Asps *AspsService

	Channels *ChannelsService

	Chromeosdevices *ChromeosdevicesService

	Customers *CustomersService

	DomainAliases *DomainAliasesService

	Domains *DomainsService

	Groups *GroupsService

	Members *MembersService

	Mobiledevices *MobiledevicesService

	Notifications *NotificationsService

	Orgunits *OrgunitsService

	Privileges *PrivilegesService

	Resources *ResourcesService

	RoleAssignments *RoleAssignmentsService

	Roles *RolesService

	Schemas *SchemasService

	Tokens *TokensService

	Users *UsersService

	VerificationCodes *VerificationCodesService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewAspsService(s *Service) *AspsService {
	rs := &AspsService{s: s}
	return rs
}

type AspsService struct {
	s *Service
}

func NewChannelsService(s *Service) *ChannelsService {
	rs := &ChannelsService{s: s}
	return rs
}

type ChannelsService struct {
	s *Service
}

func NewChromeosdevicesService(s *Service) *ChromeosdevicesService {
	rs := &ChromeosdevicesService{s: s}
	return rs
}

type ChromeosdevicesService struct {
	s *Service
}

func NewCustomersService(s *Service) *CustomersService {
	rs := &CustomersService{s: s}
	return rs
}

type CustomersService struct {
	s *Service
}

func NewDomainAliasesService(s *Service) *DomainAliasesService {
	rs := &DomainAliasesService{s: s}
	return rs
}

type DomainAliasesService struct {
	s *Service
}

func NewDomainsService(s *Service) *DomainsService {
	rs := &DomainsService{s: s}
	return rs
}

type DomainsService struct {
	s *Service
}

func NewGroupsService(s *Service) *GroupsService {
	rs := &GroupsService{s: s}
	rs.Aliases = NewGroupsAliasesService(s)
	return rs
}

type GroupsService struct {
	s *Service

	Aliases *GroupsAliasesService
}

func NewGroupsAliasesService(s *Service) *GroupsAliasesService {
	rs := &GroupsAliasesService{s: s}
	return rs
}

type GroupsAliasesService struct {
	s *Service
}

func NewMembersService(s *Service) *MembersService {
	rs := &MembersService{s: s}
	return rs
}

type MembersService struct {
	s *Service
}

func NewMobiledevicesService(s *Service) *MobiledevicesService {
	rs := &MobiledevicesService{s: s}
	return rs
}

type MobiledevicesService struct {
	s *Service
}

func NewNotificationsService(s *Service) *NotificationsService {
	rs := &NotificationsService{s: s}
	return rs
}

type NotificationsService struct {
	s *Service
}

func NewOrgunitsService(s *Service) *OrgunitsService {
	rs := &OrgunitsService{s: s}
	return rs
}

type OrgunitsService struct {
	s *Service
}

func NewPrivilegesService(s *Service) *PrivilegesService {
	rs := &PrivilegesService{s: s}
	return rs
}

type PrivilegesService struct {
	s *Service
}

func NewResourcesService(s *Service) *ResourcesService {
	rs := &ResourcesService{s: s}
	rs.Calendars = NewResourcesCalendarsService(s)
	return rs
}

type ResourcesService struct {
	s *Service

	Calendars *ResourcesCalendarsService
}

func NewResourcesCalendarsService(s *Service) *ResourcesCalendarsService {
	rs := &ResourcesCalendarsService{s: s}
	return rs
}

type ResourcesCalendarsService struct {
	s *Service
}

func NewRoleAssignmentsService(s *Service) *RoleAssignmentsService {
	rs := &RoleAssignmentsService{s: s}
	return rs
}

type RoleAssignmentsService struct {
	s *Service
}

func NewRolesService(s *Service) *RolesService {
	rs := &RolesService{s: s}
	return rs
}

type RolesService struct {
	s *Service
}

func NewSchemasService(s *Service) *SchemasService {
	rs := &SchemasService{s: s}
	return rs
}

type SchemasService struct {
	s *Service
}

func NewTokensService(s *Service) *TokensService {
	rs := &TokensService{s: s}
	return rs
}

type TokensService struct {
	s *Service
}

func NewUsersService(s *Service) *UsersService {
	rs := &UsersService{s: s}
	rs.Aliases = NewUsersAliasesService(s)
	rs.Photos = NewUsersPhotosService(s)
	return rs
}

type UsersService struct {
	s *Service

	Aliases *UsersAliasesService

	Photos *UsersPhotosService
}

func NewUsersAliasesService(s *Service) *UsersAliasesService {
	rs := &UsersAliasesService{s: s}
	return rs
}

type UsersAliasesService struct {
	s *Service
}

func NewUsersPhotosService(s *Service) *UsersPhotosService {
	rs := &UsersPhotosService{s: s}
	return rs
}

type UsersPhotosService struct {
	s *Service
}

func NewVerificationCodesService(s *Service) *VerificationCodesService {
	rs := &VerificationCodesService{s: s}
	return rs
}

type VerificationCodesService struct {
	s *Service
}

// Alias: JSON template for Alias object in Directory API.
type Alias struct {
	// Alias: A alias email
	Alias string `json:"alias,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: Unique id of the group (Read-only) Unique id of the user
	// (Read-only)
	Id string `json:"id,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// PrimaryEmail: Group's primary email (Read-only) User's primary email
	// (Read-only)
	PrimaryEmail string `json:"primaryEmail,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Alias") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alias") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Alias) MarshalJSON() ([]byte, error) {
	type noMethod Alias
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Aliases: JSON response template to list aliases in Directory API.
type Aliases struct {
	// Aliases: List of alias objects.
	Aliases []interface{} `json:"aliases,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Aliases") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Aliases") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Aliases) MarshalJSON() ([]byte, error) {
	type noMethod Aliases
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Asp: The template that returns individual ASP (Access Code) data.
type Asp struct {
	// CodeId: The unique ID of the ASP.
	CodeId int64 `json:"codeId,omitempty"`

	// CreationTime: The time when the ASP was created. Expressed in Unix
	// time format.
	CreationTime int64 `json:"creationTime,omitempty,string"`

	// Etag: ETag of the ASP.
	Etag string `json:"etag,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#asp.
	Kind string `json:"kind,omitempty"`

	// LastTimeUsed: The time when the ASP was last used. Expressed in Unix
	// time format.
	LastTimeUsed int64 `json:"lastTimeUsed,omitempty,string"`

	// Name: The name of the application that the user, represented by their
	// userId, entered when the ASP was created.
	Name string `json:"name,omitempty"`

	// UserKey: The unique ID of the user who issued the ASP.
	UserKey string `json:"userKey,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CodeId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CodeId") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Asp) MarshalJSON() ([]byte, error) {
	type noMethod Asp
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Asps struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: A list of ASP resources.
	Items []*Asp `json:"items,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#aspList.
	Kind string `json:"kind,omitempty"`

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

func (s *Asps) MarshalJSON() ([]byte, error) {
	type noMethod Asps
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CalendarResource: JSON template for Calendar Resource object in
// Directory API.
type CalendarResource struct {
	// Etags: ETag of the resource.
	Etags string `json:"etags,omitempty"`

	// Kind: The type of the resource. For calendar resources, the value is
	// admin#directory#resources#calendars#CalendarResource.
	Kind string `json:"kind,omitempty"`

	// ResourceDescription: The brief description of the calendar resource.
	ResourceDescription string `json:"resourceDescription,omitempty"`

	// ResourceEmail: The read-only email ID for the calendar resource.
	// Generated as part of creating a new calendar resource.
	ResourceEmail string `json:"resourceEmail,omitempty"`

	// ResourceId: The unique ID for the calendar resource.
	ResourceId string `json:"resourceId,omitempty"`

	// ResourceName: The name of the calendar resource. For example,
	// Training Room 1A
	ResourceName string `json:"resourceName,omitempty"`

	// ResourceType: The type of the calendar resource. Used for grouping
	// resources in the calendar user interface.
	ResourceType string `json:"resourceType,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Etags") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Etags") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CalendarResource) MarshalJSON() ([]byte, error) {
	type noMethod CalendarResource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CalendarResources: JSON template for Calendar Resource List Response
// object in Directory API.
type CalendarResources struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: The CalendarResources in this page of results.
	Items []*CalendarResource `json:"items,omitempty"`

	// Kind: Identifies this as a collection of CalendarResources. This is
	// always admin#directory#resources#calendars#calendarResourcesList.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The continuation token, used to page through large
	// result sets. Provide this value in a subsequent request to return the
	// next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *CalendarResources) MarshalJSON() ([]byte, error) {
	type noMethod CalendarResources
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Channel: An notification channel used to watch for resource changes.
type Channel struct {
	// Address: The address where notifications are delivered for this
	// channel.
	Address string `json:"address,omitempty"`

	// Expiration: Date and time of notification channel expiration,
	// expressed as a Unix timestamp, in milliseconds. Optional.
	Expiration int64 `json:"expiration,omitempty,string"`

	// Id: A UUID or similar unique string that identifies this channel.
	Id string `json:"id,omitempty"`

	// Kind: Identifies this as a notification channel used to watch for
	// changes to a resource. Value: the fixed string "api#channel".
	Kind string `json:"kind,omitempty"`

	// Params: Additional parameters controlling delivery channel behavior.
	// Optional.
	Params map[string]string `json:"params,omitempty"`

	// Payload: A Boolean value to indicate whether payload is wanted.
	// Optional.
	Payload bool `json:"payload,omitempty"`

	// ResourceId: An opaque ID that identifies the resource being watched
	// on this channel. Stable across different API versions.
	ResourceId string `json:"resourceId,omitempty"`

	// ResourceUri: A version-specific identifier for the watched resource.
	ResourceUri string `json:"resourceUri,omitempty"`

	// Token: An arbitrary string delivered to the target address with each
	// notification delivered over this channel. Optional.
	Token string `json:"token,omitempty"`

	// Type: The type of delivery mechanism used for this channel.
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Address") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Address") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Channel) MarshalJSON() ([]byte, error) {
	type noMethod Channel
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ChromeOsDevice: JSON template for Chrome Os Device resource in
// Directory API.
type ChromeOsDevice struct {
	// ActiveTimeRanges: List of active time ranges (Read-only)
	ActiveTimeRanges []*ChromeOsDeviceActiveTimeRanges `json:"activeTimeRanges,omitempty"`

	// AnnotatedAssetId: AssetId specified during enrollment or through
	// later annotation
	AnnotatedAssetId string `json:"annotatedAssetId,omitempty"`

	// AnnotatedLocation: Address or location of the device as noted by the
	// administrator
	AnnotatedLocation string `json:"annotatedLocation,omitempty"`

	// AnnotatedUser: User of the device
	AnnotatedUser string `json:"annotatedUser,omitempty"`

	// BootMode: Chromebook boot mode (Read-only)
	BootMode string `json:"bootMode,omitempty"`

	// DeviceId: Unique identifier of Chrome OS Device (Read-only)
	DeviceId string `json:"deviceId,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// EthernetMacAddress: Chromebook Mac Address on ethernet network
	// interface (Read-only)
	EthernetMacAddress string `json:"ethernetMacAddress,omitempty"`

	// FirmwareVersion: Chromebook firmware version (Read-only)
	FirmwareVersion string `json:"firmwareVersion,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// LastEnrollmentTime: Date and time the device was last enrolled
	// (Read-only)
	LastEnrollmentTime string `json:"lastEnrollmentTime,omitempty"`

	// LastSync: Date and time the device was last synchronized with the
	// policy settings in the Google Apps administrator control panel
	// (Read-only)
	LastSync string `json:"lastSync,omitempty"`

	// MacAddress: Chromebook Mac Address on wifi network interface
	// (Read-only)
	MacAddress string `json:"macAddress,omitempty"`

	// Meid: Mobile Equipment identifier for the 3G mobile card in the
	// Chromebook (Read-only)
	Meid string `json:"meid,omitempty"`

	// Model: Chromebook Model (Read-only)
	Model string `json:"model,omitempty"`

	// Notes: Notes added by the administrator
	Notes string `json:"notes,omitempty"`

	// OrderNumber: Chromebook order number (Read-only)
	OrderNumber string `json:"orderNumber,omitempty"`

	// OrgUnitPath: OrgUnit of the device
	OrgUnitPath string `json:"orgUnitPath,omitempty"`

	// OsVersion: Chromebook Os Version (Read-only)
	OsVersion string `json:"osVersion,omitempty"`

	// PlatformVersion: Chromebook platform version (Read-only)
	PlatformVersion string `json:"platformVersion,omitempty"`

	// RecentUsers: List of recent device users, in descending order by last
	// login time (Read-only)
	RecentUsers []*ChromeOsDeviceRecentUsers `json:"recentUsers,omitempty"`

	// SerialNumber: Chromebook serial number (Read-only)
	SerialNumber string `json:"serialNumber,omitempty"`

	// Status: status of the device (Read-only)
	Status string `json:"status,omitempty"`

	// SupportEndDate: Final date the device will be supported (Read-only)
	SupportEndDate string `json:"supportEndDate,omitempty"`

	// WillAutoRenew: Will Chromebook auto renew after support end date
	// (Read-only)
	WillAutoRenew bool `json:"willAutoRenew,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ActiveTimeRanges") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ActiveTimeRanges") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ChromeOsDevice) MarshalJSON() ([]byte, error) {
	type noMethod ChromeOsDevice
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ChromeOsDeviceActiveTimeRanges struct {
	// ActiveTime: Duration in milliseconds
	ActiveTime int64 `json:"activeTime,omitempty"`

	// Date: Date of usage
	Date string `json:"date,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ActiveTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ActiveTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ChromeOsDeviceActiveTimeRanges) MarshalJSON() ([]byte, error) {
	type noMethod ChromeOsDeviceActiveTimeRanges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ChromeOsDeviceRecentUsers struct {
	// Email: Email address of the user. Present only if the user type is
	// managed
	Email string `json:"email,omitempty"`

	// Type: The type of the user
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Email") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Email") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ChromeOsDeviceRecentUsers) MarshalJSON() ([]byte, error) {
	type noMethod ChromeOsDeviceRecentUsers
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ChromeOsDeviceAction: JSON request template for firing actions on
// ChromeOs Device in Directory Devices API.
type ChromeOsDeviceAction struct {
	// Action: Action to be taken on the ChromeOs Device
	Action string `json:"action,omitempty"`

	DeprovisionReason string `json:"deprovisionReason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Action") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Action") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ChromeOsDeviceAction) MarshalJSON() ([]byte, error) {
	type noMethod ChromeOsDeviceAction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ChromeOsDevices: JSON response template for List Chrome OS Devices
// operation in Directory API.
type ChromeOsDevices struct {
	// Chromeosdevices: List of Chrome OS Device objects.
	Chromeosdevices []*ChromeOsDevice `json:"chromeosdevices,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access next page of this result.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Chromeosdevices") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Chromeosdevices") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ChromeOsDevices) MarshalJSON() ([]byte, error) {
	type noMethod ChromeOsDevices
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Customer: JSON template for Customer Resource object in Directory
// API.
type Customer struct {
	// AlternateEmail: The customer's secondary contact email address. This
	// email address cannot be on the same domain as the customerDomain
	AlternateEmail string `json:"alternateEmail,omitempty"`

	// CustomerCreationTime: The customer's creation time (Readonly)
	CustomerCreationTime string `json:"customerCreationTime,omitempty"`

	// CustomerDomain: The customer's primary domain name string. Do not
	// include the www prefix when creating a new customer.
	CustomerDomain string `json:"customerDomain,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: The unique ID for the customer's Google account. (Readonly)
	Id string `json:"id,omitempty"`

	// Kind: Identifies the resource as a customer. Value:
	// admin#directory#customer
	Kind string `json:"kind,omitempty"`

	// Language: The customer's ISO 639-2 language code. The default value
	// is en-US
	Language string `json:"language,omitempty"`

	// PhoneNumber: The customer's contact phone number in E.164 format.
	PhoneNumber string `json:"phoneNumber,omitempty"`

	// PostalAddress: The customer's postal address information.
	PostalAddress *CustomerPostalAddress `json:"postalAddress,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AlternateEmail") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateEmail") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Customer) MarshalJSON() ([]byte, error) {
	type noMethod Customer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CustomerPostalAddress: JSON template for postal address of a
// customer.
type CustomerPostalAddress struct {
	// AddressLine1: A customer's physical address. The address can be
	// composed of one to three lines.
	AddressLine1 string `json:"addressLine1,omitempty"`

	// AddressLine2: Address line 2 of the address.
	AddressLine2 string `json:"addressLine2,omitempty"`

	// AddressLine3: Address line 3 of the address.
	AddressLine3 string `json:"addressLine3,omitempty"`

	// ContactName: The customer contact's name.
	ContactName string `json:"contactName,omitempty"`

	// CountryCode: This is a required property. For countryCode information
	// see the ISO 3166 country code elements.
	CountryCode string `json:"countryCode,omitempty"`

	// Locality: Name of the locality. An example of a locality value is the
	// city of San Francisco.
	Locality string `json:"locality,omitempty"`

	// OrganizationName: The company or company division name.
	OrganizationName string `json:"organizationName,omitempty"`

	// PostalCode: The postal code. A postalCode example is a postal zip
	// code such as 10009. This is in accordance with -
	// http://portablecontacts.net/draft-spec.html#address_element.
	PostalCode string `json:"postalCode,omitempty"`

	// Region: Name of the region. An example of a region value is NY for
	// the state of New York.
	Region string `json:"region,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AddressLine1") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AddressLine1") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CustomerPostalAddress) MarshalJSON() ([]byte, error) {
	type noMethod CustomerPostalAddress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DomainAlias: JSON template for Domain Alias object in Directory API.
type DomainAlias struct {
	// CreationTime: The creation time of the domain alias. (Read-only).
	CreationTime int64 `json:"creationTime,omitempty,string"`

	// DomainAliasName: The domain alias name.
	DomainAliasName string `json:"domainAliasName,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// ParentDomainName: The parent domain name that the domain alias is
	// associated with. This can either be a primary or secondary domain
	// name within a customer.
	ParentDomainName string `json:"parentDomainName,omitempty"`

	// Verified: Indicates the verification state of a domain alias.
	// (Read-only)
	Verified bool `json:"verified,omitempty"`

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

func (s *DomainAlias) MarshalJSON() ([]byte, error) {
	type noMethod DomainAlias
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DomainAliases: JSON response template to list domain aliases in
// Directory API.
type DomainAliases struct {
	// DomainAliases: List of domain alias objects.
	DomainAliases []*DomainAlias `json:"domainAliases,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DomainAliases") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DomainAliases") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DomainAliases) MarshalJSON() ([]byte, error) {
	type noMethod DomainAliases
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Domains: JSON template for Domain object in Directory API.
type Domains struct {
	// CreationTime: Creation time of the domain. (Read-only).
	CreationTime int64 `json:"creationTime,omitempty,string"`

	// DomainAliases: List of domain alias objects. (Read-only)
	DomainAliases []*DomainAlias `json:"domainAliases,omitempty"`

	// DomainName: The domain name of the customer.
	DomainName string `json:"domainName,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// IsPrimary: Indicates if the domain is a primary domain (Read-only).
	IsPrimary bool `json:"isPrimary,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Verified: Indicates the verification state of a domain. (Read-only).
	Verified bool `json:"verified,omitempty"`

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

func (s *Domains) MarshalJSON() ([]byte, error) {
	type noMethod Domains
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Domains2: JSON response template to list Domains in Directory API.
type Domains2 struct {
	// Domains: List of domain objects.
	Domains []*Domains `json:"domains,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Domains") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Domains") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Domains2) MarshalJSON() ([]byte, error) {
	type noMethod Domains2
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Group: JSON template for Group resource in Directory API.
type Group struct {
	// AdminCreated: Is the group created by admin (Read-only) *
	AdminCreated bool `json:"adminCreated,omitempty"`

	// Aliases: List of aliases (Read-only)
	Aliases []string `json:"aliases,omitempty"`

	// Description: Description of the group
	Description string `json:"description,omitempty"`

	// DirectMembersCount: Group direct members count
	DirectMembersCount int64 `json:"directMembersCount,omitempty,string"`

	// Email: Email of Group
	Email string `json:"email,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: Unique identifier of Group (Read-only)
	Id string `json:"id,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Name: Group name
	Name string `json:"name,omitempty"`

	// NonEditableAliases: List of non editable aliases (Read-only)
	NonEditableAliases []string `json:"nonEditableAliases,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AdminCreated") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdminCreated") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Group) MarshalJSON() ([]byte, error) {
	type noMethod Group
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Groups: JSON response template for List Groups operation in Directory
// API.
type Groups struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Groups: List of group objects.
	Groups []*Group `json:"groups,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access next page of this result.
	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *Groups) MarshalJSON() ([]byte, error) {
	type noMethod Groups
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Member: JSON template for Member resource in Directory API.
type Member struct {
	// Email: Email of member (Read-only)
	Email string `json:"email,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: Unique identifier of customer member (Read-only) Unique
	// identifier of group (Read-only) Unique identifier of member
	// (Read-only)
	Id string `json:"id,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Role: Role of member
	Role string `json:"role,omitempty"`

	// Status: Status of member (Immutable)
	Status string `json:"status,omitempty"`

	// Type: Type of member (Immutable)
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Email") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Email") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Member) MarshalJSON() ([]byte, error) {
	type noMethod Member
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Members: JSON response template for List Members operation in
// Directory API.
type Members struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Members: List of member objects.
	Members []*Member `json:"members,omitempty"`

	// NextPageToken: Token used to access next page of this result.
	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *Members) MarshalJSON() ([]byte, error) {
	type noMethod Members
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MobileDevice: JSON template for Mobile Device resource in Directory
// API.
type MobileDevice struct {
	// AdbStatus: Adb (USB debugging) enabled or disabled on device
	// (Read-only)
	AdbStatus bool `json:"adbStatus,omitempty"`

	// Applications: List of applications installed on Mobile Device
	Applications []*MobileDeviceApplications `json:"applications,omitempty"`

	// BasebandVersion: Mobile Device Baseband version (Read-only)
	BasebandVersion string `json:"basebandVersion,omitempty"`

	// BootloaderVersion: Mobile Device Bootloader version (Read-only)
	BootloaderVersion string `json:"bootloaderVersion,omitempty"`

	// Brand: Mobile Device Brand (Read-only)
	Brand string `json:"brand,omitempty"`

	// BuildNumber: Mobile Device Build number (Read-only)
	BuildNumber string `json:"buildNumber,omitempty"`

	// DefaultLanguage: The default locale used on the Mobile Device
	// (Read-only)
	DefaultLanguage string `json:"defaultLanguage,omitempty"`

	// DeveloperOptionsStatus: Developer options enabled or disabled on
	// device (Read-only)
	DeveloperOptionsStatus bool `json:"developerOptionsStatus,omitempty"`

	// DeviceCompromisedStatus: Mobile Device compromised status (Read-only)
	DeviceCompromisedStatus string `json:"deviceCompromisedStatus,omitempty"`

	// DeviceId: Mobile Device serial number (Read-only)
	DeviceId string `json:"deviceId,omitempty"`

	// DevicePasswordStatus: DevicePasswordStatus (Read-only)
	DevicePasswordStatus string `json:"devicePasswordStatus,omitempty"`

	// Email: List of owner user's email addresses (Read-only)
	Email []string `json:"email,omitempty"`

	// EncryptionStatus: Mobile Device Encryption Status (Read-only)
	EncryptionStatus string `json:"encryptionStatus,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// FirstSync: Date and time the device was first synchronized with the
	// policy settings in the Google Apps administrator control panel
	// (Read-only)
	FirstSync string `json:"firstSync,omitempty"`

	// Hardware: Mobile Device Hardware (Read-only)
	Hardware string `json:"hardware,omitempty"`

	// HardwareId: Mobile Device Hardware Id (Read-only)
	HardwareId string `json:"hardwareId,omitempty"`

	// Imei: Mobile Device IMEI number (Read-only)
	Imei string `json:"imei,omitempty"`

	// KernelVersion: Mobile Device Kernel version (Read-only)
	KernelVersion string `json:"kernelVersion,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// LastSync: Date and time the device was last synchronized with the
	// policy settings in the Google Apps administrator control panel
	// (Read-only)
	LastSync string `json:"lastSync,omitempty"`

	// ManagedAccountIsOnOwnerProfile: Boolean indicating if this account is
	// on owner/primary profile or not (Read-only)
	ManagedAccountIsOnOwnerProfile bool `json:"managedAccountIsOnOwnerProfile,omitempty"`

	// Manufacturer: Mobile Device manufacturer (Read-only)
	Manufacturer string `json:"manufacturer,omitempty"`

	// Meid: Mobile Device MEID number (Read-only)
	Meid string `json:"meid,omitempty"`

	// Model: Name of the model of the device
	Model string `json:"model,omitempty"`

	// Name: List of owner user's names (Read-only)
	Name []string `json:"name,omitempty"`

	// NetworkOperator: Mobile Device mobile or network operator (if
	// available) (Read-only)
	NetworkOperator string `json:"networkOperator,omitempty"`

	// Os: Name of the mobile operating system
	Os string `json:"os,omitempty"`

	// OtherAccountsInfo: List of accounts added on device (Read-only)
	OtherAccountsInfo []string `json:"otherAccountsInfo,omitempty"`

	// Privilege: DMAgentPermission (Read-only)
	Privilege string `json:"privilege,omitempty"`

	// ReleaseVersion: Mobile Device release version version (Read-only)
	ReleaseVersion string `json:"releaseVersion,omitempty"`

	// ResourceId: Unique identifier of Mobile Device (Read-only)
	ResourceId string `json:"resourceId,omitempty"`

	// SecurityPatchLevel: Mobile Device Security patch level (Read-only)
	SecurityPatchLevel int64 `json:"securityPatchLevel,omitempty,string"`

	// SerialNumber: Mobile Device SSN or Serial Number (Read-only)
	SerialNumber string `json:"serialNumber,omitempty"`

	// Status: Status of the device (Read-only)
	Status string `json:"status,omitempty"`

	// SupportsWorkProfile: Work profile supported on device (Read-only)
	SupportsWorkProfile bool `json:"supportsWorkProfile,omitempty"`

	// Type: The type of device (Read-only)
	Type string `json:"type,omitempty"`

	// UnknownSourcesStatus: Unknown sources enabled or disabled on device
	// (Read-only)
	UnknownSourcesStatus bool `json:"unknownSourcesStatus,omitempty"`

	// UserAgent: Mobile Device user agent
	UserAgent string `json:"userAgent,omitempty"`

	// WifiMacAddress: Mobile Device WiFi MAC address (Read-only)
	WifiMacAddress string `json:"wifiMacAddress,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AdbStatus") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdbStatus") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MobileDevice) MarshalJSON() ([]byte, error) {
	type noMethod MobileDevice
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MobileDeviceApplications struct {
	// DisplayName: Display name of application
	DisplayName string `json:"displayName,omitempty"`

	// PackageName: Package name of application
	PackageName string `json:"packageName,omitempty"`

	// Permission: List of Permissions for application
	Permission []string `json:"permission,omitempty"`

	// VersionCode: Version code of application
	VersionCode int64 `json:"versionCode,omitempty"`

	// VersionName: Version name of application
	VersionName string `json:"versionName,omitempty"`

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

func (s *MobileDeviceApplications) MarshalJSON() ([]byte, error) {
	type noMethod MobileDeviceApplications
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MobileDeviceAction: JSON request template for firing commands on
// Mobile Device in Directory Devices API.
type MobileDeviceAction struct {
	// Action: Action to be taken on the Mobile Device
	Action string `json:"action,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Action") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Action") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MobileDeviceAction) MarshalJSON() ([]byte, error) {
	type noMethod MobileDeviceAction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MobileDevices: JSON response template for List Mobile Devices
// operation in Directory API.
type MobileDevices struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Mobiledevices: List of Mobile Device objects.
	Mobiledevices []*MobileDevice `json:"mobiledevices,omitempty"`

	// NextPageToken: Token used to access next page of this result.
	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *MobileDevices) MarshalJSON() ([]byte, error) {
	type noMethod MobileDevices
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Notification: Template for a notification resource.
type Notification struct {
	// Body: Body of the notification (Read-only)
	Body string `json:"body,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// FromAddress: Address from which the notification is received
	// (Read-only)
	FromAddress string `json:"fromAddress,omitempty"`

	// IsUnread: Boolean indicating whether the notification is unread or
	// not.
	IsUnread bool `json:"isUnread,omitempty"`

	// Kind: The type of the resource.
	Kind string `json:"kind,omitempty"`

	NotificationId string `json:"notificationId,omitempty"`

	// SendTime: Time at which notification was sent (Read-only)
	SendTime string `json:"sendTime,omitempty"`

	// Subject: Subject of the notification (Read-only)
	Subject string `json:"subject,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Body") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Body") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Notification) MarshalJSON() ([]byte, error) {
	type noMethod Notification
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Notifications: Template for notifications list response.
type Notifications struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: List of notifications in this page.
	Items []*Notification `json:"items,omitempty"`

	// Kind: The type of the resource.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token for fetching the next page of notifications.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// UnreadNotificationsCount: Number of unread notification for the
	// domain.
	UnreadNotificationsCount int64 `json:"unreadNotificationsCount,omitempty"`

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

func (s *Notifications) MarshalJSON() ([]byte, error) {
	type noMethod Notifications
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OrgUnit: JSON template for Org Unit resource in Directory API.
type OrgUnit struct {
	// BlockInheritance: Should block inheritance
	BlockInheritance bool `json:"blockInheritance,omitempty"`

	// Description: Description of OrgUnit
	Description string `json:"description,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Name: Name of OrgUnit
	Name string `json:"name,omitempty"`

	// OrgUnitId: Id of OrgUnit
	OrgUnitId string `json:"orgUnitId,omitempty"`

	// OrgUnitPath: Path of OrgUnit
	OrgUnitPath string `json:"orgUnitPath,omitempty"`

	// ParentOrgUnitId: Id of parent OrgUnit
	ParentOrgUnitId string `json:"parentOrgUnitId,omitempty"`

	// ParentOrgUnitPath: Path of parent OrgUnit
	ParentOrgUnitPath string `json:"parentOrgUnitPath,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BlockInheritance") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BlockInheritance") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *OrgUnit) MarshalJSON() ([]byte, error) {
	type noMethod OrgUnit
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OrgUnits: JSON response template for List Organization Units
// operation in Directory API.
type OrgUnits struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// OrganizationUnits: List of user objects.
	OrganizationUnits []*OrgUnit `json:"organizationUnits,omitempty"`

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

func (s *OrgUnits) MarshalJSON() ([]byte, error) {
	type noMethod OrgUnits
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Privilege: JSON template for privilege resource in Directory API.
type Privilege struct {
	// ChildPrivileges: A list of child privileges. Privileges for a service
	// form a tree. Each privilege can have a list of child privileges; this
	// list is empty for a leaf privilege.
	ChildPrivileges []*Privilege `json:"childPrivileges,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// IsOuScopable: If the privilege can be restricted to an organization
	// unit.
	IsOuScopable bool `json:"isOuScopable,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#privilege.
	Kind string `json:"kind,omitempty"`

	// PrivilegeName: The name of the privilege.
	PrivilegeName string `json:"privilegeName,omitempty"`

	// ServiceId: The obfuscated ID of the service this privilege is for.
	ServiceId string `json:"serviceId,omitempty"`

	// ServiceName: The name of the service this privilege is for.
	ServiceName string `json:"serviceName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ChildPrivileges") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ChildPrivileges") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Privilege) MarshalJSON() ([]byte, error) {
	type noMethod Privilege
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Privileges: JSON response template for List privileges operation in
// Directory API.
type Privileges struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: A list of Privilege resources.
	Items []*Privilege `json:"items,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#privileges.
	Kind string `json:"kind,omitempty"`

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

func (s *Privileges) MarshalJSON() ([]byte, error) {
	type noMethod Privileges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Role: JSON template for role resource in Directory API.
type Role struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// IsSuperAdminRole: Returns true if the role is a super admin role.
	IsSuperAdminRole bool `json:"isSuperAdminRole,omitempty"`

	// IsSystemRole: Returns true if this is a pre-defined system role.
	IsSystemRole bool `json:"isSystemRole,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#role.
	Kind string `json:"kind,omitempty"`

	// RoleDescription: A short description of the role.
	RoleDescription string `json:"roleDescription,omitempty"`

	// RoleId: ID of the role.
	RoleId int64 `json:"roleId,omitempty,string"`

	// RoleName: Name of the role.
	RoleName string `json:"roleName,omitempty"`

	// RolePrivileges: The set of privileges that are granted to this role.
	RolePrivileges []*RoleRolePrivileges `json:"rolePrivileges,omitempty"`

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

func (s *Role) MarshalJSON() ([]byte, error) {
	type noMethod Role
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type RoleRolePrivileges struct {
	// PrivilegeName: The name of the privilege.
	PrivilegeName string `json:"privilegeName,omitempty"`

	// ServiceId: The obfuscated ID of the service this privilege is for.
	ServiceId string `json:"serviceId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PrivilegeName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PrivilegeName") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RoleRolePrivileges) MarshalJSON() ([]byte, error) {
	type noMethod RoleRolePrivileges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RoleAssignment: JSON template for roleAssignment resource in
// Directory API.
type RoleAssignment struct {
	// AssignedTo: The unique ID of the user this role is assigned to.
	AssignedTo string `json:"assignedTo,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#roleAssignment.
	Kind string `json:"kind,omitempty"`

	// OrgUnitId: If the role is restricted to an organization unit, this
	// contains the ID for the organization unit the exercise of this role
	// is restricted to.
	OrgUnitId string `json:"orgUnitId,omitempty"`

	// RoleAssignmentId: ID of this roleAssignment.
	RoleAssignmentId int64 `json:"roleAssignmentId,omitempty,string"`

	// RoleId: The ID of the role that is assigned.
	RoleId int64 `json:"roleId,omitempty,string"`

	// ScopeType: The scope in which this role is assigned. Possible values
	// are:
	// - CUSTOMER
	// - ORG_UNIT
	ScopeType string `json:"scopeType,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AssignedTo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AssignedTo") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RoleAssignment) MarshalJSON() ([]byte, error) {
	type noMethod RoleAssignment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RoleAssignments: JSON response template for List roleAssignments
// operation in Directory API.
type RoleAssignments struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: A list of RoleAssignment resources.
	Items []*RoleAssignment `json:"items,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#roleAssignments.
	Kind string `json:"kind,omitempty"`

	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *RoleAssignments) MarshalJSON() ([]byte, error) {
	type noMethod RoleAssignments
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Roles: JSON response template for List roles operation in Directory
// API.
type Roles struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: A list of Role resources.
	Items []*Role `json:"items,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#roles.
	Kind string `json:"kind,omitempty"`

	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *Roles) MarshalJSON() ([]byte, error) {
	type noMethod Roles
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Schema: JSON template for Schema resource in Directory API.
type Schema struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Fields: Fields of Schema
	Fields []*SchemaFieldSpec `json:"fields,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// SchemaId: Unique identifier of Schema (Read-only)
	SchemaId string `json:"schemaId,omitempty"`

	// SchemaName: Schema name
	SchemaName string `json:"schemaName,omitempty"`

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

func (s *Schema) MarshalJSON() ([]byte, error) {
	type noMethod Schema
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SchemaFieldSpec: JSON template for FieldSpec resource for Schemas in
// Directory API.
type SchemaFieldSpec struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// FieldId: Unique identifier of Field (Read-only)
	FieldId string `json:"fieldId,omitempty"`

	// FieldName: Name of the field.
	FieldName string `json:"fieldName,omitempty"`

	// FieldType: Type of the field.
	FieldType string `json:"fieldType,omitempty"`

	// Indexed: Boolean specifying whether the field is indexed or not.
	//
	// Default: true
	Indexed *bool `json:"indexed,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// MultiValued: Boolean specifying whether this is a multi-valued field
	// or not.
	MultiValued bool `json:"multiValued,omitempty"`

	// NumericIndexingSpec: Indexing spec for a numeric field. By default,
	// only exact match queries will be supported for numeric fields.
	// Setting the numericIndexingSpec allows range queries to be supported.
	NumericIndexingSpec *SchemaFieldSpecNumericIndexingSpec `json:"numericIndexingSpec,omitempty"`

	// ReadAccessType: Read ACLs on the field specifying who can view values
	// of this field. Valid values are "ALL_DOMAIN_USERS" and
	// "ADMINS_AND_SELF".
	ReadAccessType string `json:"readAccessType,omitempty"`

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

func (s *SchemaFieldSpec) MarshalJSON() ([]byte, error) {
	type noMethod SchemaFieldSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SchemaFieldSpecNumericIndexingSpec: Indexing spec for a numeric
// field. By default, only exact match queries will be supported for
// numeric fields. Setting the numericIndexingSpec allows range queries
// to be supported.
type SchemaFieldSpecNumericIndexingSpec struct {
	// MaxValue: Maximum value of this field. This is meant to be indicative
	// rather than enforced. Values outside this range will still be
	// indexed, but search may not be as performant.
	MaxValue float64 `json:"maxValue,omitempty"`

	// MinValue: Minimum value of this field. This is meant to be indicative
	// rather than enforced. Values outside this range will still be
	// indexed, but search may not be as performant.
	MinValue float64 `json:"minValue,omitempty"`

	// ForceSendFields is a list of field names (e.g. "MaxValue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MaxValue") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SchemaFieldSpecNumericIndexingSpec) MarshalJSON() ([]byte, error) {
	type noMethod SchemaFieldSpecNumericIndexingSpec
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Schemas: JSON response template for List Schema operation in
// Directory API.
type Schemas struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// Schemas: List of UserSchema objects.
	Schemas []*Schema `json:"schemas,omitempty"`

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

func (s *Schemas) MarshalJSON() ([]byte, error) {
	type noMethod Schemas
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Token: JSON template for token resource in Directory API.
type Token struct {
	// Anonymous: Whether the application is registered with Google. The
	// value is true if the application has an anonymous Client ID.
	Anonymous bool `json:"anonymous,omitempty"`

	// ClientId: The Client ID of the application the token is issued to.
	ClientId string `json:"clientId,omitempty"`

	// DisplayText: The displayable name of the application the token is
	// issued to.
	DisplayText string `json:"displayText,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#token.
	Kind string `json:"kind,omitempty"`

	// NativeApp: Whether the token is issued to an installed application.
	// The value is true if the application is installed to a desktop or
	// mobile device.
	NativeApp bool `json:"nativeApp,omitempty"`

	// Scopes: A list of authorization scopes the application is granted.
	Scopes []string `json:"scopes,omitempty"`

	// UserKey: The unique ID of the user that issued the token.
	UserKey string `json:"userKey,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Anonymous") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Anonymous") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Token) MarshalJSON() ([]byte, error) {
	type noMethod Token
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Tokens: JSON response template for List tokens operation in Directory
// API.
type Tokens struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: A list of Token resources.
	Items []*Token `json:"items,omitempty"`

	// Kind: The type of the API resource. This is always
	// admin#directory#tokenList.
	Kind string `json:"kind,omitempty"`

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

func (s *Tokens) MarshalJSON() ([]byte, error) {
	type noMethod Tokens
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// User: JSON template for User object in Directory API.
type User struct {
	Addresses interface{} `json:"addresses,omitempty"`

	// AgreedToTerms: Indicates if user has agreed to terms (Read-only)
	AgreedToTerms bool `json:"agreedToTerms,omitempty"`

	// Aliases: List of aliases (Read-only)
	Aliases []string `json:"aliases,omitempty"`

	// ChangePasswordAtNextLogin: Boolean indicating if the user should
	// change password in next login
	ChangePasswordAtNextLogin bool `json:"changePasswordAtNextLogin,omitempty"`

	// CreationTime: User's Google account creation time. (Read-only)
	CreationTime string `json:"creationTime,omitempty"`

	// CustomSchemas: Custom fields of the user.
	CustomSchemas map[string]googleapi.RawMessage `json:"customSchemas,omitempty"`

	// CustomerId: CustomerId of User (Read-only)
	CustomerId string `json:"customerId,omitempty"`

	DeletionTime string `json:"deletionTime,omitempty"`

	Emails interface{} `json:"emails,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	ExternalIds interface{} `json:"externalIds,omitempty"`

	// HashFunction: Hash function name for password. Supported are MD5,
	// SHA-1 and crypt
	HashFunction string `json:"hashFunction,omitempty"`

	// Id: Unique identifier of User (Read-only)
	Id string `json:"id,omitempty"`

	Ims interface{} `json:"ims,omitempty"`

	// IncludeInGlobalAddressList: Boolean indicating if user is included in
	// Global Address List
	IncludeInGlobalAddressList bool `json:"includeInGlobalAddressList,omitempty"`

	// IpWhitelisted: Boolean indicating if ip is whitelisted
	IpWhitelisted bool `json:"ipWhitelisted,omitempty"`

	// IsAdmin: Boolean indicating if the user is admin (Read-only)
	IsAdmin bool `json:"isAdmin,omitempty"`

	// IsDelegatedAdmin: Boolean indicating if the user is delegated admin
	// (Read-only)
	IsDelegatedAdmin bool `json:"isDelegatedAdmin,omitempty"`

	// IsMailboxSetup: Is mailbox setup (Read-only)
	IsMailboxSetup bool `json:"isMailboxSetup,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// LastLoginTime: User's last login time. (Read-only)
	LastLoginTime string `json:"lastLoginTime,omitempty"`

	// Name: User's name
	Name *UserName `json:"name,omitempty"`

	// NonEditableAliases: List of non editable aliases (Read-only)
	NonEditableAliases []string `json:"nonEditableAliases,omitempty"`

	Notes interface{} `json:"notes,omitempty"`

	// OrgUnitPath: OrgUnit of User
	OrgUnitPath string `json:"orgUnitPath,omitempty"`

	Organizations interface{} `json:"organizations,omitempty"`

	// Password: User's password
	Password string `json:"password,omitempty"`

	Phones interface{} `json:"phones,omitempty"`

	// PrimaryEmail: username of User
	PrimaryEmail string `json:"primaryEmail,omitempty"`

	Relations interface{} `json:"relations,omitempty"`

	// Suspended: Indicates if user is suspended
	Suspended bool `json:"suspended,omitempty"`

	// SuspensionReason: Suspension reason if user is suspended (Read-only)
	SuspensionReason string `json:"suspensionReason,omitempty"`

	// ThumbnailPhotoEtag: ETag of the user's photo (Read-only)
	ThumbnailPhotoEtag string `json:"thumbnailPhotoEtag,omitempty"`

	// ThumbnailPhotoUrl: Photo Url of the user (Read-only)
	ThumbnailPhotoUrl string `json:"thumbnailPhotoUrl,omitempty"`

	Websites interface{} `json:"websites,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Addresses") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Addresses") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *User) MarshalJSON() ([]byte, error) {
	type noMethod User
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserAbout: JSON template for About (notes) of a user in Directory
// API.
type UserAbout struct {
	// ContentType: About entry can have a type which indicates the content
	// type. It can either be plain or html. By default, notes contents are
	// assumed to contain plain text.
	ContentType string `json:"contentType,omitempty"`

	// Value: Actual value of notes.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContentType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContentType") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserAbout) MarshalJSON() ([]byte, error) {
	type noMethod UserAbout
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserAddress: JSON template for address.
type UserAddress struct {
	// Country: Country.
	Country string `json:"country,omitempty"`

	// CountryCode: Country code.
	CountryCode string `json:"countryCode,omitempty"`

	// CustomType: Custom type.
	CustomType string `json:"customType,omitempty"`

	// ExtendedAddress: Extended Address.
	ExtendedAddress string `json:"extendedAddress,omitempty"`

	// Formatted: Formatted address.
	Formatted string `json:"formatted,omitempty"`

	// Locality: Locality.
	Locality string `json:"locality,omitempty"`

	// PoBox: Other parts of address.
	PoBox string `json:"poBox,omitempty"`

	// PostalCode: Postal code.
	PostalCode string `json:"postalCode,omitempty"`

	// Primary: If this is user's primary address. Only one entry could be
	// marked as primary.
	Primary bool `json:"primary,omitempty"`

	// Region: Region.
	Region string `json:"region,omitempty"`

	// SourceIsStructured: User supplied address was structured. Structured
	// addresses are NOT supported at this time. You might be able to write
	// structured addresses, but any values will eventually be clobbered.
	SourceIsStructured bool `json:"sourceIsStructured,omitempty"`

	// StreetAddress: Street.
	StreetAddress string `json:"streetAddress,omitempty"`

	// Type: Each entry can have a type which indicates standard values of
	// that entry. For example address could be of home, work etc. In
	// addition to the standard type, an entry can have a custom type and
	// can take any value. Such type should have the CUSTOM value as type
	// and also have a customType value.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Country") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Country") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserAddress) MarshalJSON() ([]byte, error) {
	type noMethod UserAddress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserEmail: JSON template for an email.
type UserEmail struct {
	// Address: Email id of the user.
	Address string `json:"address,omitempty"`

	// CustomType: Custom Type.
	CustomType string `json:"customType,omitempty"`

	// Primary: If this is user's primary email. Only one entry could be
	// marked as primary.
	Primary bool `json:"primary,omitempty"`

	// Type: Each entry can have a type which indicates standard types of
	// that entry. For example email could be of home, work etc. In addition
	// to the standard type, an entry can have a custom type and can take
	// any value Such types should have the CUSTOM value as type and also
	// have a customType value.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Address") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Address") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserEmail) MarshalJSON() ([]byte, error) {
	type noMethod UserEmail
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserExternalId: JSON template for an externalId entry.
type UserExternalId struct {
	// CustomType: Custom type.
	CustomType string `json:"customType,omitempty"`

	// Type: The type of the Id.
	Type string `json:"type,omitempty"`

	// Value: The value of the id.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CustomType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CustomType") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserExternalId) MarshalJSON() ([]byte, error) {
	type noMethod UserExternalId
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserIm: JSON template for instant messenger of an user.
type UserIm struct {
	// CustomProtocol: Custom protocol.
	CustomProtocol string `json:"customProtocol,omitempty"`

	// CustomType: Custom type.
	CustomType string `json:"customType,omitempty"`

	// Im: Instant messenger id.
	Im string `json:"im,omitempty"`

	// Primary: If this is user's primary im. Only one entry could be marked
	// as primary.
	Primary bool `json:"primary,omitempty"`

	// Protocol: Protocol used in the instant messenger. It should be one of
	// the values from ImProtocolTypes map. Similar to type, it can take a
	// CUSTOM value and specify the custom name in customProtocol field.
	Protocol string `json:"protocol,omitempty"`

	// Type: Each entry can have a type which indicates standard types of
	// that entry. For example instant messengers could be of home, work
	// etc. In addition to the standard type, an entry can have a custom
	// type and can take any value. Such types should have the CUSTOM value
	// as type and also have a customType value.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CustomProtocol") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CustomProtocol") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *UserIm) MarshalJSON() ([]byte, error) {
	type noMethod UserIm
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserMakeAdmin: JSON request template for setting/revoking admin
// status of a user in Directory API.
type UserMakeAdmin struct {
	// Status: Boolean indicating new admin status of the user
	Status bool `json:"status,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Status") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Status") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserMakeAdmin) MarshalJSON() ([]byte, error) {
	type noMethod UserMakeAdmin
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserName: JSON template for name of a user in Directory API.
type UserName struct {
	// FamilyName: Last Name
	FamilyName string `json:"familyName,omitempty"`

	// FullName: Full Name
	FullName string `json:"fullName,omitempty"`

	// GivenName: First Name
	GivenName string `json:"givenName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FamilyName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FamilyName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserName) MarshalJSON() ([]byte, error) {
	type noMethod UserName
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserOrganization: JSON template for an organization entry.
type UserOrganization struct {
	// CostCenter: The cost center of the users department.
	CostCenter string `json:"costCenter,omitempty"`

	// CustomType: Custom type.
	CustomType string `json:"customType,omitempty"`

	// Department: Department within the organization.
	Department string `json:"department,omitempty"`

	// Description: Description of the organization.
	Description string `json:"description,omitempty"`

	// Domain: The domain to which the organization belongs to.
	Domain string `json:"domain,omitempty"`

	// Location: Location of the organization. This need not be fully
	// qualified address.
	Location string `json:"location,omitempty"`

	// Name: Name of the organization
	Name string `json:"name,omitempty"`

	// Primary: If it user's primary organization.
	Primary bool `json:"primary,omitempty"`

	// Symbol: Symbol of the organization.
	Symbol string `json:"symbol,omitempty"`

	// Title: Title (designation) of the user in the organization.
	Title string `json:"title,omitempty"`

	// Type: Each entry can have a type which indicates standard types of
	// that entry. For example organization could be of school, work etc. In
	// addition to the standard type, an entry can have a custom type and
	// can give it any name. Such types should have the CUSTOM value as type
	// and also have a CustomType value.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CostCenter") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CostCenter") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserOrganization) MarshalJSON() ([]byte, error) {
	type noMethod UserOrganization
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserPhone: JSON template for a phone entry.
type UserPhone struct {
	// CustomType: Custom Type.
	CustomType string `json:"customType,omitempty"`

	// Primary: If this is user's primary phone or not.
	Primary bool `json:"primary,omitempty"`

	// Type: Each entry can have a type which indicates standard types of
	// that entry. For example phone could be of home_fax, work, mobile etc.
	// In addition to the standard type, an entry can have a custom type and
	// can give it any name. Such types should have the CUSTOM value as type
	// and also have a customType value.
	Type string `json:"type,omitempty"`

	// Value: Phone number.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CustomType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CustomType") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserPhone) MarshalJSON() ([]byte, error) {
	type noMethod UserPhone
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserPhoto: JSON template for Photo object in Directory API.
type UserPhoto struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Height: Height in pixels of the photo
	Height int64 `json:"height,omitempty"`

	// Id: Unique identifier of User (Read-only)
	Id string `json:"id,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// MimeType: Mime Type of the photo
	MimeType string `json:"mimeType,omitempty"`

	// PhotoData: Base64 encoded photo data
	PhotoData string `json:"photoData,omitempty"`

	// PrimaryEmail: Primary email of User (Read-only)
	PrimaryEmail string `json:"primaryEmail,omitempty"`

	// Width: Width in pixels of the photo
	Width int64 `json:"width,omitempty"`

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

func (s *UserPhoto) MarshalJSON() ([]byte, error) {
	type noMethod UserPhoto
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserRelation: JSON template for a relation entry.
type UserRelation struct {
	// CustomType: Custom Type.
	CustomType string `json:"customType,omitempty"`

	// Type: The relation of the user. Some of the possible values are
	// mother, father, sister, brother, manager, assistant, partner.
	Type string `json:"type,omitempty"`

	// Value: The name of the relation.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CustomType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CustomType") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserRelation) MarshalJSON() ([]byte, error) {
	type noMethod UserRelation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserUndelete: JSON request template to undelete a user in Directory
// API.
type UserUndelete struct {
	// OrgUnitPath: OrgUnit of User
	OrgUnitPath string `json:"orgUnitPath,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OrgUnitPath") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OrgUnitPath") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserUndelete) MarshalJSON() ([]byte, error) {
	type noMethod UserUndelete
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserWebsite: JSON template for a website entry.
type UserWebsite struct {
	// CustomType: Custom Type.
	CustomType string `json:"customType,omitempty"`

	// Primary: If this is user's primary website or not.
	Primary bool `json:"primary,omitempty"`

	// Type: Each entry can have a type which indicates standard types of
	// that entry. For example website could be of home, work, blog etc. In
	// addition to the standard type, an entry can have a custom type and
	// can give it any name. Such types should have the CUSTOM value as type
	// and also have a customType value.
	Type string `json:"type,omitempty"`

	// Value: Website.
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CustomType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CustomType") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserWebsite) MarshalJSON() ([]byte, error) {
	type noMethod UserWebsite
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Users: JSON response template for List Users operation in Apps
// Directory API.
type Users struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: Kind of resource this is.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access next page of this result.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TriggerEvent: Event that triggered this response (only used in case
	// of Push Response)
	TriggerEvent string `json:"trigger_event,omitempty"`

	// Users: List of user objects.
	Users []*User `json:"users,omitempty"`

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

func (s *Users) MarshalJSON() ([]byte, error) {
	type noMethod Users
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VerificationCode: JSON template for verification codes in Directory
// API.
type VerificationCode struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Kind: The type of the resource. This is always
	// admin#directory#verificationCode.
	Kind string `json:"kind,omitempty"`

	// UserId: The obfuscated unique ID of the user.
	UserId string `json:"userId,omitempty"`

	// VerificationCode: A current verification code for the user.
	// Invalidated or used verification codes are not returned as part of
	// the result.
	VerificationCode string `json:"verificationCode,omitempty"`

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

func (s *VerificationCode) MarshalJSON() ([]byte, error) {
	type noMethod VerificationCode
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VerificationCodes: JSON response template for List verification codes
// operation in Directory API.
type VerificationCodes struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Items: A list of verification code resources.
	Items []*VerificationCode `json:"items,omitempty"`

	// Kind: The type of the resource. This is always
	// admin#directory#verificationCodesList.
	Kind string `json:"kind,omitempty"`

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

func (s *VerificationCodes) MarshalJSON() ([]byte, error) {
	type noMethod VerificationCodes
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "directory.asps.delete":

type AspsDeleteCall struct {
	s          *Service
	userKey    string
	codeId     int64
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Delete an ASP issued by a user.
func (r *AspsService) Delete(userKey string, codeId int64) *AspsDeleteCall {
	c := &AspsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.codeId = codeId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AspsDeleteCall) Fields(s ...googleapi.Field) *AspsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AspsDeleteCall) Context(ctx context.Context) *AspsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AspsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AspsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/asps/{codeId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
		"codeId":  strconv.FormatInt(c.codeId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.asps.delete" call.
func (c *AspsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Delete an ASP issued by a user.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.asps.delete",
	//   "parameterOrder": [
	//     "userKey",
	//     "codeId"
	//   ],
	//   "parameters": {
	//     "codeId": {
	//       "description": "The unique ID of the ASP to be deleted.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/asps/{codeId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.asps.get":

type AspsGetCall struct {
	s            *Service
	userKey      string
	codeId       int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Get information about an ASP issued by a user.
func (r *AspsService) Get(userKey string, codeId int64) *AspsGetCall {
	c := &AspsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.codeId = codeId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AspsGetCall) Fields(s ...googleapi.Field) *AspsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AspsGetCall) IfNoneMatch(entityTag string) *AspsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AspsGetCall) Context(ctx context.Context) *AspsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AspsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AspsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/asps/{codeId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
		"codeId":  strconv.FormatInt(c.codeId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.asps.get" call.
// Exactly one of *Asp or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Asp.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *AspsGetCall) Do(opts ...googleapi.CallOption) (*Asp, error) {
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
	ret := &Asp{
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
	//   "description": "Get information about an ASP issued by a user.",
	//   "httpMethod": "GET",
	//   "id": "directory.asps.get",
	//   "parameterOrder": [
	//     "userKey",
	//     "codeId"
	//   ],
	//   "parameters": {
	//     "codeId": {
	//       "description": "The unique ID of the ASP.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/asps/{codeId}",
	//   "response": {
	//     "$ref": "Asp"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.asps.list":

type AspsListCall struct {
	s            *Service
	userKey      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List the ASPs issued by a user.
func (r *AspsService) List(userKey string) *AspsListCall {
	c := &AspsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AspsListCall) Fields(s ...googleapi.Field) *AspsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AspsListCall) IfNoneMatch(entityTag string) *AspsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AspsListCall) Context(ctx context.Context) *AspsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AspsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AspsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/asps")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.asps.list" call.
// Exactly one of *Asps or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Asps.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *AspsListCall) Do(opts ...googleapi.CallOption) (*Asps, error) {
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
	ret := &Asps{
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
	//   "description": "List the ASPs issued by a user.",
	//   "httpMethod": "GET",
	//   "id": "directory.asps.list",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/asps",
	//   "response": {
	//     "$ref": "Asps"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "admin.channels.stop":

type ChannelsStopCall struct {
	s          *Service
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Stop: Stop watching resources through this channel
func (r *ChannelsService) Stop(channel *Channel) *ChannelsStopCall {
	c := &ChannelsStopCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.channel = channel
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChannelsStopCall) Fields(s ...googleapi.Field) *ChannelsStopCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChannelsStopCall) Context(ctx context.Context) *ChannelsStopCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChannelsStopCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChannelsStopCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.channel)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "/admin/directory_v1/channels/stop")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "admin.channels.stop" call.
func (c *ChannelsStopCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Stop watching resources through this channel",
	//   "httpMethod": "POST",
	//   "id": "admin.channels.stop",
	//   "path": "/admin/directory_v1/channels/stop",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias.readonly",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ]
	// }

}

// method id "directory.chromeosdevices.action":

type ChromeosdevicesActionCall struct {
	s                    *Service
	customerId           string
	resourceId           string
	chromeosdeviceaction *ChromeOsDeviceAction
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Action: Take action on Chrome OS Device
func (r *ChromeosdevicesService) Action(customerId string, resourceId string, chromeosdeviceaction *ChromeOsDeviceAction) *ChromeosdevicesActionCall {
	c := &ChromeosdevicesActionCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.resourceId = resourceId
	c.chromeosdeviceaction = chromeosdeviceaction
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChromeosdevicesActionCall) Fields(s ...googleapi.Field) *ChromeosdevicesActionCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChromeosdevicesActionCall) Context(ctx context.Context) *ChromeosdevicesActionCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChromeosdevicesActionCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChromeosdevicesActionCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.chromeosdeviceaction)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/chromeos/{resourceId}/action")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"resourceId": c.resourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.chromeosdevices.action" call.
func (c *ChromeosdevicesActionCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Take action on Chrome OS Device",
	//   "httpMethod": "POST",
	//   "id": "directory.chromeosdevices.action",
	//   "parameterOrder": [
	//     "customerId",
	//     "resourceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "resourceId": {
	//       "description": "Immutable id of Chrome OS Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/chromeos/{resourceId}/action",
	//   "request": {
	//     "$ref": "ChromeOsDeviceAction"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos"
	//   ]
	// }

}

// method id "directory.chromeosdevices.get":

type ChromeosdevicesGetCall struct {
	s            *Service
	customerId   string
	deviceId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve Chrome OS Device
func (r *ChromeosdevicesService) Get(customerId string, deviceId string) *ChromeosdevicesGetCall {
	c := &ChromeosdevicesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.deviceId = deviceId
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "BASIC" - Includes only the basic metadata fields (e.g., deviceId,
// serialNumber, status, and user)
//   "FULL" - Includes all metadata fields
func (c *ChromeosdevicesGetCall) Projection(projection string) *ChromeosdevicesGetCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChromeosdevicesGetCall) Fields(s ...googleapi.Field) *ChromeosdevicesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ChromeosdevicesGetCall) IfNoneMatch(entityTag string) *ChromeosdevicesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChromeosdevicesGetCall) Context(ctx context.Context) *ChromeosdevicesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChromeosdevicesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChromeosdevicesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/chromeos/{deviceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"deviceId":   c.deviceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.chromeosdevices.get" call.
// Exactly one of *ChromeOsDevice or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ChromeOsDevice.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ChromeosdevicesGetCall) Do(opts ...googleapi.CallOption) (*ChromeOsDevice, error) {
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
	ret := &ChromeOsDevice{
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
	//   "description": "Retrieve Chrome OS Device",
	//   "httpMethod": "GET",
	//   "id": "directory.chromeosdevices.get",
	//   "parameterOrder": [
	//     "customerId",
	//     "deviceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "deviceId": {
	//       "description": "Immutable id of Chrome OS Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "enumDescriptions": [
	//         "Includes only the basic metadata fields (e.g., deviceId, serialNumber, status, and user)",
	//         "Includes all metadata fields"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/chromeos/{deviceId}",
	//   "response": {
	//     "$ref": "ChromeOsDevice"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos",
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly"
	//   ]
	// }

}

// method id "directory.chromeosdevices.list":

type ChromeosdevicesListCall struct {
	s            *Service
	customerId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve all Chrome OS Devices of a customer (paginated)
func (r *ChromeosdevicesService) List(customerId string) *ChromeosdevicesListCall {
	c := &ChromeosdevicesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 100
func (c *ChromeosdevicesListCall) MaxResults(maxResults int64) *ChromeosdevicesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": Column to use for
// sorting results
//
// Possible values:
//   "annotatedLocation" - Chromebook location as annotated by the
// administrator.
//   "annotatedUser" - Chromebook user as annotated by administrator.
//   "lastSync" - Chromebook last sync.
//   "notes" - Chromebook notes as annotated by the administrator.
//   "serialNumber" - Chromebook Serial Number.
//   "status" - Chromebook status.
//   "supportEndDate" - Chromebook support end date.
func (c *ChromeosdevicesListCall) OrderBy(orderBy string) *ChromeosdevicesListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list
func (c *ChromeosdevicesListCall) PageToken(pageToken string) *ChromeosdevicesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "BASIC" - Includes only the basic metadata fields (e.g., deviceId,
// serialNumber, status, and user)
//   "FULL" - Includes all metadata fields
func (c *ChromeosdevicesListCall) Projection(projection string) *ChromeosdevicesListCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Query sets the optional parameter "query": Search string in the
// format given at
// http://support.google.com/chromeos/a/bin/answer.py?hl=en&answer=1698333
func (c *ChromeosdevicesListCall) Query(query string) *ChromeosdevicesListCall {
	c.urlParams_.Set("query", query)
	return c
}

// SortOrder sets the optional parameter "sortOrder": Whether to return
// results in ascending or descending order. Only of use when orderBy is
// also used
//
// Possible values:
//   "ASCENDING" - Ascending order.
//   "DESCENDING" - Descending order.
func (c *ChromeosdevicesListCall) SortOrder(sortOrder string) *ChromeosdevicesListCall {
	c.urlParams_.Set("sortOrder", sortOrder)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChromeosdevicesListCall) Fields(s ...googleapi.Field) *ChromeosdevicesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ChromeosdevicesListCall) IfNoneMatch(entityTag string) *ChromeosdevicesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChromeosdevicesListCall) Context(ctx context.Context) *ChromeosdevicesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChromeosdevicesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChromeosdevicesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/chromeos")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.chromeosdevices.list" call.
// Exactly one of *ChromeOsDevices or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ChromeOsDevices.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ChromeosdevicesListCall) Do(opts ...googleapi.CallOption) (*ChromeOsDevices, error) {
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
	ret := &ChromeOsDevices{
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
	//   "description": "Retrieve all Chrome OS Devices of a customer (paginated)",
	//   "httpMethod": "GET",
	//   "id": "directory.chromeosdevices.list",
	//   "parameterOrder": [
	//     "customerId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 100",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "Column to use for sorting results",
	//       "enum": [
	//         "annotatedLocation",
	//         "annotatedUser",
	//         "lastSync",
	//         "notes",
	//         "serialNumber",
	//         "status",
	//         "supportEndDate"
	//       ],
	//       "enumDescriptions": [
	//         "Chromebook location as annotated by the administrator.",
	//         "Chromebook user as annotated by administrator.",
	//         "Chromebook last sync.",
	//         "Chromebook notes as annotated by the administrator.",
	//         "Chromebook Serial Number.",
	//         "Chromebook status.",
	//         "Chromebook support end date."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "enumDescriptions": [
	//         "Includes only the basic metadata fields (e.g., deviceId, serialNumber, status, and user)",
	//         "Includes all metadata fields"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "query": {
	//       "description": "Search string in the format given at http://support.google.com/chromeos/a/bin/answer.py?hl=en\u0026answer=1698333",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sortOrder": {
	//       "description": "Whether to return results in ascending or descending order. Only of use when orderBy is also used",
	//       "enum": [
	//         "ASCENDING",
	//         "DESCENDING"
	//       ],
	//       "enumDescriptions": [
	//         "Ascending order.",
	//         "Descending order."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/chromeos",
	//   "response": {
	//     "$ref": "ChromeOsDevices"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos",
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ChromeosdevicesListCall) Pages(ctx context.Context, f func(*ChromeOsDevices) error) error {
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

// method id "directory.chromeosdevices.patch":

type ChromeosdevicesPatchCall struct {
	s              *Service
	customerId     string
	deviceId       string
	chromeosdevice *ChromeOsDevice
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Patch: Update Chrome OS Device. This method supports patch semantics.
func (r *ChromeosdevicesService) Patch(customerId string, deviceId string, chromeosdevice *ChromeOsDevice) *ChromeosdevicesPatchCall {
	c := &ChromeosdevicesPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.deviceId = deviceId
	c.chromeosdevice = chromeosdevice
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "BASIC" - Includes only the basic metadata fields (e.g., deviceId,
// serialNumber, status, and user)
//   "FULL" - Includes all metadata fields
func (c *ChromeosdevicesPatchCall) Projection(projection string) *ChromeosdevicesPatchCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChromeosdevicesPatchCall) Fields(s ...googleapi.Field) *ChromeosdevicesPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChromeosdevicesPatchCall) Context(ctx context.Context) *ChromeosdevicesPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChromeosdevicesPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChromeosdevicesPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.chromeosdevice)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/chromeos/{deviceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"deviceId":   c.deviceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.chromeosdevices.patch" call.
// Exactly one of *ChromeOsDevice or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ChromeOsDevice.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ChromeosdevicesPatchCall) Do(opts ...googleapi.CallOption) (*ChromeOsDevice, error) {
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
	ret := &ChromeOsDevice{
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
	//   "description": "Update Chrome OS Device. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.chromeosdevices.patch",
	//   "parameterOrder": [
	//     "customerId",
	//     "deviceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "deviceId": {
	//       "description": "Immutable id of Chrome OS Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "enumDescriptions": [
	//         "Includes only the basic metadata fields (e.g., deviceId, serialNumber, status, and user)",
	//         "Includes all metadata fields"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/chromeos/{deviceId}",
	//   "request": {
	//     "$ref": "ChromeOsDevice"
	//   },
	//   "response": {
	//     "$ref": "ChromeOsDevice"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos"
	//   ]
	// }

}

// method id "directory.chromeosdevices.update":

type ChromeosdevicesUpdateCall struct {
	s              *Service
	customerId     string
	deviceId       string
	chromeosdevice *ChromeOsDevice
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Update: Update Chrome OS Device
func (r *ChromeosdevicesService) Update(customerId string, deviceId string, chromeosdevice *ChromeOsDevice) *ChromeosdevicesUpdateCall {
	c := &ChromeosdevicesUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.deviceId = deviceId
	c.chromeosdevice = chromeosdevice
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "BASIC" - Includes only the basic metadata fields (e.g., deviceId,
// serialNumber, status, and user)
//   "FULL" - Includes all metadata fields
func (c *ChromeosdevicesUpdateCall) Projection(projection string) *ChromeosdevicesUpdateCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ChromeosdevicesUpdateCall) Fields(s ...googleapi.Field) *ChromeosdevicesUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ChromeosdevicesUpdateCall) Context(ctx context.Context) *ChromeosdevicesUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ChromeosdevicesUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ChromeosdevicesUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.chromeosdevice)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/chromeos/{deviceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"deviceId":   c.deviceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.chromeosdevices.update" call.
// Exactly one of *ChromeOsDevice or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ChromeOsDevice.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ChromeosdevicesUpdateCall) Do(opts ...googleapi.CallOption) (*ChromeOsDevice, error) {
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
	ret := &ChromeOsDevice{
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
	//   "description": "Update Chrome OS Device",
	//   "httpMethod": "PUT",
	//   "id": "directory.chromeosdevices.update",
	//   "parameterOrder": [
	//     "customerId",
	//     "deviceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "deviceId": {
	//       "description": "Immutable id of Chrome OS Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "enumDescriptions": [
	//         "Includes only the basic metadata fields (e.g., deviceId, serialNumber, status, and user)",
	//         "Includes all metadata fields"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/chromeos/{deviceId}",
	//   "request": {
	//     "$ref": "ChromeOsDevice"
	//   },
	//   "response": {
	//     "$ref": "ChromeOsDevice"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.chromeos"
	//   ]
	// }

}

// method id "directory.customers.get":

type CustomersGetCall struct {
	s            *Service
	customerKey  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a customer.
func (r *CustomersService) Get(customerKey string) *CustomersGetCall {
	c := &CustomersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerKey = customerKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CustomersGetCall) Fields(s ...googleapi.Field) *CustomersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CustomersGetCall) IfNoneMatch(entityTag string) *CustomersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CustomersGetCall) Context(ctx context.Context) *CustomersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CustomersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CustomersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customers/{customerKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerKey": c.customerKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.customers.get" call.
// Exactly one of *Customer or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Customer.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CustomersGetCall) Do(opts ...googleapi.CallOption) (*Customer, error) {
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
	ret := &Customer{
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
	//   "description": "Retrieves a customer.",
	//   "httpMethod": "GET",
	//   "id": "directory.customers.get",
	//   "parameterOrder": [
	//     "customerKey"
	//   ],
	//   "parameters": {
	//     "customerKey": {
	//       "description": "Id of the customer to be retrieved",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customers/{customerKey}",
	//   "response": {
	//     "$ref": "Customer"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.customer",
	//     "https://www.googleapis.com/auth/admin.directory.customer.readonly"
	//   ]
	// }

}

// method id "directory.customers.patch":

type CustomersPatchCall struct {
	s           *Service
	customerKey string
	customer    *Customer
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Patch: Updates a customer. This method supports patch semantics.
func (r *CustomersService) Patch(customerKey string, customer *Customer) *CustomersPatchCall {
	c := &CustomersPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerKey = customerKey
	c.customer = customer
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CustomersPatchCall) Fields(s ...googleapi.Field) *CustomersPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CustomersPatchCall) Context(ctx context.Context) *CustomersPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CustomersPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CustomersPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.customer)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customers/{customerKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerKey": c.customerKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.customers.patch" call.
// Exactly one of *Customer or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Customer.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CustomersPatchCall) Do(opts ...googleapi.CallOption) (*Customer, error) {
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
	ret := &Customer{
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
	//   "description": "Updates a customer. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.customers.patch",
	//   "parameterOrder": [
	//     "customerKey"
	//   ],
	//   "parameters": {
	//     "customerKey": {
	//       "description": "Id of the customer to be updated",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customers/{customerKey}",
	//   "request": {
	//     "$ref": "Customer"
	//   },
	//   "response": {
	//     "$ref": "Customer"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.customer"
	//   ]
	// }

}

// method id "directory.customers.update":

type CustomersUpdateCall struct {
	s           *Service
	customerKey string
	customer    *Customer
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Update: Updates a customer.
func (r *CustomersService) Update(customerKey string, customer *Customer) *CustomersUpdateCall {
	c := &CustomersUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerKey = customerKey
	c.customer = customer
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CustomersUpdateCall) Fields(s ...googleapi.Field) *CustomersUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CustomersUpdateCall) Context(ctx context.Context) *CustomersUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CustomersUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CustomersUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.customer)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customers/{customerKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerKey": c.customerKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.customers.update" call.
// Exactly one of *Customer or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Customer.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CustomersUpdateCall) Do(opts ...googleapi.CallOption) (*Customer, error) {
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
	ret := &Customer{
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
	//   "description": "Updates a customer.",
	//   "httpMethod": "PUT",
	//   "id": "directory.customers.update",
	//   "parameterOrder": [
	//     "customerKey"
	//   ],
	//   "parameters": {
	//     "customerKey": {
	//       "description": "Id of the customer to be updated",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customers/{customerKey}",
	//   "request": {
	//     "$ref": "Customer"
	//   },
	//   "response": {
	//     "$ref": "Customer"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.customer"
	//   ]
	// }

}

// method id "directory.domainAliases.delete":

type DomainAliasesDeleteCall struct {
	s               *Service
	customer        string
	domainAliasName string
	urlParams_      gensupport.URLParams
	ctx_            context.Context
	header_         http.Header
}

// Delete: Deletes a Domain Alias of the customer.
func (r *DomainAliasesService) Delete(customer string, domainAliasName string) *DomainAliasesDeleteCall {
	c := &DomainAliasesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.domainAliasName = domainAliasName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainAliasesDeleteCall) Fields(s ...googleapi.Field) *DomainAliasesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainAliasesDeleteCall) Context(ctx context.Context) *DomainAliasesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainAliasesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainAliasesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domainaliases/{domainAliasName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":        c.customer,
		"domainAliasName": c.domainAliasName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domainAliases.delete" call.
func (c *DomainAliasesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a Domain Alias of the customer.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.domainAliases.delete",
	//   "parameterOrder": [
	//     "customer",
	//     "domainAliasName"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "domainAliasName": {
	//       "description": "Name of domain alias to be retrieved.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domainaliases/{domainAliasName}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain"
	//   ]
	// }

}

// method id "directory.domainAliases.get":

type DomainAliasesGetCall struct {
	s               *Service
	customer        string
	domainAliasName string
	urlParams_      gensupport.URLParams
	ifNoneMatch_    string
	ctx_            context.Context
	header_         http.Header
}

// Get: Retrieves a domain alias of the customer.
func (r *DomainAliasesService) Get(customer string, domainAliasName string) *DomainAliasesGetCall {
	c := &DomainAliasesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.domainAliasName = domainAliasName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainAliasesGetCall) Fields(s ...googleapi.Field) *DomainAliasesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DomainAliasesGetCall) IfNoneMatch(entityTag string) *DomainAliasesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainAliasesGetCall) Context(ctx context.Context) *DomainAliasesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainAliasesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainAliasesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domainaliases/{domainAliasName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":        c.customer,
		"domainAliasName": c.domainAliasName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domainAliases.get" call.
// Exactly one of *DomainAlias or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *DomainAlias.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *DomainAliasesGetCall) Do(opts ...googleapi.CallOption) (*DomainAlias, error) {
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
	ret := &DomainAlias{
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
	//   "description": "Retrieves a domain alias of the customer.",
	//   "httpMethod": "GET",
	//   "id": "directory.domainAliases.get",
	//   "parameterOrder": [
	//     "customer",
	//     "domainAliasName"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "domainAliasName": {
	//       "description": "Name of domain alias to be retrieved.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domainaliases/{domainAliasName}",
	//   "response": {
	//     "$ref": "DomainAlias"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain",
	//     "https://www.googleapis.com/auth/admin.directory.domain.readonly"
	//   ]
	// }

}

// method id "directory.domainAliases.insert":

type DomainAliasesInsertCall struct {
	s           *Service
	customer    string
	domainalias *DomainAlias
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Insert: Inserts a Domain alias of the customer.
func (r *DomainAliasesService) Insert(customer string, domainalias *DomainAlias) *DomainAliasesInsertCall {
	c := &DomainAliasesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.domainalias = domainalias
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainAliasesInsertCall) Fields(s ...googleapi.Field) *DomainAliasesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainAliasesInsertCall) Context(ctx context.Context) *DomainAliasesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainAliasesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainAliasesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.domainalias)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domainaliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domainAliases.insert" call.
// Exactly one of *DomainAlias or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *DomainAlias.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *DomainAliasesInsertCall) Do(opts ...googleapi.CallOption) (*DomainAlias, error) {
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
	ret := &DomainAlias{
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
	//   "description": "Inserts a Domain alias of the customer.",
	//   "httpMethod": "POST",
	//   "id": "directory.domainAliases.insert",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domainaliases",
	//   "request": {
	//     "$ref": "DomainAlias"
	//   },
	//   "response": {
	//     "$ref": "DomainAlias"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain"
	//   ]
	// }

}

// method id "directory.domainAliases.list":

type DomainAliasesListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the domain aliases of the customer.
func (r *DomainAliasesService) List(customer string) *DomainAliasesListCall {
	c := &DomainAliasesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// ParentDomainName sets the optional parameter "parentDomainName": Name
// of the parent domain for which domain aliases are to be fetched.
func (c *DomainAliasesListCall) ParentDomainName(parentDomainName string) *DomainAliasesListCall {
	c.urlParams_.Set("parentDomainName", parentDomainName)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainAliasesListCall) Fields(s ...googleapi.Field) *DomainAliasesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DomainAliasesListCall) IfNoneMatch(entityTag string) *DomainAliasesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainAliasesListCall) Context(ctx context.Context) *DomainAliasesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainAliasesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainAliasesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domainaliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domainAliases.list" call.
// Exactly one of *DomainAliases or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *DomainAliases.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *DomainAliasesListCall) Do(opts ...googleapi.CallOption) (*DomainAliases, error) {
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
	ret := &DomainAliases{
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
	//   "description": "Lists the domain aliases of the customer.",
	//   "httpMethod": "GET",
	//   "id": "directory.domainAliases.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "parentDomainName": {
	//       "description": "Name of the parent domain for which domain aliases are to be fetched.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domainaliases",
	//   "response": {
	//     "$ref": "DomainAliases"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain",
	//     "https://www.googleapis.com/auth/admin.directory.domain.readonly"
	//   ]
	// }

}

// method id "directory.domains.delete":

type DomainsDeleteCall struct {
	s          *Service
	customer   string
	domainName string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a domain of the customer.
func (r *DomainsService) Delete(customer string, domainName string) *DomainsDeleteCall {
	c := &DomainsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.domainName = domainName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainsDeleteCall) Fields(s ...googleapi.Field) *DomainsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainsDeleteCall) Context(ctx context.Context) *DomainsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domains/{domainName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":   c.customer,
		"domainName": c.domainName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domains.delete" call.
func (c *DomainsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a domain of the customer.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.domains.delete",
	//   "parameterOrder": [
	//     "customer",
	//     "domainName"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "domainName": {
	//       "description": "Name of domain to be deleted",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domains/{domainName}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain"
	//   ]
	// }

}

// method id "directory.domains.get":

type DomainsGetCall struct {
	s            *Service
	customer     string
	domainName   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a domain of the customer.
func (r *DomainsService) Get(customer string, domainName string) *DomainsGetCall {
	c := &DomainsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.domainName = domainName
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainsGetCall) Fields(s ...googleapi.Field) *DomainsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DomainsGetCall) IfNoneMatch(entityTag string) *DomainsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainsGetCall) Context(ctx context.Context) *DomainsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domains/{domainName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":   c.customer,
		"domainName": c.domainName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domains.get" call.
// Exactly one of *Domains or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Domains.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *DomainsGetCall) Do(opts ...googleapi.CallOption) (*Domains, error) {
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
	ret := &Domains{
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
	//   "description": "Retrieves a domain of the customer.",
	//   "httpMethod": "GET",
	//   "id": "directory.domains.get",
	//   "parameterOrder": [
	//     "customer",
	//     "domainName"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "domainName": {
	//       "description": "Name of domain to be retrieved",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domains/{domainName}",
	//   "response": {
	//     "$ref": "Domains"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain",
	//     "https://www.googleapis.com/auth/admin.directory.domain.readonly"
	//   ]
	// }

}

// method id "directory.domains.insert":

type DomainsInsertCall struct {
	s          *Service
	customer   string
	domains    *Domains
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Inserts a domain of the customer.
func (r *DomainsService) Insert(customer string, domains *Domains) *DomainsInsertCall {
	c := &DomainsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.domains = domains
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainsInsertCall) Fields(s ...googleapi.Field) *DomainsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainsInsertCall) Context(ctx context.Context) *DomainsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.domains)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domains")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domains.insert" call.
// Exactly one of *Domains or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Domains.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *DomainsInsertCall) Do(opts ...googleapi.CallOption) (*Domains, error) {
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
	ret := &Domains{
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
	//   "description": "Inserts a domain of the customer.",
	//   "httpMethod": "POST",
	//   "id": "directory.domains.insert",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domains",
	//   "request": {
	//     "$ref": "Domains"
	//   },
	//   "response": {
	//     "$ref": "Domains"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain"
	//   ]
	// }

}

// method id "directory.domains.list":

type DomainsListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the domains of the customer.
func (r *DomainsService) List(customer string) *DomainsListCall {
	c := &DomainsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DomainsListCall) Fields(s ...googleapi.Field) *DomainsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DomainsListCall) IfNoneMatch(entityTag string) *DomainsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DomainsListCall) Context(ctx context.Context) *DomainsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DomainsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DomainsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/domains")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.domains.list" call.
// Exactly one of *Domains2 or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Domains2.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *DomainsListCall) Do(opts ...googleapi.CallOption) (*Domains2, error) {
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
	ret := &Domains2{
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
	//   "description": "Lists the domains of the customer.",
	//   "httpMethod": "GET",
	//   "id": "directory.domains.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/domains",
	//   "response": {
	//     "$ref": "Domains2"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.domain",
	//     "https://www.googleapis.com/auth/admin.directory.domain.readonly"
	//   ]
	// }

}

// method id "directory.groups.delete":

type GroupsDeleteCall struct {
	s          *Service
	groupKey   string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Delete Group
func (r *GroupsService) Delete(groupKey string) *GroupsDeleteCall {
	c := &GroupsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsDeleteCall) Fields(s ...googleapi.Field) *GroupsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsDeleteCall) Context(ctx context.Context) *GroupsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.delete" call.
func (c *GroupsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Delete Group",
	//   "httpMethod": "DELETE",
	//   "id": "directory.groups.delete",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group"
	//   ]
	// }

}

// method id "directory.groups.get":

type GroupsGetCall struct {
	s            *Service
	groupKey     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve Group
func (r *GroupsService) Get(groupKey string) *GroupsGetCall {
	c := &GroupsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsGetCall) Fields(s ...googleapi.Field) *GroupsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *GroupsGetCall) IfNoneMatch(entityTag string) *GroupsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsGetCall) Context(ctx context.Context) *GroupsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.get" call.
// Exactly one of *Group or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Group.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsGetCall) Do(opts ...googleapi.CallOption) (*Group, error) {
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
	ret := &Group{
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
	//   "description": "Retrieve Group",
	//   "httpMethod": "GET",
	//   "id": "directory.groups.get",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}",
	//   "response": {
	//     "$ref": "Group"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.readonly"
	//   ]
	// }

}

// method id "directory.groups.insert":

type GroupsInsertCall struct {
	s          *Service
	group      *Group
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Create Group
func (r *GroupsService) Insert(group *Group) *GroupsInsertCall {
	c := &GroupsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.group = group
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsInsertCall) Fields(s ...googleapi.Field) *GroupsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsInsertCall) Context(ctx context.Context) *GroupsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.group)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.insert" call.
// Exactly one of *Group or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Group.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsInsertCall) Do(opts ...googleapi.CallOption) (*Group, error) {
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
	ret := &Group{
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
	//   "description": "Create Group",
	//   "httpMethod": "POST",
	//   "id": "directory.groups.insert",
	//   "path": "groups",
	//   "request": {
	//     "$ref": "Group"
	//   },
	//   "response": {
	//     "$ref": "Group"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group"
	//   ]
	// }

}

// method id "directory.groups.list":

type GroupsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve all groups in a domain (paginated)
func (r *GroupsService) List() *GroupsListCall {
	c := &GroupsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Customer sets the optional parameter "customer": Immutable id of the
// Google Apps account. In case of multi-domain, to fetch all groups for
// a customer, fill this field instead of domain.
func (c *GroupsListCall) Customer(customer string) *GroupsListCall {
	c.urlParams_.Set("customer", customer)
	return c
}

// Domain sets the optional parameter "domain": Name of the domain. Fill
// this field to get groups from only this domain. To return all groups
// in a multi-domain fill customer field instead.
func (c *GroupsListCall) Domain(domain string) *GroupsListCall {
	c.urlParams_.Set("domain", domain)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 200
func (c *GroupsListCall) MaxResults(maxResults int64) *GroupsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list
func (c *GroupsListCall) PageToken(pageToken string) *GroupsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// UserKey sets the optional parameter "userKey": Email or immutable Id
// of the user if only those groups are to be listed, the given user is
// a member of. If Id, it should match with id of user object
func (c *GroupsListCall) UserKey(userKey string) *GroupsListCall {
	c.urlParams_.Set("userKey", userKey)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsListCall) Fields(s ...googleapi.Field) *GroupsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *GroupsListCall) IfNoneMatch(entityTag string) *GroupsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsListCall) Context(ctx context.Context) *GroupsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.list" call.
// Exactly one of *Groups or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Groups.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsListCall) Do(opts ...googleapi.CallOption) (*Groups, error) {
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
	ret := &Groups{
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
	//   "description": "Retrieve all groups in a domain (paginated)",
	//   "httpMethod": "GET",
	//   "id": "directory.groups.list",
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account. In case of multi-domain, to fetch all groups for a customer, fill this field instead of domain.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "domain": {
	//       "description": "Name of the domain. Fill this field to get groups from only this domain. To return all groups in a multi-domain fill customer field instead.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 200",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Email or immutable Id of the user if only those groups are to be listed, the given user is a member of. If Id, it should match with id of user object",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups",
	//   "response": {
	//     "$ref": "Groups"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *GroupsListCall) Pages(ctx context.Context, f func(*Groups) error) error {
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

// method id "directory.groups.patch":

type GroupsPatchCall struct {
	s          *Service
	groupKey   string
	group      *Group
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Update Group. This method supports patch semantics.
func (r *GroupsService) Patch(groupKey string, group *Group) *GroupsPatchCall {
	c := &GroupsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.group = group
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsPatchCall) Fields(s ...googleapi.Field) *GroupsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsPatchCall) Context(ctx context.Context) *GroupsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.group)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.patch" call.
// Exactly one of *Group or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Group.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsPatchCall) Do(opts ...googleapi.CallOption) (*Group, error) {
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
	ret := &Group{
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
	//   "description": "Update Group. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.groups.patch",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group. If Id, it should match with id of group object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}",
	//   "request": {
	//     "$ref": "Group"
	//   },
	//   "response": {
	//     "$ref": "Group"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group"
	//   ]
	// }

}

// method id "directory.groups.update":

type GroupsUpdateCall struct {
	s          *Service
	groupKey   string
	group      *Group
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Update Group
func (r *GroupsService) Update(groupKey string, group *Group) *GroupsUpdateCall {
	c := &GroupsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.group = group
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsUpdateCall) Fields(s ...googleapi.Field) *GroupsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsUpdateCall) Context(ctx context.Context) *GroupsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.group)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.update" call.
// Exactly one of *Group or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Group.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsUpdateCall) Do(opts ...googleapi.CallOption) (*Group, error) {
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
	ret := &Group{
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
	//   "description": "Update Group",
	//   "httpMethod": "PUT",
	//   "id": "directory.groups.update",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group. If Id, it should match with id of group object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}",
	//   "request": {
	//     "$ref": "Group"
	//   },
	//   "response": {
	//     "$ref": "Group"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group"
	//   ]
	// }

}

// method id "directory.groups.aliases.delete":

type GroupsAliasesDeleteCall struct {
	s          *Service
	groupKey   string
	alias      string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Remove a alias for the group
func (r *GroupsAliasesService) Delete(groupKey string, alias string) *GroupsAliasesDeleteCall {
	c := &GroupsAliasesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.alias = alias
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsAliasesDeleteCall) Fields(s ...googleapi.Field) *GroupsAliasesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsAliasesDeleteCall) Context(ctx context.Context) *GroupsAliasesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsAliasesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsAliasesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/aliases/{alias}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
		"alias":    c.alias,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.aliases.delete" call.
func (c *GroupsAliasesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove a alias for the group",
	//   "httpMethod": "DELETE",
	//   "id": "directory.groups.aliases.delete",
	//   "parameterOrder": [
	//     "groupKey",
	//     "alias"
	//   ],
	//   "parameters": {
	//     "alias": {
	//       "description": "The alias to be removed",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/aliases/{alias}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group"
	//   ]
	// }

}

// method id "directory.groups.aliases.insert":

type GroupsAliasesInsertCall struct {
	s          *Service
	groupKey   string
	alias      *Alias
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Add a alias for the group
func (r *GroupsAliasesService) Insert(groupKey string, alias *Alias) *GroupsAliasesInsertCall {
	c := &GroupsAliasesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.alias = alias
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsAliasesInsertCall) Fields(s ...googleapi.Field) *GroupsAliasesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsAliasesInsertCall) Context(ctx context.Context) *GroupsAliasesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsAliasesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsAliasesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.alias)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/aliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.aliases.insert" call.
// Exactly one of *Alias or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Alias.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsAliasesInsertCall) Do(opts ...googleapi.CallOption) (*Alias, error) {
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
	ret := &Alias{
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
	//   "description": "Add a alias for the group",
	//   "httpMethod": "POST",
	//   "id": "directory.groups.aliases.insert",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/aliases",
	//   "request": {
	//     "$ref": "Alias"
	//   },
	//   "response": {
	//     "$ref": "Alias"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group"
	//   ]
	// }

}

// method id "directory.groups.aliases.list":

type GroupsAliasesListCall struct {
	s            *Service
	groupKey     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List all aliases for a group
func (r *GroupsAliasesService) List(groupKey string) *GroupsAliasesListCall {
	c := &GroupsAliasesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *GroupsAliasesListCall) Fields(s ...googleapi.Field) *GroupsAliasesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *GroupsAliasesListCall) IfNoneMatch(entityTag string) *GroupsAliasesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *GroupsAliasesListCall) Context(ctx context.Context) *GroupsAliasesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *GroupsAliasesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *GroupsAliasesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/aliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.groups.aliases.list" call.
// Exactly one of *Aliases or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Aliases.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *GroupsAliasesListCall) Do(opts ...googleapi.CallOption) (*Aliases, error) {
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
	ret := &Aliases{
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
	//   "description": "List all aliases for a group",
	//   "httpMethod": "GET",
	//   "id": "directory.groups.aliases.list",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/aliases",
	//   "response": {
	//     "$ref": "Aliases"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "directory.members.delete":

type MembersDeleteCall struct {
	s          *Service
	groupKey   string
	memberKey  string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Remove membership.
func (r *MembersService) Delete(groupKey string, memberKey string) *MembersDeleteCall {
	c := &MembersDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.memberKey = memberKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MembersDeleteCall) Fields(s ...googleapi.Field) *MembersDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MembersDeleteCall) Context(ctx context.Context) *MembersDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MembersDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MembersDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/members/{memberKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey":  c.groupKey,
		"memberKey": c.memberKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.members.delete" call.
func (c *MembersDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove membership.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.members.delete",
	//   "parameterOrder": [
	//     "groupKey",
	//     "memberKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "memberKey": {
	//       "description": "Email or immutable Id of the member",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/members/{memberKey}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.member"
	//   ]
	// }

}

// method id "directory.members.get":

type MembersGetCall struct {
	s            *Service
	groupKey     string
	memberKey    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve Group Member
func (r *MembersService) Get(groupKey string, memberKey string) *MembersGetCall {
	c := &MembersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.memberKey = memberKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MembersGetCall) Fields(s ...googleapi.Field) *MembersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MembersGetCall) IfNoneMatch(entityTag string) *MembersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MembersGetCall) Context(ctx context.Context) *MembersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MembersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MembersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/members/{memberKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey":  c.groupKey,
		"memberKey": c.memberKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.members.get" call.
// Exactly one of *Member or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Member.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MembersGetCall) Do(opts ...googleapi.CallOption) (*Member, error) {
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
	ret := &Member{
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
	//   "description": "Retrieve Group Member",
	//   "httpMethod": "GET",
	//   "id": "directory.members.get",
	//   "parameterOrder": [
	//     "groupKey",
	//     "memberKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "memberKey": {
	//       "description": "Email or immutable Id of the member",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/members/{memberKey}",
	//   "response": {
	//     "$ref": "Member"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.member",
	//     "https://www.googleapis.com/auth/admin.directory.group.member.readonly",
	//     "https://www.googleapis.com/auth/admin.directory.group.readonly"
	//   ]
	// }

}

// method id "directory.members.insert":

type MembersInsertCall struct {
	s          *Service
	groupKey   string
	member     *Member
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Add user to the specified group.
func (r *MembersService) Insert(groupKey string, member *Member) *MembersInsertCall {
	c := &MembersInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.member = member
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MembersInsertCall) Fields(s ...googleapi.Field) *MembersInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MembersInsertCall) Context(ctx context.Context) *MembersInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MembersInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MembersInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.member)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/members")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.members.insert" call.
// Exactly one of *Member or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Member.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MembersInsertCall) Do(opts ...googleapi.CallOption) (*Member, error) {
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
	ret := &Member{
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
	//   "description": "Add user to the specified group.",
	//   "httpMethod": "POST",
	//   "id": "directory.members.insert",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/members",
	//   "request": {
	//     "$ref": "Member"
	//   },
	//   "response": {
	//     "$ref": "Member"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.member"
	//   ]
	// }

}

// method id "directory.members.list":

type MembersListCall struct {
	s            *Service
	groupKey     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve all members in a group (paginated)
func (r *MembersService) List(groupKey string) *MembersListCall {
	c := &MembersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 200
func (c *MembersListCall) MaxResults(maxResults int64) *MembersListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list
func (c *MembersListCall) PageToken(pageToken string) *MembersListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Roles sets the optional parameter "roles": Comma separated role
// values to filter list results on.
func (c *MembersListCall) Roles(roles string) *MembersListCall {
	c.urlParams_.Set("roles", roles)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MembersListCall) Fields(s ...googleapi.Field) *MembersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MembersListCall) IfNoneMatch(entityTag string) *MembersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MembersListCall) Context(ctx context.Context) *MembersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MembersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MembersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/members")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey": c.groupKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.members.list" call.
// Exactly one of *Members or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Members.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MembersListCall) Do(opts ...googleapi.CallOption) (*Members, error) {
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
	ret := &Members{
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
	//   "description": "Retrieve all members in a group (paginated)",
	//   "httpMethod": "GET",
	//   "id": "directory.members.list",
	//   "parameterOrder": [
	//     "groupKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 200",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "roles": {
	//       "description": "Comma separated role values to filter list results on.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/members",
	//   "response": {
	//     "$ref": "Members"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.member",
	//     "https://www.googleapis.com/auth/admin.directory.group.member.readonly",
	//     "https://www.googleapis.com/auth/admin.directory.group.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *MembersListCall) Pages(ctx context.Context, f func(*Members) error) error {
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

// method id "directory.members.patch":

type MembersPatchCall struct {
	s          *Service
	groupKey   string
	memberKey  string
	member     *Member
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Update membership of a user in the specified group. This
// method supports patch semantics.
func (r *MembersService) Patch(groupKey string, memberKey string, member *Member) *MembersPatchCall {
	c := &MembersPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.memberKey = memberKey
	c.member = member
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MembersPatchCall) Fields(s ...googleapi.Field) *MembersPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MembersPatchCall) Context(ctx context.Context) *MembersPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MembersPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MembersPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.member)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/members/{memberKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey":  c.groupKey,
		"memberKey": c.memberKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.members.patch" call.
// Exactly one of *Member or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Member.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MembersPatchCall) Do(opts ...googleapi.CallOption) (*Member, error) {
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
	ret := &Member{
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
	//   "description": "Update membership of a user in the specified group. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.members.patch",
	//   "parameterOrder": [
	//     "groupKey",
	//     "memberKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group. If Id, it should match with id of group object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "memberKey": {
	//       "description": "Email or immutable Id of the user. If Id, it should match with id of member object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/members/{memberKey}",
	//   "request": {
	//     "$ref": "Member"
	//   },
	//   "response": {
	//     "$ref": "Member"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.member"
	//   ]
	// }

}

// method id "directory.members.update":

type MembersUpdateCall struct {
	s          *Service
	groupKey   string
	memberKey  string
	member     *Member
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Update membership of a user in the specified group.
func (r *MembersService) Update(groupKey string, memberKey string, member *Member) *MembersUpdateCall {
	c := &MembersUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.groupKey = groupKey
	c.memberKey = memberKey
	c.member = member
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MembersUpdateCall) Fields(s ...googleapi.Field) *MembersUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MembersUpdateCall) Context(ctx context.Context) *MembersUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MembersUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MembersUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.member)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "groups/{groupKey}/members/{memberKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"groupKey":  c.groupKey,
		"memberKey": c.memberKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.members.update" call.
// Exactly one of *Member or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Member.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MembersUpdateCall) Do(opts ...googleapi.CallOption) (*Member, error) {
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
	ret := &Member{
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
	//   "description": "Update membership of a user in the specified group.",
	//   "httpMethod": "PUT",
	//   "id": "directory.members.update",
	//   "parameterOrder": [
	//     "groupKey",
	//     "memberKey"
	//   ],
	//   "parameters": {
	//     "groupKey": {
	//       "description": "Email or immutable Id of the group. If Id, it should match with id of group object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "memberKey": {
	//       "description": "Email or immutable Id of the user. If Id, it should match with id of member object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "groups/{groupKey}/members/{memberKey}",
	//   "request": {
	//     "$ref": "Member"
	//   },
	//   "response": {
	//     "$ref": "Member"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.group",
	//     "https://www.googleapis.com/auth/admin.directory.group.member"
	//   ]
	// }

}

// method id "directory.mobiledevices.action":

type MobiledevicesActionCall struct {
	s                  *Service
	customerId         string
	resourceId         string
	mobiledeviceaction *MobileDeviceAction
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Action: Take action on Mobile Device
func (r *MobiledevicesService) Action(customerId string, resourceId string, mobiledeviceaction *MobileDeviceAction) *MobiledevicesActionCall {
	c := &MobiledevicesActionCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.resourceId = resourceId
	c.mobiledeviceaction = mobiledeviceaction
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobiledevicesActionCall) Fields(s ...googleapi.Field) *MobiledevicesActionCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobiledevicesActionCall) Context(ctx context.Context) *MobiledevicesActionCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobiledevicesActionCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobiledevicesActionCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.mobiledeviceaction)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/mobile/{resourceId}/action")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"resourceId": c.resourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.mobiledevices.action" call.
func (c *MobiledevicesActionCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Take action on Mobile Device",
	//   "httpMethod": "POST",
	//   "id": "directory.mobiledevices.action",
	//   "parameterOrder": [
	//     "customerId",
	//     "resourceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "resourceId": {
	//       "description": "Immutable id of Mobile Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/mobile/{resourceId}/action",
	//   "request": {
	//     "$ref": "MobileDeviceAction"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile",
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile.action"
	//   ]
	// }

}

// method id "directory.mobiledevices.delete":

type MobiledevicesDeleteCall struct {
	s          *Service
	customerId string
	resourceId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Delete Mobile Device
func (r *MobiledevicesService) Delete(customerId string, resourceId string) *MobiledevicesDeleteCall {
	c := &MobiledevicesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.resourceId = resourceId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobiledevicesDeleteCall) Fields(s ...googleapi.Field) *MobiledevicesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobiledevicesDeleteCall) Context(ctx context.Context) *MobiledevicesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobiledevicesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobiledevicesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/mobile/{resourceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"resourceId": c.resourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.mobiledevices.delete" call.
func (c *MobiledevicesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Delete Mobile Device",
	//   "httpMethod": "DELETE",
	//   "id": "directory.mobiledevices.delete",
	//   "parameterOrder": [
	//     "customerId",
	//     "resourceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "resourceId": {
	//       "description": "Immutable id of Mobile Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/mobile/{resourceId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile"
	//   ]
	// }

}

// method id "directory.mobiledevices.get":

type MobiledevicesGetCall struct {
	s            *Service
	customerId   string
	resourceId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve Mobile Device
func (r *MobiledevicesService) Get(customerId string, resourceId string) *MobiledevicesGetCall {
	c := &MobiledevicesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.resourceId = resourceId
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "BASIC" - Includes only the basic metadata fields (e.g., deviceId,
// model, status, type, and status)
//   "FULL" - Includes all metadata fields
func (c *MobiledevicesGetCall) Projection(projection string) *MobiledevicesGetCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobiledevicesGetCall) Fields(s ...googleapi.Field) *MobiledevicesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MobiledevicesGetCall) IfNoneMatch(entityTag string) *MobiledevicesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobiledevicesGetCall) Context(ctx context.Context) *MobiledevicesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobiledevicesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobiledevicesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/mobile/{resourceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"resourceId": c.resourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.mobiledevices.get" call.
// Exactly one of *MobileDevice or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *MobileDevice.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MobiledevicesGetCall) Do(opts ...googleapi.CallOption) (*MobileDevice, error) {
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
	ret := &MobileDevice{
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
	//   "description": "Retrieve Mobile Device",
	//   "httpMethod": "GET",
	//   "id": "directory.mobiledevices.get",
	//   "parameterOrder": [
	//     "customerId",
	//     "resourceId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "enumDescriptions": [
	//         "Includes only the basic metadata fields (e.g., deviceId, model, status, type, and status)",
	//         "Includes all metadata fields"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "resourceId": {
	//       "description": "Immutable id of Mobile Device",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/mobile/{resourceId}",
	//   "response": {
	//     "$ref": "MobileDevice"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile",
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile.action",
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile.readonly"
	//   ]
	// }

}

// method id "directory.mobiledevices.list":

type MobiledevicesListCall struct {
	s            *Service
	customerId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve all Mobile Devices of a customer (paginated)
func (r *MobiledevicesService) List(customerId string) *MobiledevicesListCall {
	c := &MobiledevicesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 100
func (c *MobiledevicesListCall) MaxResults(maxResults int64) *MobiledevicesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": Column to use for
// sorting results
//
// Possible values:
//   "deviceId" - Mobile Device serial number.
//   "email" - Owner user email.
//   "lastSync" - Last policy settings sync date time of the device.
//   "model" - Mobile Device model.
//   "name" - Owner user name.
//   "os" - Mobile operating system.
//   "status" - Status of the device.
//   "type" - Type of the device.
func (c *MobiledevicesListCall) OrderBy(orderBy string) *MobiledevicesListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list
func (c *MobiledevicesListCall) PageToken(pageToken string) *MobiledevicesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "BASIC" - Includes only the basic metadata fields (e.g., deviceId,
// model, status, type, and status)
//   "FULL" - Includes all metadata fields
func (c *MobiledevicesListCall) Projection(projection string) *MobiledevicesListCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Query sets the optional parameter "query": Search string in the
// format given at
// http://support.google.com/a/bin/answer.py?hl=en&answer=1408863#search
func (c *MobiledevicesListCall) Query(query string) *MobiledevicesListCall {
	c.urlParams_.Set("query", query)
	return c
}

// SortOrder sets the optional parameter "sortOrder": Whether to return
// results in ascending or descending order. Only of use when orderBy is
// also used
//
// Possible values:
//   "ASCENDING" - Ascending order.
//   "DESCENDING" - Descending order.
func (c *MobiledevicesListCall) SortOrder(sortOrder string) *MobiledevicesListCall {
	c.urlParams_.Set("sortOrder", sortOrder)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MobiledevicesListCall) Fields(s ...googleapi.Field) *MobiledevicesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MobiledevicesListCall) IfNoneMatch(entityTag string) *MobiledevicesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MobiledevicesListCall) Context(ctx context.Context) *MobiledevicesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MobiledevicesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MobiledevicesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/devices/mobile")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.mobiledevices.list" call.
// Exactly one of *MobileDevices or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *MobileDevices.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MobiledevicesListCall) Do(opts ...googleapi.CallOption) (*MobileDevices, error) {
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
	ret := &MobileDevices{
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
	//   "description": "Retrieve all Mobile Devices of a customer (paginated)",
	//   "httpMethod": "GET",
	//   "id": "directory.mobiledevices.list",
	//   "parameterOrder": [
	//     "customerId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 100",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "Column to use for sorting results",
	//       "enum": [
	//         "deviceId",
	//         "email",
	//         "lastSync",
	//         "model",
	//         "name",
	//         "os",
	//         "status",
	//         "type"
	//       ],
	//       "enumDescriptions": [
	//         "Mobile Device serial number.",
	//         "Owner user email.",
	//         "Last policy settings sync date time of the device.",
	//         "Mobile Device model.",
	//         "Owner user name.",
	//         "Mobile operating system.",
	//         "Status of the device.",
	//         "Type of the device."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "BASIC",
	//         "FULL"
	//       ],
	//       "enumDescriptions": [
	//         "Includes only the basic metadata fields (e.g., deviceId, model, status, type, and status)",
	//         "Includes all metadata fields"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "query": {
	//       "description": "Search string in the format given at http://support.google.com/a/bin/answer.py?hl=en\u0026answer=1408863#search",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sortOrder": {
	//       "description": "Whether to return results in ascending or descending order. Only of use when orderBy is also used",
	//       "enum": [
	//         "ASCENDING",
	//         "DESCENDING"
	//       ],
	//       "enumDescriptions": [
	//         "Ascending order.",
	//         "Descending order."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/devices/mobile",
	//   "response": {
	//     "$ref": "MobileDevices"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile",
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile.action",
	//     "https://www.googleapis.com/auth/admin.directory.device.mobile.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *MobiledevicesListCall) Pages(ctx context.Context, f func(*MobileDevices) error) error {
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

// method id "directory.notifications.delete":

type NotificationsDeleteCall struct {
	s              *Service
	customer       string
	notificationId string
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Delete: Deletes a notification
func (r *NotificationsService) Delete(customer string, notificationId string) *NotificationsDeleteCall {
	c := &NotificationsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.notificationId = notificationId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NotificationsDeleteCall) Fields(s ...googleapi.Field) *NotificationsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NotificationsDeleteCall) Context(ctx context.Context) *NotificationsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NotificationsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NotificationsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/notifications/{notificationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":       c.customer,
		"notificationId": c.notificationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.notifications.delete" call.
func (c *NotificationsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a notification",
	//   "httpMethod": "DELETE",
	//   "id": "directory.notifications.delete",
	//   "parameterOrder": [
	//     "customer",
	//     "notificationId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. The customerId is also returned as part of the Users resource.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "notificationId": {
	//       "description": "The unique ID of the notification.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/notifications/{notificationId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.notifications"
	//   ]
	// }

}

// method id "directory.notifications.get":

type NotificationsGetCall struct {
	s              *Service
	customer       string
	notificationId string
	urlParams_     gensupport.URLParams
	ifNoneMatch_   string
	ctx_           context.Context
	header_        http.Header
}

// Get: Retrieves a notification.
func (r *NotificationsService) Get(customer string, notificationId string) *NotificationsGetCall {
	c := &NotificationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.notificationId = notificationId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NotificationsGetCall) Fields(s ...googleapi.Field) *NotificationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *NotificationsGetCall) IfNoneMatch(entityTag string) *NotificationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NotificationsGetCall) Context(ctx context.Context) *NotificationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NotificationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NotificationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/notifications/{notificationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":       c.customer,
		"notificationId": c.notificationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.notifications.get" call.
// Exactly one of *Notification or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Notification.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *NotificationsGetCall) Do(opts ...googleapi.CallOption) (*Notification, error) {
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
	ret := &Notification{
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
	//   "description": "Retrieves a notification.",
	//   "httpMethod": "GET",
	//   "id": "directory.notifications.get",
	//   "parameterOrder": [
	//     "customer",
	//     "notificationId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. The customerId is also returned as part of the Users resource.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "notificationId": {
	//       "description": "The unique ID of the notification.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/notifications/{notificationId}",
	//   "response": {
	//     "$ref": "Notification"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.notifications"
	//   ]
	// }

}

// method id "directory.notifications.list":

type NotificationsListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of notifications.
func (r *NotificationsService) List(customer string) *NotificationsListCall {
	c := &NotificationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// Language sets the optional parameter "language": The ISO 639-1 code
// of the language notifications are returned in. The default is English
// (en).
func (c *NotificationsListCall) Language(language string) *NotificationsListCall {
	c.urlParams_.Set("language", language)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of notifications to return per page. The default is 100.
func (c *NotificationsListCall) MaxResults(maxResults int64) *NotificationsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": The token to
// specify the page of results to retrieve.
func (c *NotificationsListCall) PageToken(pageToken string) *NotificationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NotificationsListCall) Fields(s ...googleapi.Field) *NotificationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *NotificationsListCall) IfNoneMatch(entityTag string) *NotificationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NotificationsListCall) Context(ctx context.Context) *NotificationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NotificationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NotificationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/notifications")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.notifications.list" call.
// Exactly one of *Notifications or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Notifications.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *NotificationsListCall) Do(opts ...googleapi.CallOption) (*Notifications, error) {
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
	ret := &Notifications{
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
	//   "description": "Retrieves a list of notifications.",
	//   "httpMethod": "GET",
	//   "id": "directory.notifications.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "language": {
	//       "description": "The ISO 639-1 code of the language notifications are returned in. The default is English (en).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of notifications to return per page. The default is 100.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The token to specify the page of results to retrieve.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/notifications",
	//   "response": {
	//     "$ref": "Notifications"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.notifications"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *NotificationsListCall) Pages(ctx context.Context, f func(*Notifications) error) error {
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

// method id "directory.notifications.patch":

type NotificationsPatchCall struct {
	s              *Service
	customer       string
	notificationId string
	notification   *Notification
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Patch: Updates a notification. This method supports patch semantics.
func (r *NotificationsService) Patch(customer string, notificationId string, notification *Notification) *NotificationsPatchCall {
	c := &NotificationsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.notificationId = notificationId
	c.notification = notification
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NotificationsPatchCall) Fields(s ...googleapi.Field) *NotificationsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NotificationsPatchCall) Context(ctx context.Context) *NotificationsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NotificationsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NotificationsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.notification)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/notifications/{notificationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":       c.customer,
		"notificationId": c.notificationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.notifications.patch" call.
// Exactly one of *Notification or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Notification.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *NotificationsPatchCall) Do(opts ...googleapi.CallOption) (*Notification, error) {
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
	ret := &Notification{
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
	//   "description": "Updates a notification. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.notifications.patch",
	//   "parameterOrder": [
	//     "customer",
	//     "notificationId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "notificationId": {
	//       "description": "The unique ID of the notification.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/notifications/{notificationId}",
	//   "request": {
	//     "$ref": "Notification"
	//   },
	//   "response": {
	//     "$ref": "Notification"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.notifications"
	//   ]
	// }

}

// method id "directory.notifications.update":

type NotificationsUpdateCall struct {
	s              *Service
	customer       string
	notificationId string
	notification   *Notification
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Update: Updates a notification.
func (r *NotificationsService) Update(customer string, notificationId string, notification *Notification) *NotificationsUpdateCall {
	c := &NotificationsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.notificationId = notificationId
	c.notification = notification
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NotificationsUpdateCall) Fields(s ...googleapi.Field) *NotificationsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NotificationsUpdateCall) Context(ctx context.Context) *NotificationsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NotificationsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NotificationsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.notification)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/notifications/{notificationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":       c.customer,
		"notificationId": c.notificationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.notifications.update" call.
// Exactly one of *Notification or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Notification.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *NotificationsUpdateCall) Do(opts ...googleapi.CallOption) (*Notification, error) {
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
	ret := &Notification{
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
	//   "description": "Updates a notification.",
	//   "httpMethod": "PUT",
	//   "id": "directory.notifications.update",
	//   "parameterOrder": [
	//     "customer",
	//     "notificationId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "notificationId": {
	//       "description": "The unique ID of the notification.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/notifications/{notificationId}",
	//   "request": {
	//     "$ref": "Notification"
	//   },
	//   "response": {
	//     "$ref": "Notification"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.notifications"
	//   ]
	// }

}

// method id "directory.orgunits.delete":

type OrgunitsDeleteCall struct {
	s           *Service
	customerId  string
	orgUnitPath []string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Delete: Remove Organization Unit
func (r *OrgunitsService) Delete(customerId string, orgUnitPath []string) *OrgunitsDeleteCall {
	c := &OrgunitsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.orgUnitPath = append([]string{}, orgUnitPath...)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OrgunitsDeleteCall) Fields(s ...googleapi.Field) *OrgunitsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OrgunitsDeleteCall) Context(ctx context.Context) *OrgunitsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OrgunitsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OrgunitsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/orgunits{/orgUnitPath*}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId":  c.customerId,
		"orgUnitPath": c.orgUnitPath[0],
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.orgunits.delete" call.
func (c *OrgunitsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove Organization Unit",
	//   "httpMethod": "DELETE",
	//   "id": "directory.orgunits.delete",
	//   "parameterOrder": [
	//     "customerId",
	//     "orgUnitPath"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "orgUnitPath": {
	//       "description": "Full path of the organization unit or its Id",
	//       "location": "path",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/orgunits{/orgUnitPath*}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.orgunit"
	//   ]
	// }

}

// method id "directory.orgunits.get":

type OrgunitsGetCall struct {
	s            *Service
	customerId   string
	orgUnitPath  []string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve Organization Unit
func (r *OrgunitsService) Get(customerId string, orgUnitPath []string) *OrgunitsGetCall {
	c := &OrgunitsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.orgUnitPath = append([]string{}, orgUnitPath...)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OrgunitsGetCall) Fields(s ...googleapi.Field) *OrgunitsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *OrgunitsGetCall) IfNoneMatch(entityTag string) *OrgunitsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OrgunitsGetCall) Context(ctx context.Context) *OrgunitsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OrgunitsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OrgunitsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/orgunits{/orgUnitPath*}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId":  c.customerId,
		"orgUnitPath": c.orgUnitPath[0],
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.orgunits.get" call.
// Exactly one of *OrgUnit or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *OrgUnit.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *OrgunitsGetCall) Do(opts ...googleapi.CallOption) (*OrgUnit, error) {
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
	ret := &OrgUnit{
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
	//   "description": "Retrieve Organization Unit",
	//   "httpMethod": "GET",
	//   "id": "directory.orgunits.get",
	//   "parameterOrder": [
	//     "customerId",
	//     "orgUnitPath"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "orgUnitPath": {
	//       "description": "Full path of the organization unit or its Id",
	//       "location": "path",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/orgunits{/orgUnitPath*}",
	//   "response": {
	//     "$ref": "OrgUnit"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.orgunit",
	//     "https://www.googleapis.com/auth/admin.directory.orgunit.readonly"
	//   ]
	// }

}

// method id "directory.orgunits.insert":

type OrgunitsInsertCall struct {
	s          *Service
	customerId string
	orgunit    *OrgUnit
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Add Organization Unit
func (r *OrgunitsService) Insert(customerId string, orgunit *OrgUnit) *OrgunitsInsertCall {
	c := &OrgunitsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.orgunit = orgunit
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OrgunitsInsertCall) Fields(s ...googleapi.Field) *OrgunitsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OrgunitsInsertCall) Context(ctx context.Context) *OrgunitsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OrgunitsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OrgunitsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.orgunit)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/orgunits")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.orgunits.insert" call.
// Exactly one of *OrgUnit or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *OrgUnit.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *OrgunitsInsertCall) Do(opts ...googleapi.CallOption) (*OrgUnit, error) {
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
	ret := &OrgUnit{
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
	//   "description": "Add Organization Unit",
	//   "httpMethod": "POST",
	//   "id": "directory.orgunits.insert",
	//   "parameterOrder": [
	//     "customerId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/orgunits",
	//   "request": {
	//     "$ref": "OrgUnit"
	//   },
	//   "response": {
	//     "$ref": "OrgUnit"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.orgunit"
	//   ]
	// }

}

// method id "directory.orgunits.list":

type OrgunitsListCall struct {
	s            *Service
	customerId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve all Organization Units
func (r *OrgunitsService) List(customerId string) *OrgunitsListCall {
	c := &OrgunitsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	return c
}

// OrgUnitPath sets the optional parameter "orgUnitPath": the
// URL-encoded organization unit's path or its Id
func (c *OrgunitsListCall) OrgUnitPath(orgUnitPath string) *OrgunitsListCall {
	c.urlParams_.Set("orgUnitPath", orgUnitPath)
	return c
}

// Type sets the optional parameter "type": Whether to return all
// sub-organizations or just immediate children
//
// Possible values:
//   "all" - All sub-organization units.
//   "children" - Immediate children only (default).
func (c *OrgunitsListCall) Type(type_ string) *OrgunitsListCall {
	c.urlParams_.Set("type", type_)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OrgunitsListCall) Fields(s ...googleapi.Field) *OrgunitsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *OrgunitsListCall) IfNoneMatch(entityTag string) *OrgunitsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OrgunitsListCall) Context(ctx context.Context) *OrgunitsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OrgunitsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OrgunitsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/orgunits")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.orgunits.list" call.
// Exactly one of *OrgUnits or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *OrgUnits.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *OrgunitsListCall) Do(opts ...googleapi.CallOption) (*OrgUnits, error) {
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
	ret := &OrgUnits{
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
	//   "description": "Retrieve all Organization Units",
	//   "httpMethod": "GET",
	//   "id": "directory.orgunits.list",
	//   "parameterOrder": [
	//     "customerId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "orgUnitPath": {
	//       "default": "",
	//       "description": "the URL-encoded organization unit's path or its Id",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "type": {
	//       "description": "Whether to return all sub-organizations or just immediate children",
	//       "enum": [
	//         "all",
	//         "children"
	//       ],
	//       "enumDescriptions": [
	//         "All sub-organization units.",
	//         "Immediate children only (default)."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/orgunits",
	//   "response": {
	//     "$ref": "OrgUnits"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.orgunit",
	//     "https://www.googleapis.com/auth/admin.directory.orgunit.readonly"
	//   ]
	// }

}

// method id "directory.orgunits.patch":

type OrgunitsPatchCall struct {
	s           *Service
	customerId  string
	orgUnitPath []string
	orgunit     *OrgUnit
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Patch: Update Organization Unit. This method supports patch
// semantics.
func (r *OrgunitsService) Patch(customerId string, orgUnitPath []string, orgunit *OrgUnit) *OrgunitsPatchCall {
	c := &OrgunitsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.orgUnitPath = append([]string{}, orgUnitPath...)
	c.orgunit = orgunit
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OrgunitsPatchCall) Fields(s ...googleapi.Field) *OrgunitsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OrgunitsPatchCall) Context(ctx context.Context) *OrgunitsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OrgunitsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OrgunitsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.orgunit)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/orgunits{/orgUnitPath*}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId":  c.customerId,
		"orgUnitPath": c.orgUnitPath[0],
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.orgunits.patch" call.
// Exactly one of *OrgUnit or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *OrgUnit.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *OrgunitsPatchCall) Do(opts ...googleapi.CallOption) (*OrgUnit, error) {
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
	ret := &OrgUnit{
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
	//   "description": "Update Organization Unit. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.orgunits.patch",
	//   "parameterOrder": [
	//     "customerId",
	//     "orgUnitPath"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "orgUnitPath": {
	//       "description": "Full path of the organization unit or its Id",
	//       "location": "path",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/orgunits{/orgUnitPath*}",
	//   "request": {
	//     "$ref": "OrgUnit"
	//   },
	//   "response": {
	//     "$ref": "OrgUnit"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.orgunit"
	//   ]
	// }

}

// method id "directory.orgunits.update":

type OrgunitsUpdateCall struct {
	s           *Service
	customerId  string
	orgUnitPath []string
	orgunit     *OrgUnit
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Update: Update Organization Unit
func (r *OrgunitsService) Update(customerId string, orgUnitPath []string, orgunit *OrgUnit) *OrgunitsUpdateCall {
	c := &OrgunitsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.orgUnitPath = append([]string{}, orgUnitPath...)
	c.orgunit = orgunit
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OrgunitsUpdateCall) Fields(s ...googleapi.Field) *OrgunitsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OrgunitsUpdateCall) Context(ctx context.Context) *OrgunitsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OrgunitsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OrgunitsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.orgunit)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/orgunits{/orgUnitPath*}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId":  c.customerId,
		"orgUnitPath": c.orgUnitPath[0],
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.orgunits.update" call.
// Exactly one of *OrgUnit or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *OrgUnit.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *OrgunitsUpdateCall) Do(opts ...googleapi.CallOption) (*OrgUnit, error) {
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
	ret := &OrgUnit{
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
	//   "description": "Update Organization Unit",
	//   "httpMethod": "PUT",
	//   "id": "directory.orgunits.update",
	//   "parameterOrder": [
	//     "customerId",
	//     "orgUnitPath"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "orgUnitPath": {
	//       "description": "Full path of the organization unit or its Id",
	//       "location": "path",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/orgunits{/orgUnitPath*}",
	//   "request": {
	//     "$ref": "OrgUnit"
	//   },
	//   "response": {
	//     "$ref": "OrgUnit"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.orgunit"
	//   ]
	// }

}

// method id "directory.privileges.list":

type PrivilegesListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a paginated list of all privileges for a customer.
func (r *PrivilegesService) List(customer string) *PrivilegesListCall {
	c := &PrivilegesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PrivilegesListCall) Fields(s ...googleapi.Field) *PrivilegesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PrivilegesListCall) IfNoneMatch(entityTag string) *PrivilegesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PrivilegesListCall) Context(ctx context.Context) *PrivilegesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PrivilegesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PrivilegesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles/ALL/privileges")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.privileges.list" call.
// Exactly one of *Privileges or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Privileges.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *PrivilegesListCall) Do(opts ...googleapi.CallOption) (*Privileges, error) {
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
	ret := &Privileges{
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
	//   "description": "Retrieves a paginated list of all privileges for a customer.",
	//   "httpMethod": "GET",
	//   "id": "directory.privileges.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles/ALL/privileges",
	//   "response": {
	//     "$ref": "Privileges"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement",
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly"
	//   ]
	// }

}

// method id "directory.resources.calendars.delete":

type ResourcesCalendarsDeleteCall struct {
	s                  *Service
	customer           string
	calendarResourceId string
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Delete: Deletes a calendar resource.
func (r *ResourcesCalendarsService) Delete(customer string, calendarResourceId string) *ResourcesCalendarsDeleteCall {
	c := &ResourcesCalendarsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.calendarResourceId = calendarResourceId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourcesCalendarsDeleteCall) Fields(s ...googleapi.Field) *ResourcesCalendarsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourcesCalendarsDeleteCall) Context(ctx context.Context) *ResourcesCalendarsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourcesCalendarsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourcesCalendarsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/resources/calendars/{calendarResourceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":           c.customer,
		"calendarResourceId": c.calendarResourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.resources.calendars.delete" call.
func (c *ResourcesCalendarsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a calendar resource.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.resources.calendars.delete",
	//   "parameterOrder": [
	//     "customer",
	//     "calendarResourceId"
	//   ],
	//   "parameters": {
	//     "calendarResourceId": {
	//       "description": "The unique ID of the calendar resource to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. As an account administrator, you can also use the my_customer alias to represent your account's customer ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/resources/calendars/{calendarResourceId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar"
	//   ]
	// }

}

// method id "directory.resources.calendars.get":

type ResourcesCalendarsGetCall struct {
	s                  *Service
	customer           string
	calendarResourceId string
	urlParams_         gensupport.URLParams
	ifNoneMatch_       string
	ctx_               context.Context
	header_            http.Header
}

// Get: Retrieves a calendar resource.
func (r *ResourcesCalendarsService) Get(customer string, calendarResourceId string) *ResourcesCalendarsGetCall {
	c := &ResourcesCalendarsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.calendarResourceId = calendarResourceId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourcesCalendarsGetCall) Fields(s ...googleapi.Field) *ResourcesCalendarsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ResourcesCalendarsGetCall) IfNoneMatch(entityTag string) *ResourcesCalendarsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourcesCalendarsGetCall) Context(ctx context.Context) *ResourcesCalendarsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourcesCalendarsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourcesCalendarsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/resources/calendars/{calendarResourceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":           c.customer,
		"calendarResourceId": c.calendarResourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.resources.calendars.get" call.
// Exactly one of *CalendarResource or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarResource.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResourcesCalendarsGetCall) Do(opts ...googleapi.CallOption) (*CalendarResource, error) {
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
	ret := &CalendarResource{
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
	//   "description": "Retrieves a calendar resource.",
	//   "httpMethod": "GET",
	//   "id": "directory.resources.calendars.get",
	//   "parameterOrder": [
	//     "customer",
	//     "calendarResourceId"
	//   ],
	//   "parameters": {
	//     "calendarResourceId": {
	//       "description": "The unique ID of the calendar resource to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. As an account administrator, you can also use the my_customer alias to represent your account's customer ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/resources/calendars/{calendarResourceId}",
	//   "response": {
	//     "$ref": "CalendarResource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar",
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar.readonly"
	//   ]
	// }

}

// method id "directory.resources.calendars.insert":

type ResourcesCalendarsInsertCall struct {
	s                *Service
	customer         string
	calendarresource *CalendarResource
	urlParams_       gensupport.URLParams
	ctx_             context.Context
	header_          http.Header
}

// Insert: Inserts a calendar resource.
func (r *ResourcesCalendarsService) Insert(customer string, calendarresource *CalendarResource) *ResourcesCalendarsInsertCall {
	c := &ResourcesCalendarsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.calendarresource = calendarresource
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourcesCalendarsInsertCall) Fields(s ...googleapi.Field) *ResourcesCalendarsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourcesCalendarsInsertCall) Context(ctx context.Context) *ResourcesCalendarsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourcesCalendarsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourcesCalendarsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendarresource)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/resources/calendars")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.resources.calendars.insert" call.
// Exactly one of *CalendarResource or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarResource.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResourcesCalendarsInsertCall) Do(opts ...googleapi.CallOption) (*CalendarResource, error) {
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
	ret := &CalendarResource{
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
	//   "description": "Inserts a calendar resource.",
	//   "httpMethod": "POST",
	//   "id": "directory.resources.calendars.insert",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. As an account administrator, you can also use the my_customer alias to represent your account's customer ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/resources/calendars",
	//   "request": {
	//     "$ref": "CalendarResource"
	//   },
	//   "response": {
	//     "$ref": "CalendarResource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar"
	//   ]
	// }

}

// method id "directory.resources.calendars.list":

type ResourcesCalendarsListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of calendar resources for an account.
func (r *ResourcesCalendarsService) List(customer string) *ResourcesCalendarsListCall {
	c := &ResourcesCalendarsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return.
func (c *ResourcesCalendarsListCall) MaxResults(maxResults int64) *ResourcesCalendarsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// the next page in the list.
func (c *ResourcesCalendarsListCall) PageToken(pageToken string) *ResourcesCalendarsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourcesCalendarsListCall) Fields(s ...googleapi.Field) *ResourcesCalendarsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ResourcesCalendarsListCall) IfNoneMatch(entityTag string) *ResourcesCalendarsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourcesCalendarsListCall) Context(ctx context.Context) *ResourcesCalendarsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourcesCalendarsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourcesCalendarsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/resources/calendars")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.resources.calendars.list" call.
// Exactly one of *CalendarResources or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarResources.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResourcesCalendarsListCall) Do(opts ...googleapi.CallOption) (*CalendarResources, error) {
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
	ret := &CalendarResources{
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
	//   "description": "Retrieves a list of calendar resources for an account.",
	//   "httpMethod": "GET",
	//   "id": "directory.resources.calendars.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. As an account administrator, you can also use the my_customer alias to represent your account's customer ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "500",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify the next page in the list.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/resources/calendars",
	//   "response": {
	//     "$ref": "CalendarResources"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar",
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ResourcesCalendarsListCall) Pages(ctx context.Context, f func(*CalendarResources) error) error {
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

// method id "directory.resources.calendars.patch":

type ResourcesCalendarsPatchCall struct {
	s                  *Service
	customer           string
	calendarResourceId string
	calendarresource   *CalendarResource
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Patch: Updates a calendar resource. This method supports patch
// semantics.
func (r *ResourcesCalendarsService) Patch(customer string, calendarResourceId string, calendarresource *CalendarResource) *ResourcesCalendarsPatchCall {
	c := &ResourcesCalendarsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.calendarResourceId = calendarResourceId
	c.calendarresource = calendarresource
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourcesCalendarsPatchCall) Fields(s ...googleapi.Field) *ResourcesCalendarsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourcesCalendarsPatchCall) Context(ctx context.Context) *ResourcesCalendarsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourcesCalendarsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourcesCalendarsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendarresource)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/resources/calendars/{calendarResourceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":           c.customer,
		"calendarResourceId": c.calendarResourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.resources.calendars.patch" call.
// Exactly one of *CalendarResource or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarResource.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResourcesCalendarsPatchCall) Do(opts ...googleapi.CallOption) (*CalendarResource, error) {
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
	ret := &CalendarResource{
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
	//   "description": "Updates a calendar resource. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.resources.calendars.patch",
	//   "parameterOrder": [
	//     "customer",
	//     "calendarResourceId"
	//   ],
	//   "parameters": {
	//     "calendarResourceId": {
	//       "description": "The unique ID of the calendar resource to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. As an account administrator, you can also use the my_customer alias to represent your account's customer ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/resources/calendars/{calendarResourceId}",
	//   "request": {
	//     "$ref": "CalendarResource"
	//   },
	//   "response": {
	//     "$ref": "CalendarResource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar"
	//   ]
	// }

}

// method id "directory.resources.calendars.update":

type ResourcesCalendarsUpdateCall struct {
	s                  *Service
	customer           string
	calendarResourceId string
	calendarresource   *CalendarResource
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Update: Updates a calendar resource.
func (r *ResourcesCalendarsService) Update(customer string, calendarResourceId string, calendarresource *CalendarResource) *ResourcesCalendarsUpdateCall {
	c := &ResourcesCalendarsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.calendarResourceId = calendarResourceId
	c.calendarresource = calendarresource
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ResourcesCalendarsUpdateCall) Fields(s ...googleapi.Field) *ResourcesCalendarsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ResourcesCalendarsUpdateCall) Context(ctx context.Context) *ResourcesCalendarsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ResourcesCalendarsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ResourcesCalendarsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendarresource)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/resources/calendars/{calendarResourceId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":           c.customer,
		"calendarResourceId": c.calendarResourceId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.resources.calendars.update" call.
// Exactly one of *CalendarResource or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarResource.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ResourcesCalendarsUpdateCall) Do(opts ...googleapi.CallOption) (*CalendarResource, error) {
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
	ret := &CalendarResource{
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
	//   "description": "Updates a calendar resource.",
	//   "httpMethod": "PUT",
	//   "id": "directory.resources.calendars.update",
	//   "parameterOrder": [
	//     "customer",
	//     "calendarResourceId"
	//   ],
	//   "parameters": {
	//     "calendarResourceId": {
	//       "description": "The unique ID of the calendar resource to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "customer": {
	//       "description": "The unique ID for the customer's Google account. As an account administrator, you can also use the my_customer alias to represent your account's customer ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/resources/calendars/{calendarResourceId}",
	//   "request": {
	//     "$ref": "CalendarResource"
	//   },
	//   "response": {
	//     "$ref": "CalendarResource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.resource.calendar"
	//   ]
	// }

}

// method id "directory.roleAssignments.delete":

type RoleAssignmentsDeleteCall struct {
	s                *Service
	customer         string
	roleAssignmentId string
	urlParams_       gensupport.URLParams
	ctx_             context.Context
	header_          http.Header
}

// Delete: Deletes a role assignment.
func (r *RoleAssignmentsService) Delete(customer string, roleAssignmentId string) *RoleAssignmentsDeleteCall {
	c := &RoleAssignmentsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleAssignmentId = roleAssignmentId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RoleAssignmentsDeleteCall) Fields(s ...googleapi.Field) *RoleAssignmentsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RoleAssignmentsDeleteCall) Context(ctx context.Context) *RoleAssignmentsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RoleAssignmentsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RoleAssignmentsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roleassignments/{roleAssignmentId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":         c.customer,
		"roleAssignmentId": c.roleAssignmentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roleAssignments.delete" call.
func (c *RoleAssignmentsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a role assignment.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.roleAssignments.delete",
	//   "parameterOrder": [
	//     "customer",
	//     "roleAssignmentId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "roleAssignmentId": {
	//       "description": "Immutable ID of the role assignment.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roleassignments/{roleAssignmentId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement"
	//   ]
	// }

}

// method id "directory.roleAssignments.get":

type RoleAssignmentsGetCall struct {
	s                *Service
	customer         string
	roleAssignmentId string
	urlParams_       gensupport.URLParams
	ifNoneMatch_     string
	ctx_             context.Context
	header_          http.Header
}

// Get: Retrieve a role assignment.
func (r *RoleAssignmentsService) Get(customer string, roleAssignmentId string) *RoleAssignmentsGetCall {
	c := &RoleAssignmentsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleAssignmentId = roleAssignmentId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RoleAssignmentsGetCall) Fields(s ...googleapi.Field) *RoleAssignmentsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RoleAssignmentsGetCall) IfNoneMatch(entityTag string) *RoleAssignmentsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RoleAssignmentsGetCall) Context(ctx context.Context) *RoleAssignmentsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RoleAssignmentsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RoleAssignmentsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roleassignments/{roleAssignmentId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer":         c.customer,
		"roleAssignmentId": c.roleAssignmentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roleAssignments.get" call.
// Exactly one of *RoleAssignment or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RoleAssignment.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RoleAssignmentsGetCall) Do(opts ...googleapi.CallOption) (*RoleAssignment, error) {
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
	ret := &RoleAssignment{
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
	//   "description": "Retrieve a role assignment.",
	//   "httpMethod": "GET",
	//   "id": "directory.roleAssignments.get",
	//   "parameterOrder": [
	//     "customer",
	//     "roleAssignmentId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "roleAssignmentId": {
	//       "description": "Immutable ID of the role assignment.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roleassignments/{roleAssignmentId}",
	//   "response": {
	//     "$ref": "RoleAssignment"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement",
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly"
	//   ]
	// }

}

// method id "directory.roleAssignments.insert":

type RoleAssignmentsInsertCall struct {
	s              *Service
	customer       string
	roleassignment *RoleAssignment
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Insert: Creates a role assignment.
func (r *RoleAssignmentsService) Insert(customer string, roleassignment *RoleAssignment) *RoleAssignmentsInsertCall {
	c := &RoleAssignmentsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleassignment = roleassignment
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RoleAssignmentsInsertCall) Fields(s ...googleapi.Field) *RoleAssignmentsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RoleAssignmentsInsertCall) Context(ctx context.Context) *RoleAssignmentsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RoleAssignmentsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RoleAssignmentsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.roleassignment)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roleassignments")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roleAssignments.insert" call.
// Exactly one of *RoleAssignment or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RoleAssignment.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RoleAssignmentsInsertCall) Do(opts ...googleapi.CallOption) (*RoleAssignment, error) {
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
	ret := &RoleAssignment{
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
	//   "description": "Creates a role assignment.",
	//   "httpMethod": "POST",
	//   "id": "directory.roleAssignments.insert",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roleassignments",
	//   "request": {
	//     "$ref": "RoleAssignment"
	//   },
	//   "response": {
	//     "$ref": "RoleAssignment"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement"
	//   ]
	// }

}

// method id "directory.roleAssignments.list":

type RoleAssignmentsListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a paginated list of all roleAssignments.
func (r *RoleAssignmentsService) List(customer string) *RoleAssignmentsListCall {
	c := &RoleAssignmentsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return.
func (c *RoleAssignmentsListCall) MaxResults(maxResults int64) *RoleAssignmentsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// the next page in the list.
func (c *RoleAssignmentsListCall) PageToken(pageToken string) *RoleAssignmentsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// RoleId sets the optional parameter "roleId": Immutable ID of a role.
// If included in the request, returns only role assignments containing
// this role ID.
func (c *RoleAssignmentsListCall) RoleId(roleId string) *RoleAssignmentsListCall {
	c.urlParams_.Set("roleId", roleId)
	return c
}

// UserKey sets the optional parameter "userKey": The user's primary
// email address, alias email address, or unique user ID. If included in
// the request, returns role assignments only for this user.
func (c *RoleAssignmentsListCall) UserKey(userKey string) *RoleAssignmentsListCall {
	c.urlParams_.Set("userKey", userKey)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RoleAssignmentsListCall) Fields(s ...googleapi.Field) *RoleAssignmentsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RoleAssignmentsListCall) IfNoneMatch(entityTag string) *RoleAssignmentsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RoleAssignmentsListCall) Context(ctx context.Context) *RoleAssignmentsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RoleAssignmentsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RoleAssignmentsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roleassignments")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roleAssignments.list" call.
// Exactly one of *RoleAssignments or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RoleAssignments.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RoleAssignmentsListCall) Do(opts ...googleapi.CallOption) (*RoleAssignments, error) {
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
	ret := &RoleAssignments{
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
	//   "description": "Retrieves a paginated list of all roleAssignments.",
	//   "httpMethod": "GET",
	//   "id": "directory.roleAssignments.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "200",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify the next page in the list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "roleId": {
	//       "description": "Immutable ID of a role. If included in the request, returns only role assignments containing this role ID.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "The user's primary email address, alias email address, or unique user ID. If included in the request, returns role assignments only for this user.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roleassignments",
	//   "response": {
	//     "$ref": "RoleAssignments"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement",
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *RoleAssignmentsListCall) Pages(ctx context.Context, f func(*RoleAssignments) error) error {
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

// method id "directory.roles.delete":

type RolesDeleteCall struct {
	s          *Service
	customer   string
	roleId     string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a role.
func (r *RolesService) Delete(customer string, roleId string) *RolesDeleteCall {
	c := &RolesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleId = roleId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RolesDeleteCall) Fields(s ...googleapi.Field) *RolesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RolesDeleteCall) Context(ctx context.Context) *RolesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RolesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RolesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles/{roleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
		"roleId":   c.roleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roles.delete" call.
func (c *RolesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a role.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.roles.delete",
	//   "parameterOrder": [
	//     "customer",
	//     "roleId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "roleId": {
	//       "description": "Immutable ID of the role.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles/{roleId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement"
	//   ]
	// }

}

// method id "directory.roles.get":

type RolesGetCall struct {
	s            *Service
	customer     string
	roleId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a role.
func (r *RolesService) Get(customer string, roleId string) *RolesGetCall {
	c := &RolesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleId = roleId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RolesGetCall) Fields(s ...googleapi.Field) *RolesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RolesGetCall) IfNoneMatch(entityTag string) *RolesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RolesGetCall) Context(ctx context.Context) *RolesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RolesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RolesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles/{roleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
		"roleId":   c.roleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roles.get" call.
// Exactly one of *Role or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Role.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RolesGetCall) Do(opts ...googleapi.CallOption) (*Role, error) {
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
	ret := &Role{
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
	//   "description": "Retrieves a role.",
	//   "httpMethod": "GET",
	//   "id": "directory.roles.get",
	//   "parameterOrder": [
	//     "customer",
	//     "roleId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "roleId": {
	//       "description": "Immutable ID of the role.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles/{roleId}",
	//   "response": {
	//     "$ref": "Role"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement",
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly"
	//   ]
	// }

}

// method id "directory.roles.insert":

type RolesInsertCall struct {
	s          *Service
	customer   string
	role       *Role
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates a role.
func (r *RolesService) Insert(customer string, role *Role) *RolesInsertCall {
	c := &RolesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.role = role
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RolesInsertCall) Fields(s ...googleapi.Field) *RolesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RolesInsertCall) Context(ctx context.Context) *RolesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RolesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RolesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.role)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roles.insert" call.
// Exactly one of *Role or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Role.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RolesInsertCall) Do(opts ...googleapi.CallOption) (*Role, error) {
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
	ret := &Role{
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
	//   "description": "Creates a role.",
	//   "httpMethod": "POST",
	//   "id": "directory.roles.insert",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles",
	//   "request": {
	//     "$ref": "Role"
	//   },
	//   "response": {
	//     "$ref": "Role"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement"
	//   ]
	// }

}

// method id "directory.roles.list":

type RolesListCall struct {
	s            *Service
	customer     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a paginated list of all the roles in a domain.
func (r *RolesService) List(customer string) *RolesListCall {
	c := &RolesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return.
func (c *RolesListCall) MaxResults(maxResults int64) *RolesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// the next page in the list.
func (c *RolesListCall) PageToken(pageToken string) *RolesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RolesListCall) Fields(s ...googleapi.Field) *RolesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RolesListCall) IfNoneMatch(entityTag string) *RolesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RolesListCall) Context(ctx context.Context) *RolesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RolesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RolesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roles.list" call.
// Exactly one of *Roles or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Roles.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *RolesListCall) Do(opts ...googleapi.CallOption) (*Roles, error) {
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
	ret := &Roles{
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
	//   "description": "Retrieves a paginated list of all the roles in a domain.",
	//   "httpMethod": "GET",
	//   "id": "directory.roles.list",
	//   "parameterOrder": [
	//     "customer"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "100",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify the next page in the list.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles",
	//   "response": {
	//     "$ref": "Roles"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement",
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *RolesListCall) Pages(ctx context.Context, f func(*Roles) error) error {
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

// method id "directory.roles.patch":

type RolesPatchCall struct {
	s          *Service
	customer   string
	roleId     string
	role       *Role
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates a role. This method supports patch semantics.
func (r *RolesService) Patch(customer string, roleId string, role *Role) *RolesPatchCall {
	c := &RolesPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleId = roleId
	c.role = role
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RolesPatchCall) Fields(s ...googleapi.Field) *RolesPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RolesPatchCall) Context(ctx context.Context) *RolesPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RolesPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RolesPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.role)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles/{roleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
		"roleId":   c.roleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roles.patch" call.
// Exactly one of *Role or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Role.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RolesPatchCall) Do(opts ...googleapi.CallOption) (*Role, error) {
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
	ret := &Role{
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
	//   "description": "Updates a role. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.roles.patch",
	//   "parameterOrder": [
	//     "customer",
	//     "roleId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "roleId": {
	//       "description": "Immutable ID of the role.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles/{roleId}",
	//   "request": {
	//     "$ref": "Role"
	//   },
	//   "response": {
	//     "$ref": "Role"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement"
	//   ]
	// }

}

// method id "directory.roles.update":

type RolesUpdateCall struct {
	s          *Service
	customer   string
	roleId     string
	role       *Role
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates a role.
func (r *RolesService) Update(customer string, roleId string, role *Role) *RolesUpdateCall {
	c := &RolesUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customer = customer
	c.roleId = roleId
	c.role = role
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RolesUpdateCall) Fields(s ...googleapi.Field) *RolesUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RolesUpdateCall) Context(ctx context.Context) *RolesUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RolesUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RolesUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.role)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customer}/roles/{roleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customer": c.customer,
		"roleId":   c.roleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.roles.update" call.
// Exactly one of *Role or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Role.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RolesUpdateCall) Do(opts ...googleapi.CallOption) (*Role, error) {
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
	ret := &Role{
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
	//   "description": "Updates a role.",
	//   "httpMethod": "PUT",
	//   "id": "directory.roles.update",
	//   "parameterOrder": [
	//     "customer",
	//     "roleId"
	//   ],
	//   "parameters": {
	//     "customer": {
	//       "description": "Immutable ID of the Google Apps account.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "roleId": {
	//       "description": "Immutable ID of the role.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customer}/roles/{roleId}",
	//   "request": {
	//     "$ref": "Role"
	//   },
	//   "response": {
	//     "$ref": "Role"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.rolemanagement"
	//   ]
	// }

}

// method id "directory.schemas.delete":

type SchemasDeleteCall struct {
	s          *Service
	customerId string
	schemaKey  string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Delete schema
func (r *SchemasService) Delete(customerId string, schemaKey string) *SchemasDeleteCall {
	c := &SchemasDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.schemaKey = schemaKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SchemasDeleteCall) Fields(s ...googleapi.Field) *SchemasDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SchemasDeleteCall) Context(ctx context.Context) *SchemasDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SchemasDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SchemasDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/schemas/{schemaKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"schemaKey":  c.schemaKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.schemas.delete" call.
func (c *SchemasDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Delete schema",
	//   "httpMethod": "DELETE",
	//   "id": "directory.schemas.delete",
	//   "parameterOrder": [
	//     "customerId",
	//     "schemaKey"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "schemaKey": {
	//       "description": "Name or immutable Id of the schema",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/schemas/{schemaKey}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.userschema"
	//   ]
	// }

}

// method id "directory.schemas.get":

type SchemasGetCall struct {
	s            *Service
	customerId   string
	schemaKey    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve schema
func (r *SchemasService) Get(customerId string, schemaKey string) *SchemasGetCall {
	c := &SchemasGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.schemaKey = schemaKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SchemasGetCall) Fields(s ...googleapi.Field) *SchemasGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SchemasGetCall) IfNoneMatch(entityTag string) *SchemasGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SchemasGetCall) Context(ctx context.Context) *SchemasGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SchemasGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SchemasGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/schemas/{schemaKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"schemaKey":  c.schemaKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.schemas.get" call.
// Exactly one of *Schema or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Schema.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SchemasGetCall) Do(opts ...googleapi.CallOption) (*Schema, error) {
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
	ret := &Schema{
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
	//   "description": "Retrieve schema",
	//   "httpMethod": "GET",
	//   "id": "directory.schemas.get",
	//   "parameterOrder": [
	//     "customerId",
	//     "schemaKey"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "schemaKey": {
	//       "description": "Name or immutable Id of the schema",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/schemas/{schemaKey}",
	//   "response": {
	//     "$ref": "Schema"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.userschema",
	//     "https://www.googleapis.com/auth/admin.directory.userschema.readonly"
	//   ]
	// }

}

// method id "directory.schemas.insert":

type SchemasInsertCall struct {
	s          *Service
	customerId string
	schema     *Schema
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Create schema.
func (r *SchemasService) Insert(customerId string, schema *Schema) *SchemasInsertCall {
	c := &SchemasInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.schema = schema
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SchemasInsertCall) Fields(s ...googleapi.Field) *SchemasInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SchemasInsertCall) Context(ctx context.Context) *SchemasInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SchemasInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SchemasInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.schema)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/schemas")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.schemas.insert" call.
// Exactly one of *Schema or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Schema.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SchemasInsertCall) Do(opts ...googleapi.CallOption) (*Schema, error) {
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
	ret := &Schema{
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
	//   "description": "Create schema.",
	//   "httpMethod": "POST",
	//   "id": "directory.schemas.insert",
	//   "parameterOrder": [
	//     "customerId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/schemas",
	//   "request": {
	//     "$ref": "Schema"
	//   },
	//   "response": {
	//     "$ref": "Schema"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.userschema"
	//   ]
	// }

}

// method id "directory.schemas.list":

type SchemasListCall struct {
	s            *Service
	customerId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve all schemas for a customer
func (r *SchemasService) List(customerId string) *SchemasListCall {
	c := &SchemasListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SchemasListCall) Fields(s ...googleapi.Field) *SchemasListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SchemasListCall) IfNoneMatch(entityTag string) *SchemasListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SchemasListCall) Context(ctx context.Context) *SchemasListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SchemasListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SchemasListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/schemas")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.schemas.list" call.
// Exactly one of *Schemas or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Schemas.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SchemasListCall) Do(opts ...googleapi.CallOption) (*Schemas, error) {
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
	ret := &Schemas{
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
	//   "description": "Retrieve all schemas for a customer",
	//   "httpMethod": "GET",
	//   "id": "directory.schemas.list",
	//   "parameterOrder": [
	//     "customerId"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/schemas",
	//   "response": {
	//     "$ref": "Schemas"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.userschema",
	//     "https://www.googleapis.com/auth/admin.directory.userschema.readonly"
	//   ]
	// }

}

// method id "directory.schemas.patch":

type SchemasPatchCall struct {
	s          *Service
	customerId string
	schemaKey  string
	schema     *Schema
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Update schema. This method supports patch semantics.
func (r *SchemasService) Patch(customerId string, schemaKey string, schema *Schema) *SchemasPatchCall {
	c := &SchemasPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.schemaKey = schemaKey
	c.schema = schema
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SchemasPatchCall) Fields(s ...googleapi.Field) *SchemasPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SchemasPatchCall) Context(ctx context.Context) *SchemasPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SchemasPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SchemasPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.schema)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/schemas/{schemaKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"schemaKey":  c.schemaKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.schemas.patch" call.
// Exactly one of *Schema or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Schema.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SchemasPatchCall) Do(opts ...googleapi.CallOption) (*Schema, error) {
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
	ret := &Schema{
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
	//   "description": "Update schema. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.schemas.patch",
	//   "parameterOrder": [
	//     "customerId",
	//     "schemaKey"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "schemaKey": {
	//       "description": "Name or immutable Id of the schema.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/schemas/{schemaKey}",
	//   "request": {
	//     "$ref": "Schema"
	//   },
	//   "response": {
	//     "$ref": "Schema"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.userschema"
	//   ]
	// }

}

// method id "directory.schemas.update":

type SchemasUpdateCall struct {
	s          *Service
	customerId string
	schemaKey  string
	schema     *Schema
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Update schema
func (r *SchemasService) Update(customerId string, schemaKey string, schema *Schema) *SchemasUpdateCall {
	c := &SchemasUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.customerId = customerId
	c.schemaKey = schemaKey
	c.schema = schema
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SchemasUpdateCall) Fields(s ...googleapi.Field) *SchemasUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SchemasUpdateCall) Context(ctx context.Context) *SchemasUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SchemasUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SchemasUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.schema)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "customer/{customerId}/schemas/{schemaKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"customerId": c.customerId,
		"schemaKey":  c.schemaKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.schemas.update" call.
// Exactly one of *Schema or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Schema.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SchemasUpdateCall) Do(opts ...googleapi.CallOption) (*Schema, error) {
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
	ret := &Schema{
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
	//   "description": "Update schema",
	//   "httpMethod": "PUT",
	//   "id": "directory.schemas.update",
	//   "parameterOrder": [
	//     "customerId",
	//     "schemaKey"
	//   ],
	//   "parameters": {
	//     "customerId": {
	//       "description": "Immutable id of the Google Apps account",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "schemaKey": {
	//       "description": "Name or immutable Id of the schema.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "customer/{customerId}/schemas/{schemaKey}",
	//   "request": {
	//     "$ref": "Schema"
	//   },
	//   "response": {
	//     "$ref": "Schema"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.userschema"
	//   ]
	// }

}

// method id "directory.tokens.delete":

type TokensDeleteCall struct {
	s          *Service
	userKey    string
	clientId   string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Delete all access tokens issued by a user for an application.
func (r *TokensService) Delete(userKey string, clientId string) *TokensDeleteCall {
	c := &TokensDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.clientId = clientId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TokensDeleteCall) Fields(s ...googleapi.Field) *TokensDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TokensDeleteCall) Context(ctx context.Context) *TokensDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TokensDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TokensDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/tokens/{clientId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey":  c.userKey,
		"clientId": c.clientId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.tokens.delete" call.
func (c *TokensDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Delete all access tokens issued by a user for an application.",
	//   "httpMethod": "DELETE",
	//   "id": "directory.tokens.delete",
	//   "parameterOrder": [
	//     "userKey",
	//     "clientId"
	//   ],
	//   "parameters": {
	//     "clientId": {
	//       "description": "The Client ID of the application the token is issued to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/tokens/{clientId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.tokens.get":

type TokensGetCall struct {
	s            *Service
	userKey      string
	clientId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Get information about an access token issued by a user.
func (r *TokensService) Get(userKey string, clientId string) *TokensGetCall {
	c := &TokensGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.clientId = clientId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TokensGetCall) Fields(s ...googleapi.Field) *TokensGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TokensGetCall) IfNoneMatch(entityTag string) *TokensGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TokensGetCall) Context(ctx context.Context) *TokensGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TokensGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TokensGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/tokens/{clientId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey":  c.userKey,
		"clientId": c.clientId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.tokens.get" call.
// Exactly one of *Token or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Token.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TokensGetCall) Do(opts ...googleapi.CallOption) (*Token, error) {
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
	ret := &Token{
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
	//   "description": "Get information about an access token issued by a user.",
	//   "httpMethod": "GET",
	//   "id": "directory.tokens.get",
	//   "parameterOrder": [
	//     "userKey",
	//     "clientId"
	//   ],
	//   "parameters": {
	//     "clientId": {
	//       "description": "The Client ID of the application the token is issued to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/tokens/{clientId}",
	//   "response": {
	//     "$ref": "Token"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.tokens.list":

type TokensListCall struct {
	s            *Service
	userKey      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns the set of tokens specified user has issued to 3rd
// party applications.
func (r *TokensService) List(userKey string) *TokensListCall {
	c := &TokensListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TokensListCall) Fields(s ...googleapi.Field) *TokensListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TokensListCall) IfNoneMatch(entityTag string) *TokensListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TokensListCall) Context(ctx context.Context) *TokensListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TokensListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TokensListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/tokens")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.tokens.list" call.
// Exactly one of *Tokens or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Tokens.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TokensListCall) Do(opts ...googleapi.CallOption) (*Tokens, error) {
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
	ret := &Tokens{
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
	//   "description": "Returns the set of tokens specified user has issued to 3rd party applications.",
	//   "httpMethod": "GET",
	//   "id": "directory.tokens.list",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/tokens",
	//   "response": {
	//     "$ref": "Tokens"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.users.delete":

type UsersDeleteCall struct {
	s          *Service
	userKey    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Delete user
func (r *UsersService) Delete(userKey string) *UsersDeleteCall {
	c := &UsersDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDeleteCall) Fields(s ...googleapi.Field) *UsersDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersDeleteCall) Context(ctx context.Context) *UsersDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.delete" call.
func (c *UsersDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Delete user",
	//   "httpMethod": "DELETE",
	//   "id": "directory.users.delete",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.get":

type UsersGetCall struct {
	s            *Service
	userKey      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: retrieve user
func (r *UsersService) Get(userKey string) *UsersGetCall {
	c := &UsersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// CustomFieldMask sets the optional parameter "customFieldMask":
// Comma-separated list of schema names. All fields from these schemas
// are fetched. This should only be set when projection=custom.
func (c *UsersGetCall) CustomFieldMask(customFieldMask string) *UsersGetCall {
	c.urlParams_.Set("customFieldMask", customFieldMask)
	return c
}

// Projection sets the optional parameter "projection": What subset of
// fields to fetch for this user.
//
// Possible values:
//   "basic" (default) - Do not include any custom fields for the user.
//   "custom" - Include custom fields from schemas mentioned in
// customFieldMask.
//   "full" - Include all fields associated with this user.
func (c *UsersGetCall) Projection(projection string) *UsersGetCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// ViewType sets the optional parameter "viewType": Whether to fetch the
// ADMIN_VIEW or DOMAIN_PUBLIC view of the user.
//
// Possible values:
//   "admin_view" (default) - Fetches the ADMIN_VIEW of the user.
//   "domain_public" - Fetches the DOMAIN_PUBLIC view of the user.
func (c *UsersGetCall) ViewType(viewType string) *UsersGetCall {
	c.urlParams_.Set("viewType", viewType)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersGetCall) Fields(s ...googleapi.Field) *UsersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersGetCall) IfNoneMatch(entityTag string) *UsersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersGetCall) Context(ctx context.Context) *UsersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.get" call.
// Exactly one of *User or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *User.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *UsersGetCall) Do(opts ...googleapi.CallOption) (*User, error) {
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
	ret := &User{
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
	//   "description": "retrieve user",
	//   "httpMethod": "GET",
	//   "id": "directory.users.get",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "customFieldMask": {
	//       "description": "Comma-separated list of schema names. All fields from these schemas are fetched. This should only be set when projection=custom.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "default": "basic",
	//       "description": "What subset of fields to fetch for this user.",
	//       "enum": [
	//         "basic",
	//         "custom",
	//         "full"
	//       ],
	//       "enumDescriptions": [
	//         "Do not include any custom fields for the user.",
	//         "Include custom fields from schemas mentioned in customFieldMask.",
	//         "Include all fields associated with this user."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "viewType": {
	//       "default": "admin_view",
	//       "description": "Whether to fetch the ADMIN_VIEW or DOMAIN_PUBLIC view of the user.",
	//       "enum": [
	//         "admin_view",
	//         "domain_public"
	//       ],
	//       "enumDescriptions": [
	//         "Fetches the ADMIN_VIEW of the user.",
	//         "Fetches the DOMAIN_PUBLIC view of the user."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}",
	//   "response": {
	//     "$ref": "User"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ]
	// }

}

// method id "directory.users.insert":

type UsersInsertCall struct {
	s          *Service
	user       *User
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: create user.
func (r *UsersService) Insert(user *User) *UsersInsertCall {
	c := &UsersInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.user = user
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersInsertCall) Fields(s ...googleapi.Field) *UsersInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersInsertCall) Context(ctx context.Context) *UsersInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.user)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.insert" call.
// Exactly one of *User or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *User.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *UsersInsertCall) Do(opts ...googleapi.CallOption) (*User, error) {
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
	ret := &User{
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
	//   "description": "create user.",
	//   "httpMethod": "POST",
	//   "id": "directory.users.insert",
	//   "path": "users",
	//   "request": {
	//     "$ref": "User"
	//   },
	//   "response": {
	//     "$ref": "User"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.list":

type UsersListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieve either deleted users or all users in a domain
// (paginated)
func (r *UsersService) List() *UsersListCall {
	c := &UsersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// CustomFieldMask sets the optional parameter "customFieldMask":
// Comma-separated list of schema names. All fields from these schemas
// are fetched. This should only be set when projection=custom.
func (c *UsersListCall) CustomFieldMask(customFieldMask string) *UsersListCall {
	c.urlParams_.Set("customFieldMask", customFieldMask)
	return c
}

// Customer sets the optional parameter "customer": Immutable id of the
// Google Apps account. In case of multi-domain, to fetch all users for
// a customer, fill this field instead of domain.
func (c *UsersListCall) Customer(customer string) *UsersListCall {
	c.urlParams_.Set("customer", customer)
	return c
}

// Domain sets the optional parameter "domain": Name of the domain. Fill
// this field to get users from only this domain. To return all users in
// a multi-domain fill customer field instead.
func (c *UsersListCall) Domain(domain string) *UsersListCall {
	c.urlParams_.Set("domain", domain)
	return c
}

// Event sets the optional parameter "event": Event on which
// subscription is intended (if subscribing)
//
// Possible values:
//   "add" - User Created Event
//   "delete" - User Deleted Event
//   "makeAdmin" - User Admin Status Change Event
//   "undelete" - User Undeleted Event
//   "update" - User Updated Event
func (c *UsersListCall) Event(event string) *UsersListCall {
	c.urlParams_.Set("event", event)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 100. Max allowed is 500
func (c *UsersListCall) MaxResults(maxResults int64) *UsersListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": Column to use for
// sorting results
//
// Possible values:
//   "email" - Primary email of the user.
//   "familyName" - User's family name.
//   "givenName" - User's given name.
func (c *UsersListCall) OrderBy(orderBy string) *UsersListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list
func (c *UsersListCall) PageToken(pageToken string) *UsersListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Projection sets the optional parameter "projection": What subset of
// fields to fetch for this user.
//
// Possible values:
//   "basic" (default) - Do not include any custom fields for the user.
//   "custom" - Include custom fields from schemas mentioned in
// customFieldMask.
//   "full" - Include all fields associated with this user.
func (c *UsersListCall) Projection(projection string) *UsersListCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Query sets the optional parameter "query": Query string search.
// Should be of the form "". Complete documentation is at
// https://developers.google.com/admin-sdk/directory/v1/guides/search-users
func (c *UsersListCall) Query(query string) *UsersListCall {
	c.urlParams_.Set("query", query)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": If set to true
// retrieves the list of deleted users. Default is false
func (c *UsersListCall) ShowDeleted(showDeleted string) *UsersListCall {
	c.urlParams_.Set("showDeleted", showDeleted)
	return c
}

// SortOrder sets the optional parameter "sortOrder": Whether to return
// results in ascending or descending order.
//
// Possible values:
//   "ASCENDING" - Ascending order.
//   "DESCENDING" - Descending order.
func (c *UsersListCall) SortOrder(sortOrder string) *UsersListCall {
	c.urlParams_.Set("sortOrder", sortOrder)
	return c
}

// ViewType sets the optional parameter "viewType": Whether to fetch the
// ADMIN_VIEW or DOMAIN_PUBLIC view of the user.
//
// Possible values:
//   "admin_view" (default) - Fetches the ADMIN_VIEW of the user.
//   "domain_public" - Fetches the DOMAIN_PUBLIC view of the user.
func (c *UsersListCall) ViewType(viewType string) *UsersListCall {
	c.urlParams_.Set("viewType", viewType)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersListCall) Fields(s ...googleapi.Field) *UsersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersListCall) IfNoneMatch(entityTag string) *UsersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersListCall) Context(ctx context.Context) *UsersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.list" call.
// Exactly one of *Users or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Users.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersListCall) Do(opts ...googleapi.CallOption) (*Users, error) {
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
	ret := &Users{
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
	//   "description": "Retrieve either deleted users or all users in a domain (paginated)",
	//   "httpMethod": "GET",
	//   "id": "directory.users.list",
	//   "parameters": {
	//     "customFieldMask": {
	//       "description": "Comma-separated list of schema names. All fields from these schemas are fetched. This should only be set when projection=custom.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account. In case of multi-domain, to fetch all users for a customer, fill this field instead of domain.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "domain": {
	//       "description": "Name of the domain. Fill this field to get users from only this domain. To return all users in a multi-domain fill customer field instead.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "event": {
	//       "description": "Event on which subscription is intended (if subscribing)",
	//       "enum": [
	//         "add",
	//         "delete",
	//         "makeAdmin",
	//         "undelete",
	//         "update"
	//       ],
	//       "enumDescriptions": [
	//         "User Created Event",
	//         "User Deleted Event",
	//         "User Admin Status Change Event",
	//         "User Undeleted Event",
	//         "User Updated Event"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 100. Max allowed is 500",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "500",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "Column to use for sorting results",
	//       "enum": [
	//         "email",
	//         "familyName",
	//         "givenName"
	//       ],
	//       "enumDescriptions": [
	//         "Primary email of the user.",
	//         "User's family name.",
	//         "User's given name."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "default": "basic",
	//       "description": "What subset of fields to fetch for this user.",
	//       "enum": [
	//         "basic",
	//         "custom",
	//         "full"
	//       ],
	//       "enumDescriptions": [
	//         "Do not include any custom fields for the user.",
	//         "Include custom fields from schemas mentioned in customFieldMask.",
	//         "Include all fields associated with this user."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "query": {
	//       "description": "Query string search. Should be of the form \"\". Complete documentation is at https://developers.google.com/admin-sdk/directory/v1/guides/search-users",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "If set to true retrieves the list of deleted users. Default is false",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sortOrder": {
	//       "description": "Whether to return results in ascending or descending order.",
	//       "enum": [
	//         "ASCENDING",
	//         "DESCENDING"
	//       ],
	//       "enumDescriptions": [
	//         "Ascending order.",
	//         "Descending order."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "viewType": {
	//       "default": "admin_view",
	//       "description": "Whether to fetch the ADMIN_VIEW or DOMAIN_PUBLIC view of the user.",
	//       "enum": [
	//         "admin_view",
	//         "domain_public"
	//       ],
	//       "enumDescriptions": [
	//         "Fetches the ADMIN_VIEW of the user.",
	//         "Fetches the DOMAIN_PUBLIC view of the user."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users",
	//   "response": {
	//     "$ref": "Users"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UsersListCall) Pages(ctx context.Context, f func(*Users) error) error {
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

// method id "directory.users.makeAdmin":

type UsersMakeAdminCall struct {
	s             *Service
	userKey       string
	usermakeadmin *UserMakeAdmin
	urlParams_    gensupport.URLParams
	ctx_          context.Context
	header_       http.Header
}

// MakeAdmin: change admin status of a user
func (r *UsersService) MakeAdmin(userKey string, usermakeadmin *UserMakeAdmin) *UsersMakeAdminCall {
	c := &UsersMakeAdminCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.usermakeadmin = usermakeadmin
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMakeAdminCall) Fields(s ...googleapi.Field) *UsersMakeAdminCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMakeAdminCall) Context(ctx context.Context) *UsersMakeAdminCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMakeAdminCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMakeAdminCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.usermakeadmin)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/makeAdmin")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.makeAdmin" call.
func (c *UsersMakeAdminCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "change admin status of a user",
	//   "httpMethod": "POST",
	//   "id": "directory.users.makeAdmin",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user as admin",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/makeAdmin",
	//   "request": {
	//     "$ref": "UserMakeAdmin"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.patch":

type UsersPatchCall struct {
	s          *Service
	userKey    string
	user       *User
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: update user. This method supports patch semantics.
func (r *UsersService) Patch(userKey string, user *User) *UsersPatchCall {
	c := &UsersPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.user = user
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersPatchCall) Fields(s ...googleapi.Field) *UsersPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersPatchCall) Context(ctx context.Context) *UsersPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.user)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.patch" call.
// Exactly one of *User or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *User.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *UsersPatchCall) Do(opts ...googleapi.CallOption) (*User, error) {
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
	ret := &User{
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
	//   "description": "update user. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.users.patch",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user. If Id, it should match with id of user object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}",
	//   "request": {
	//     "$ref": "User"
	//   },
	//   "response": {
	//     "$ref": "User"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.undelete":

type UsersUndeleteCall struct {
	s            *Service
	userKey      string
	userundelete *UserUndelete
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Undelete: Undelete a deleted user
func (r *UsersService) Undelete(userKey string, userundelete *UserUndelete) *UsersUndeleteCall {
	c := &UsersUndeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.userundelete = userundelete
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersUndeleteCall) Fields(s ...googleapi.Field) *UsersUndeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersUndeleteCall) Context(ctx context.Context) *UsersUndeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersUndeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersUndeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.userundelete)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/undelete")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.undelete" call.
func (c *UsersUndeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Undelete a deleted user",
	//   "httpMethod": "POST",
	//   "id": "directory.users.undelete",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "The immutable id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/undelete",
	//   "request": {
	//     "$ref": "UserUndelete"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.update":

type UsersUpdateCall struct {
	s          *Service
	userKey    string
	user       *User
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: update user
func (r *UsersService) Update(userKey string, user *User) *UsersUpdateCall {
	c := &UsersUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.user = user
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersUpdateCall) Fields(s ...googleapi.Field) *UsersUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersUpdateCall) Context(ctx context.Context) *UsersUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.user)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.update" call.
// Exactly one of *User or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *User.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *UsersUpdateCall) Do(opts ...googleapi.CallOption) (*User, error) {
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
	ret := &User{
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
	//   "description": "update user",
	//   "httpMethod": "PUT",
	//   "id": "directory.users.update",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user. If Id, it should match with id of user object",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}",
	//   "request": {
	//     "$ref": "User"
	//   },
	//   "response": {
	//     "$ref": "User"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.watch":

type UsersWatchCall struct {
	s          *Service
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Watch: Watch for changes in users list
func (r *UsersService) Watch(channel *Channel) *UsersWatchCall {
	c := &UsersWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.channel = channel
	return c
}

// CustomFieldMask sets the optional parameter "customFieldMask":
// Comma-separated list of schema names. All fields from these schemas
// are fetched. This should only be set when projection=custom.
func (c *UsersWatchCall) CustomFieldMask(customFieldMask string) *UsersWatchCall {
	c.urlParams_.Set("customFieldMask", customFieldMask)
	return c
}

// Customer sets the optional parameter "customer": Immutable id of the
// Google Apps account. In case of multi-domain, to fetch all users for
// a customer, fill this field instead of domain.
func (c *UsersWatchCall) Customer(customer string) *UsersWatchCall {
	c.urlParams_.Set("customer", customer)
	return c
}

// Domain sets the optional parameter "domain": Name of the domain. Fill
// this field to get users from only this domain. To return all users in
// a multi-domain fill customer field instead.
func (c *UsersWatchCall) Domain(domain string) *UsersWatchCall {
	c.urlParams_.Set("domain", domain)
	return c
}

// Event sets the optional parameter "event": Event on which
// subscription is intended (if subscribing)
//
// Possible values:
//   "add" - User Created Event
//   "delete" - User Deleted Event
//   "makeAdmin" - User Admin Status Change Event
//   "undelete" - User Undeleted Event
//   "update" - User Updated Event
func (c *UsersWatchCall) Event(event string) *UsersWatchCall {
	c.urlParams_.Set("event", event)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return. Default is 100. Max allowed is 500
func (c *UsersWatchCall) MaxResults(maxResults int64) *UsersWatchCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": Column to use for
// sorting results
//
// Possible values:
//   "email" - Primary email of the user.
//   "familyName" - User's family name.
//   "givenName" - User's given name.
func (c *UsersWatchCall) OrderBy(orderBy string) *UsersWatchCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token to specify
// next page in the list
func (c *UsersWatchCall) PageToken(pageToken string) *UsersWatchCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Projection sets the optional parameter "projection": What subset of
// fields to fetch for this user.
//
// Possible values:
//   "basic" (default) - Do not include any custom fields for the user.
//   "custom" - Include custom fields from schemas mentioned in
// customFieldMask.
//   "full" - Include all fields associated with this user.
func (c *UsersWatchCall) Projection(projection string) *UsersWatchCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Query sets the optional parameter "query": Query string search.
// Should be of the form "". Complete documentation is at
// https://developers.google.com/admin-sdk/directory/v1/guides/search-users
func (c *UsersWatchCall) Query(query string) *UsersWatchCall {
	c.urlParams_.Set("query", query)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": If set to true
// retrieves the list of deleted users. Default is false
func (c *UsersWatchCall) ShowDeleted(showDeleted string) *UsersWatchCall {
	c.urlParams_.Set("showDeleted", showDeleted)
	return c
}

// SortOrder sets the optional parameter "sortOrder": Whether to return
// results in ascending or descending order.
//
// Possible values:
//   "ASCENDING" - Ascending order.
//   "DESCENDING" - Descending order.
func (c *UsersWatchCall) SortOrder(sortOrder string) *UsersWatchCall {
	c.urlParams_.Set("sortOrder", sortOrder)
	return c
}

// ViewType sets the optional parameter "viewType": Whether to fetch the
// ADMIN_VIEW or DOMAIN_PUBLIC view of the user.
//
// Possible values:
//   "admin_view" (default) - Fetches the ADMIN_VIEW of the user.
//   "domain_public" - Fetches the DOMAIN_PUBLIC view of the user.
func (c *UsersWatchCall) ViewType(viewType string) *UsersWatchCall {
	c.urlParams_.Set("viewType", viewType)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersWatchCall) Fields(s ...googleapi.Field) *UsersWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersWatchCall) Context(ctx context.Context) *UsersWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersWatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.channel)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.watch" call.
// Exactly one of *Channel or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Channel.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersWatchCall) Do(opts ...googleapi.CallOption) (*Channel, error) {
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
	ret := &Channel{
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
	//   "description": "Watch for changes in users list",
	//   "httpMethod": "POST",
	//   "id": "directory.users.watch",
	//   "parameters": {
	//     "customFieldMask": {
	//       "description": "Comma-separated list of schema names. All fields from these schemas are fetched. This should only be set when projection=custom.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "customer": {
	//       "description": "Immutable id of the Google Apps account. In case of multi-domain, to fetch all users for a customer, fill this field instead of domain.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "domain": {
	//       "description": "Name of the domain. Fill this field to get users from only this domain. To return all users in a multi-domain fill customer field instead.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "event": {
	//       "description": "Event on which subscription is intended (if subscribing)",
	//       "enum": [
	//         "add",
	//         "delete",
	//         "makeAdmin",
	//         "undelete",
	//         "update"
	//       ],
	//       "enumDescriptions": [
	//         "User Created Event",
	//         "User Deleted Event",
	//         "User Admin Status Change Event",
	//         "User Undeleted Event",
	//         "User Updated Event"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return. Default is 100. Max allowed is 500",
	//       "format": "int32",
	//       "location": "query",
	//       "maximum": "500",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "Column to use for sorting results",
	//       "enum": [
	//         "email",
	//         "familyName",
	//         "givenName"
	//       ],
	//       "enumDescriptions": [
	//         "Primary email of the user.",
	//         "User's family name.",
	//         "User's given name."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token to specify next page in the list",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "default": "basic",
	//       "description": "What subset of fields to fetch for this user.",
	//       "enum": [
	//         "basic",
	//         "custom",
	//         "full"
	//       ],
	//       "enumDescriptions": [
	//         "Do not include any custom fields for the user.",
	//         "Include custom fields from schemas mentioned in customFieldMask.",
	//         "Include all fields associated with this user."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "query": {
	//       "description": "Query string search. Should be of the form \"\". Complete documentation is at https://developers.google.com/admin-sdk/directory/v1/guides/search-users",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "If set to true retrieves the list of deleted users. Default is false",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sortOrder": {
	//       "description": "Whether to return results in ascending or descending order.",
	//       "enum": [
	//         "ASCENDING",
	//         "DESCENDING"
	//       ],
	//       "enumDescriptions": [
	//         "Ascending order.",
	//         "Descending order."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "viewType": {
	//       "default": "admin_view",
	//       "description": "Whether to fetch the ADMIN_VIEW or DOMAIN_PUBLIC view of the user.",
	//       "enum": [
	//         "admin_view",
	//         "domain_public"
	//       ],
	//       "enumDescriptions": [
	//         "Fetches the ADMIN_VIEW of the user.",
	//         "Fetches the DOMAIN_PUBLIC view of the user."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/watch",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "response": {
	//     "$ref": "Channel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "directory.users.aliases.delete":

type UsersAliasesDeleteCall struct {
	s          *Service
	userKey    string
	alias      string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Remove a alias for the user
func (r *UsersAliasesService) Delete(userKey string, alias string) *UsersAliasesDeleteCall {
	c := &UsersAliasesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.alias = alias
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersAliasesDeleteCall) Fields(s ...googleapi.Field) *UsersAliasesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersAliasesDeleteCall) Context(ctx context.Context) *UsersAliasesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersAliasesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersAliasesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/aliases/{alias}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
		"alias":   c.alias,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.aliases.delete" call.
func (c *UsersAliasesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove a alias for the user",
	//   "httpMethod": "DELETE",
	//   "id": "directory.users.aliases.delete",
	//   "parameterOrder": [
	//     "userKey",
	//     "alias"
	//   ],
	//   "parameters": {
	//     "alias": {
	//       "description": "The alias to be removed",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/aliases/{alias}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias"
	//   ]
	// }

}

// method id "directory.users.aliases.insert":

type UsersAliasesInsertCall struct {
	s          *Service
	userKey    string
	alias      *Alias
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Add a alias for the user
func (r *UsersAliasesService) Insert(userKey string, alias *Alias) *UsersAliasesInsertCall {
	c := &UsersAliasesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.alias = alias
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersAliasesInsertCall) Fields(s ...googleapi.Field) *UsersAliasesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersAliasesInsertCall) Context(ctx context.Context) *UsersAliasesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersAliasesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersAliasesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.alias)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/aliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.aliases.insert" call.
// Exactly one of *Alias or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Alias.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersAliasesInsertCall) Do(opts ...googleapi.CallOption) (*Alias, error) {
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
	ret := &Alias{
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
	//   "description": "Add a alias for the user",
	//   "httpMethod": "POST",
	//   "id": "directory.users.aliases.insert",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/aliases",
	//   "request": {
	//     "$ref": "Alias"
	//   },
	//   "response": {
	//     "$ref": "Alias"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias"
	//   ]
	// }

}

// method id "directory.users.aliases.list":

type UsersAliasesListCall struct {
	s            *Service
	userKey      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List all aliases for a user
func (r *UsersAliasesService) List(userKey string) *UsersAliasesListCall {
	c := &UsersAliasesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Event sets the optional parameter "event": Event on which
// subscription is intended (if subscribing)
//
// Possible values:
//   "add" - Alias Created Event
//   "delete" - Alias Deleted Event
func (c *UsersAliasesListCall) Event(event string) *UsersAliasesListCall {
	c.urlParams_.Set("event", event)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersAliasesListCall) Fields(s ...googleapi.Field) *UsersAliasesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersAliasesListCall) IfNoneMatch(entityTag string) *UsersAliasesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersAliasesListCall) Context(ctx context.Context) *UsersAliasesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersAliasesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersAliasesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/aliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.aliases.list" call.
// Exactly one of *Aliases or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Aliases.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersAliasesListCall) Do(opts ...googleapi.CallOption) (*Aliases, error) {
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
	ret := &Aliases{
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
	//   "description": "List all aliases for a user",
	//   "httpMethod": "GET",
	//   "id": "directory.users.aliases.list",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "event": {
	//       "description": "Event on which subscription is intended (if subscribing)",
	//       "enum": [
	//         "add",
	//         "delete"
	//       ],
	//       "enumDescriptions": [
	//         "Alias Created Event",
	//         "Alias Deleted Event"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/aliases",
	//   "response": {
	//     "$ref": "Aliases"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias.readonly",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "directory.users.aliases.watch":

type UsersAliasesWatchCall struct {
	s          *Service
	userKey    string
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Watch: Watch for changes in user aliases list
func (r *UsersAliasesService) Watch(userKey string, channel *Channel) *UsersAliasesWatchCall {
	c := &UsersAliasesWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.channel = channel
	return c
}

// Event sets the optional parameter "event": Event on which
// subscription is intended (if subscribing)
//
// Possible values:
//   "add" - Alias Created Event
//   "delete" - Alias Deleted Event
func (c *UsersAliasesWatchCall) Event(event string) *UsersAliasesWatchCall {
	c.urlParams_.Set("event", event)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersAliasesWatchCall) Fields(s ...googleapi.Field) *UsersAliasesWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersAliasesWatchCall) Context(ctx context.Context) *UsersAliasesWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersAliasesWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersAliasesWatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.channel)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/aliases/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.aliases.watch" call.
// Exactly one of *Channel or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Channel.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersAliasesWatchCall) Do(opts ...googleapi.CallOption) (*Channel, error) {
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
	ret := &Channel{
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
	//   "description": "Watch for changes in user aliases list",
	//   "httpMethod": "POST",
	//   "id": "directory.users.aliases.watch",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "event": {
	//       "description": "Event on which subscription is intended (if subscribing)",
	//       "enum": [
	//         "add",
	//         "delete"
	//       ],
	//       "enumDescriptions": [
	//         "Alias Created Event",
	//         "Alias Deleted Event"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/aliases/watch",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "response": {
	//     "$ref": "Channel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias",
	//     "https://www.googleapis.com/auth/admin.directory.user.alias.readonly",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "directory.users.photos.delete":

type UsersPhotosDeleteCall struct {
	s          *Service
	userKey    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Remove photos for the user
func (r *UsersPhotosService) Delete(userKey string) *UsersPhotosDeleteCall {
	c := &UsersPhotosDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersPhotosDeleteCall) Fields(s ...googleapi.Field) *UsersPhotosDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersPhotosDeleteCall) Context(ctx context.Context) *UsersPhotosDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersPhotosDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersPhotosDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/photos/thumbnail")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.photos.delete" call.
func (c *UsersPhotosDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove photos for the user",
	//   "httpMethod": "DELETE",
	//   "id": "directory.users.photos.delete",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/photos/thumbnail",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.photos.get":

type UsersPhotosGetCall struct {
	s            *Service
	userKey      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieve photo of a user
func (r *UsersPhotosService) Get(userKey string) *UsersPhotosGetCall {
	c := &UsersPhotosGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersPhotosGetCall) Fields(s ...googleapi.Field) *UsersPhotosGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersPhotosGetCall) IfNoneMatch(entityTag string) *UsersPhotosGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersPhotosGetCall) Context(ctx context.Context) *UsersPhotosGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersPhotosGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersPhotosGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/photos/thumbnail")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.photos.get" call.
// Exactly one of *UserPhoto or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *UserPhoto.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersPhotosGetCall) Do(opts ...googleapi.CallOption) (*UserPhoto, error) {
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
	ret := &UserPhoto{
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
	//   "description": "Retrieve photo of a user",
	//   "httpMethod": "GET",
	//   "id": "directory.users.photos.get",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/photos/thumbnail",
	//   "response": {
	//     "$ref": "UserPhoto"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user",
	//     "https://www.googleapis.com/auth/admin.directory.user.readonly"
	//   ]
	// }

}

// method id "directory.users.photos.patch":

type UsersPhotosPatchCall struct {
	s          *Service
	userKey    string
	userphoto  *UserPhoto
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Add a photo for the user. This method supports patch
// semantics.
func (r *UsersPhotosService) Patch(userKey string, userphoto *UserPhoto) *UsersPhotosPatchCall {
	c := &UsersPhotosPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.userphoto = userphoto
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersPhotosPatchCall) Fields(s ...googleapi.Field) *UsersPhotosPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersPhotosPatchCall) Context(ctx context.Context) *UsersPhotosPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersPhotosPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersPhotosPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.userphoto)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/photos/thumbnail")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.photos.patch" call.
// Exactly one of *UserPhoto or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *UserPhoto.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersPhotosPatchCall) Do(opts ...googleapi.CallOption) (*UserPhoto, error) {
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
	ret := &UserPhoto{
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
	//   "description": "Add a photo for the user. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "directory.users.photos.patch",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/photos/thumbnail",
	//   "request": {
	//     "$ref": "UserPhoto"
	//   },
	//   "response": {
	//     "$ref": "UserPhoto"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.users.photos.update":

type UsersPhotosUpdateCall struct {
	s          *Service
	userKey    string
	userphoto  *UserPhoto
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Add a photo for the user
func (r *UsersPhotosService) Update(userKey string, userphoto *UserPhoto) *UsersPhotosUpdateCall {
	c := &UsersPhotosUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	c.userphoto = userphoto
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersPhotosUpdateCall) Fields(s ...googleapi.Field) *UsersPhotosUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersPhotosUpdateCall) Context(ctx context.Context) *UsersPhotosUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersPhotosUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersPhotosUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.userphoto)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/photos/thumbnail")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.users.photos.update" call.
// Exactly one of *UserPhoto or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *UserPhoto.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersPhotosUpdateCall) Do(opts ...googleapi.CallOption) (*UserPhoto, error) {
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
	ret := &UserPhoto{
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
	//   "description": "Add a photo for the user",
	//   "httpMethod": "PUT",
	//   "id": "directory.users.photos.update",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/photos/thumbnail",
	//   "request": {
	//     "$ref": "UserPhoto"
	//   },
	//   "response": {
	//     "$ref": "UserPhoto"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user"
	//   ]
	// }

}

// method id "directory.verificationCodes.generate":

type VerificationCodesGenerateCall struct {
	s          *Service
	userKey    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Generate: Generate new backup verification codes for the user.
func (r *VerificationCodesService) Generate(userKey string) *VerificationCodesGenerateCall {
	c := &VerificationCodesGenerateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VerificationCodesGenerateCall) Fields(s ...googleapi.Field) *VerificationCodesGenerateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VerificationCodesGenerateCall) Context(ctx context.Context) *VerificationCodesGenerateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VerificationCodesGenerateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VerificationCodesGenerateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/verificationCodes/generate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.verificationCodes.generate" call.
func (c *VerificationCodesGenerateCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Generate new backup verification codes for the user.",
	//   "httpMethod": "POST",
	//   "id": "directory.verificationCodes.generate",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/verificationCodes/generate",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.verificationCodes.invalidate":

type VerificationCodesInvalidateCall struct {
	s          *Service
	userKey    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Invalidate: Invalidate the current backup verification codes for the
// user.
func (r *VerificationCodesService) Invalidate(userKey string) *VerificationCodesInvalidateCall {
	c := &VerificationCodesInvalidateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VerificationCodesInvalidateCall) Fields(s ...googleapi.Field) *VerificationCodesInvalidateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VerificationCodesInvalidateCall) Context(ctx context.Context) *VerificationCodesInvalidateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VerificationCodesInvalidateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VerificationCodesInvalidateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/verificationCodes/invalidate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.verificationCodes.invalidate" call.
func (c *VerificationCodesInvalidateCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Invalidate the current backup verification codes for the user.",
	//   "httpMethod": "POST",
	//   "id": "directory.verificationCodes.invalidate",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Email or immutable Id of the user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/verificationCodes/invalidate",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

// method id "directory.verificationCodes.list":

type VerificationCodesListCall struct {
	s            *Service
	userKey      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns the current set of valid backup verification codes for
// the specified user.
func (r *VerificationCodesService) List(userKey string) *VerificationCodesListCall {
	c := &VerificationCodesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userKey = userKey
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VerificationCodesListCall) Fields(s ...googleapi.Field) *VerificationCodesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VerificationCodesListCall) IfNoneMatch(entityTag string) *VerificationCodesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VerificationCodesListCall) Context(ctx context.Context) *VerificationCodesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VerificationCodesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VerificationCodesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userKey}/verificationCodes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userKey": c.userKey,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "directory.verificationCodes.list" call.
// Exactly one of *VerificationCodes or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *VerificationCodes.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *VerificationCodesListCall) Do(opts ...googleapi.CallOption) (*VerificationCodes, error) {
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
	ret := &VerificationCodes{
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
	//   "description": "Returns the current set of valid backup verification codes for the specified user.",
	//   "httpMethod": "GET",
	//   "id": "directory.verificationCodes.list",
	//   "parameterOrder": [
	//     "userKey"
	//   ],
	//   "parameters": {
	//     "userKey": {
	//       "description": "Identifies the user in the API request. The value can be the user's primary email address, alias email address, or unique user ID.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userKey}/verificationCodes",
	//   "response": {
	//     "$ref": "VerificationCodes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/admin.directory.user.security"
	//   ]
	// }

}

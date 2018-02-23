// Package calendar provides access to the Calendar API.
//
// See https://developers.google.com/google-apps/calendar/firstapp
//
// Usage example:
//
//   import "google.golang.org/api/calendar/v3"
//   ...
//   calendarService, err := calendar.New(oauthHttpClient)
package calendar // import "google.golang.org/api/calendar/v3"

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

const apiId = "calendar:v3"
const apiName = "calendar"
const apiVersion = "v3"
const basePath = "https://www.googleapis.com/calendar/v3/"

// OAuth2 scopes used by this API.
const (
	// Manage your calendars
	CalendarScope = "https://www.googleapis.com/auth/calendar"

	// View your calendars
	CalendarReadonlyScope = "https://www.googleapis.com/auth/calendar.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Acl = NewAclService(s)
	s.CalendarList = NewCalendarListService(s)
	s.Calendars = NewCalendarsService(s)
	s.Channels = NewChannelsService(s)
	s.Colors = NewColorsService(s)
	s.Events = NewEventsService(s)
	s.Freebusy = NewFreebusyService(s)
	s.Settings = NewSettingsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Acl *AclService

	CalendarList *CalendarListService

	Calendars *CalendarsService

	Channels *ChannelsService

	Colors *ColorsService

	Events *EventsService

	Freebusy *FreebusyService

	Settings *SettingsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewAclService(s *Service) *AclService {
	rs := &AclService{s: s}
	return rs
}

type AclService struct {
	s *Service
}

func NewCalendarListService(s *Service) *CalendarListService {
	rs := &CalendarListService{s: s}
	return rs
}

type CalendarListService struct {
	s *Service
}

func NewCalendarsService(s *Service) *CalendarsService {
	rs := &CalendarsService{s: s}
	return rs
}

type CalendarsService struct {
	s *Service
}

func NewChannelsService(s *Service) *ChannelsService {
	rs := &ChannelsService{s: s}
	return rs
}

type ChannelsService struct {
	s *Service
}

func NewColorsService(s *Service) *ColorsService {
	rs := &ColorsService{s: s}
	return rs
}

type ColorsService struct {
	s *Service
}

func NewEventsService(s *Service) *EventsService {
	rs := &EventsService{s: s}
	return rs
}

type EventsService struct {
	s *Service
}

func NewFreebusyService(s *Service) *FreebusyService {
	rs := &FreebusyService{s: s}
	return rs
}

type FreebusyService struct {
	s *Service
}

func NewSettingsService(s *Service) *SettingsService {
	rs := &SettingsService{s: s}
	return rs
}

type SettingsService struct {
	s *Service
}

type Acl struct {
	// Etag: ETag of the collection.
	Etag string `json:"etag,omitempty"`

	// Items: List of rules on the access control list.
	Items []*AclRule `json:"items,omitempty"`

	// Kind: Type of the collection ("calendar#acl").
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result.
	// Omitted if no further results are available, in which case
	// nextSyncToken is provided.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// NextSyncToken: Token used at a later point in time to retrieve only
	// the entries that have changed since this result was returned. Omitted
	// if further results are available, in which case nextPageToken is
	// provided.
	NextSyncToken string `json:"nextSyncToken,omitempty"`

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

func (s *Acl) MarshalJSON() ([]byte, error) {
	type noMethod Acl
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AclRule struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: Identifier of the ACL rule.
	Id string `json:"id,omitempty"`

	// Kind: Type of the resource ("calendar#aclRule").
	Kind string `json:"kind,omitempty"`

	// Role: The role assigned to the scope. Possible values are:
	// - "none" - Provides no access.
	// - "freeBusyReader" - Provides read access to free/busy information.
	//
	// - "reader" - Provides read access to the calendar. Private events
	// will appear to users with reader access, but event details will be
	// hidden.
	// - "writer" - Provides read and write access to the calendar. Private
	// events will appear to users with writer access, and event details
	// will be visible.
	// - "owner" - Provides ownership of the calendar. This role has all of
	// the permissions of the writer role with the additional ability to see
	// and manipulate ACLs.
	Role string `json:"role,omitempty"`

	// Scope: The scope of the rule.
	Scope *AclRuleScope `json:"scope,omitempty"`

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

func (s *AclRule) MarshalJSON() ([]byte, error) {
	type noMethod AclRule
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AclRuleScope: The scope of the rule.
type AclRuleScope struct {
	// Type: The type of the scope. Possible values are:
	// - "default" - The public scope. This is the default value.
	// - "user" - Limits the scope to a single user.
	// - "group" - Limits the scope to a group.
	// - "domain" - Limits the scope to a domain.  Note: The permissions
	// granted to the "default", or public, scope apply to any user,
	// authenticated or not.
	Type string `json:"type,omitempty"`

	// Value: The email address of a user or group, or the name of a domain,
	// depending on the scope type. Omitted for type "default".
	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Type") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Type") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AclRuleScope) MarshalJSON() ([]byte, error) {
	type noMethod AclRuleScope
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Calendar struct {
	// Description: Description of the calendar. Optional.
	Description string `json:"description,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: Identifier of the calendar. To retrieve IDs call the
	// calendarList.list() method.
	Id string `json:"id,omitempty"`

	// Kind: Type of the resource ("calendar#calendar").
	Kind string `json:"kind,omitempty"`

	// Location: Geographic location of the calendar as free-form text.
	// Optional.
	Location string `json:"location,omitempty"`

	// Summary: Title of the calendar.
	Summary string `json:"summary,omitempty"`

	// TimeZone: The time zone of the calendar. (Formatted as an IANA Time
	// Zone Database name, e.g. "Europe/Zurich".) Optional.
	TimeZone string `json:"timeZone,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *Calendar) MarshalJSON() ([]byte, error) {
	type noMethod Calendar
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CalendarList struct {
	// Etag: ETag of the collection.
	Etag string `json:"etag,omitempty"`

	// Items: Calendars that are present on the user's calendar list.
	Items []*CalendarListEntry `json:"items,omitempty"`

	// Kind: Type of the collection ("calendar#calendarList").
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result.
	// Omitted if no further results are available, in which case
	// nextSyncToken is provided.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// NextSyncToken: Token used at a later point in time to retrieve only
	// the entries that have changed since this result was returned. Omitted
	// if further results are available, in which case nextPageToken is
	// provided.
	NextSyncToken string `json:"nextSyncToken,omitempty"`

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

func (s *CalendarList) MarshalJSON() ([]byte, error) {
	type noMethod CalendarList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CalendarListEntry struct {
	// AccessRole: The effective access role that the authenticated user has
	// on the calendar. Read-only. Possible values are:
	// - "freeBusyReader" - Provides read access to free/busy information.
	//
	// - "reader" - Provides read access to the calendar. Private events
	// will appear to users with reader access, but event details will be
	// hidden.
	// - "writer" - Provides read and write access to the calendar. Private
	// events will appear to users with writer access, and event details
	// will be visible.
	// - "owner" - Provides ownership of the calendar. This role has all of
	// the permissions of the writer role with the additional ability to see
	// and manipulate ACLs.
	AccessRole string `json:"accessRole,omitempty"`

	// BackgroundColor: The main color of the calendar in the hexadecimal
	// format "#0088aa". This property supersedes the index-based colorId
	// property. To set or change this property, you need to specify
	// colorRgbFormat=true in the parameters of the insert, update and patch
	// methods. Optional.
	BackgroundColor string `json:"backgroundColor,omitempty"`

	// ColorId: The color of the calendar. This is an ID referring to an
	// entry in the calendar section of the colors definition (see the
	// colors endpoint). This property is superseded by the backgroundColor
	// and foregroundColor properties and can be ignored when using these
	// properties. Optional.
	ColorId string `json:"colorId,omitempty"`

	// DefaultReminders: The default reminders that the authenticated user
	// has for this calendar.
	DefaultReminders []*EventReminder `json:"defaultReminders,omitempty"`

	// Deleted: Whether this calendar list entry has been deleted from the
	// calendar list. Read-only. Optional. The default is False.
	Deleted bool `json:"deleted,omitempty"`

	// Description: Description of the calendar. Optional. Read-only.
	Description string `json:"description,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// ForegroundColor: The foreground color of the calendar in the
	// hexadecimal format "#ffffff". This property supersedes the
	// index-based colorId property. To set or change this property, you
	// need to specify colorRgbFormat=true in the parameters of the insert,
	// update and patch methods. Optional.
	ForegroundColor string `json:"foregroundColor,omitempty"`

	// Hidden: Whether the calendar has been hidden from the list. Optional.
	// The default is False.
	Hidden bool `json:"hidden,omitempty"`

	// Id: Identifier of the calendar.
	Id string `json:"id,omitempty"`

	// Kind: Type of the resource ("calendar#calendarListEntry").
	Kind string `json:"kind,omitempty"`

	// Location: Geographic location of the calendar as free-form text.
	// Optional. Read-only.
	Location string `json:"location,omitempty"`

	// NotificationSettings: The notifications that the authenticated user
	// is receiving for this calendar.
	NotificationSettings *CalendarListEntryNotificationSettings `json:"notificationSettings,omitempty"`

	// Primary: Whether the calendar is the primary calendar of the
	// authenticated user. Read-only. Optional. The default is False.
	Primary bool `json:"primary,omitempty"`

	// Selected: Whether the calendar content shows up in the calendar UI.
	// Optional. The default is False.
	Selected bool `json:"selected,omitempty"`

	// Summary: Title of the calendar. Read-only.
	Summary string `json:"summary,omitempty"`

	// SummaryOverride: The summary that the authenticated user has set for
	// this calendar. Optional.
	SummaryOverride string `json:"summaryOverride,omitempty"`

	// TimeZone: The time zone of the calendar. Optional. Read-only.
	TimeZone string `json:"timeZone,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AccessRole") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccessRole") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CalendarListEntry) MarshalJSON() ([]byte, error) {
	type noMethod CalendarListEntry
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CalendarListEntryNotificationSettings: The notifications that the
// authenticated user is receiving for this calendar.
type CalendarListEntryNotificationSettings struct {
	// Notifications: The list of notifications set for this calendar.
	Notifications []*CalendarNotification `json:"notifications,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Notifications") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Notifications") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CalendarListEntryNotificationSettings) MarshalJSON() ([]byte, error) {
	type noMethod CalendarListEntryNotificationSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CalendarNotification struct {
	// Method: The method used to deliver the notification. Possible values
	// are:
	// - "email" - Reminders are sent via email.
	// - "sms" - Reminders are sent via SMS. This value is read-only and is
	// ignored on inserts and updates. SMS reminders are only available for
	// Google Apps for Work, Education, and Government customers.
	Method string `json:"method,omitempty"`

	// Type: The type of notification. Possible values are:
	// - "eventCreation" - Notification sent when a new event is put on the
	// calendar.
	// - "eventChange" - Notification sent when an event is changed.
	// - "eventCancellation" - Notification sent when an event is cancelled.
	//
	// - "eventResponse" - Notification sent when an event is changed.
	// - "agenda" - An agenda with the events of the day (sent out in the
	// morning).
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Method") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Method") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CalendarNotification) MarshalJSON() ([]byte, error) {
	type noMethod CalendarNotification
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

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

type ColorDefinition struct {
	// Background: The background color associated with this color
	// definition.
	Background string `json:"background,omitempty"`

	// Foreground: The foreground color that can be used to write on top of
	// a background with 'background' color.
	Foreground string `json:"foreground,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Background") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Background") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ColorDefinition) MarshalJSON() ([]byte, error) {
	type noMethod ColorDefinition
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Colors struct {
	// Calendar: A global palette of calendar colors, mapping from the color
	// ID to its definition. A calendarListEntry resource refers to one of
	// these color IDs in its color field. Read-only.
	Calendar map[string]ColorDefinition `json:"calendar,omitempty"`

	// Event: A global palette of event colors, mapping from the color ID to
	// its definition. An event resource may refer to one of these color IDs
	// in its color field. Read-only.
	Event map[string]ColorDefinition `json:"event,omitempty"`

	// Kind: Type of the resource ("calendar#colors").
	Kind string `json:"kind,omitempty"`

	// Updated: Last modification time of the color palette (as a RFC3339
	// timestamp). Read-only.
	Updated string `json:"updated,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Calendar") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Calendar") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Colors) MarshalJSON() ([]byte, error) {
	type noMethod Colors
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Error struct {
	// Domain: Domain, or broad category, of the error.
	Domain string `json:"domain,omitempty"`

	// Reason: Specific reason for the error. Some of the possible values
	// are:
	// - "groupTooBig" - The group of users requested is too large for a
	// single query.
	// - "tooManyCalendarsRequested" - The number of calendars requested is
	// too large for a single query.
	// - "notFound" - The requested resource was not found.
	// - "internalError" - The API service has encountered an internal
	// error.  Additional error types may be added in the future, so clients
	// should gracefully handle additional error statuses not included in
	// this list.
	Reason string `json:"reason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Domain") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Domain") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Error) MarshalJSON() ([]byte, error) {
	type noMethod Error
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Event struct {
	// AnyoneCanAddSelf: Whether anyone can invite themselves to the event
	// (currently works for Google+ events only). Optional. The default is
	// False.
	AnyoneCanAddSelf bool `json:"anyoneCanAddSelf,omitempty"`

	// Attachments: File attachments for the event. Currently only Google
	// Drive attachments are supported.
	// In order to modify attachments the supportsAttachments request
	// parameter should be set to true.
	// There can be at most 25 attachments per event,
	Attachments []*EventAttachment `json:"attachments,omitempty"`

	// Attendees: The attendees of the event. See the Events with attendees
	// guide for more information on scheduling events with other calendar
	// users.
	Attendees []*EventAttendee `json:"attendees,omitempty"`

	// AttendeesOmitted: Whether attendees may have been omitted from the
	// event's representation. When retrieving an event, this may be due to
	// a restriction specified by the maxAttendee query parameter. When
	// updating an event, this can be used to only update the participant's
	// response. Optional. The default is False.
	AttendeesOmitted bool `json:"attendeesOmitted,omitempty"`

	// ColorId: The color of the event. This is an ID referring to an entry
	// in the event section of the colors definition (see the  colors
	// endpoint). Optional.
	ColorId string `json:"colorId,omitempty"`

	// Created: Creation time of the event (as a RFC3339 timestamp).
	// Read-only.
	Created string `json:"created,omitempty"`

	// Creator: The creator of the event. Read-only.
	Creator *EventCreator `json:"creator,omitempty"`

	// Description: Description of the event. Optional.
	Description string `json:"description,omitempty"`

	// End: The (exclusive) end time of the event. For a recurring event,
	// this is the end time of the first instance.
	End *EventDateTime `json:"end,omitempty"`

	// EndTimeUnspecified: Whether the end time is actually unspecified. An
	// end time is still provided for compatibility reasons, even if this
	// attribute is set to True. The default is False.
	EndTimeUnspecified bool `json:"endTimeUnspecified,omitempty"`

	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// ExtendedProperties: Extended properties of the event.
	ExtendedProperties *EventExtendedProperties `json:"extendedProperties,omitempty"`

	// Gadget: A gadget that extends this event.
	Gadget *EventGadget `json:"gadget,omitempty"`

	// GuestsCanInviteOthers: Whether attendees other than the organizer can
	// invite others to the event. Optional. The default is True.
	//
	// Default: true
	GuestsCanInviteOthers *bool `json:"guestsCanInviteOthers,omitempty"`

	// GuestsCanModify: Whether attendees other than the organizer can
	// modify the event. Optional. The default is False.
	GuestsCanModify bool `json:"guestsCanModify,omitempty"`

	// GuestsCanSeeOtherGuests: Whether attendees other than the organizer
	// can see who the event's attendees are. Optional. The default is True.
	//
	// Default: true
	GuestsCanSeeOtherGuests *bool `json:"guestsCanSeeOtherGuests,omitempty"`

	// HangoutLink: An absolute link to the Google+ hangout associated with
	// this event. Read-only.
	HangoutLink string `json:"hangoutLink,omitempty"`

	// HtmlLink: An absolute link to this event in the Google Calendar Web
	// UI. Read-only.
	HtmlLink string `json:"htmlLink,omitempty"`

	// ICalUID: Event unique identifier as defined in RFC5545. It is used to
	// uniquely identify events accross calendaring systems and must be
	// supplied when importing events via the import method.
	// Note that the icalUID and the id are not identical and only one of
	// them should be supplied at event creation time. One difference in
	// their semantics is that in recurring events, all occurrences of one
	// event have different ids while they all share the same icalUIDs.
	ICalUID string `json:"iCalUID,omitempty"`

	// Id: Opaque identifier of the event. When creating new single or
	// recurring events, you can specify their IDs. Provided IDs must follow
	// these rules:
	// - characters allowed in the ID are those used in base32hex encoding,
	// i.e. lowercase letters a-v and digits 0-9, see section 3.1.2 in
	// RFC2938
	// - the length of the ID must be between 5 and 1024 characters
	// - the ID must be unique per calendar  Due to the globally distributed
	// nature of the system, we cannot guarantee that ID collisions will be
	// detected at event creation time. To minimize the risk of collisions
	// we recommend using an established UUID algorithm such as one
	// described in RFC4122.
	// If you do not specify an ID, it will be automatically generated by
	// the server.
	// Note that the icalUID and the id are not identical and only one of
	// them should be supplied at event creation time. One difference in
	// their semantics is that in recurring events, all occurrences of one
	// event have different ids while they all share the same icalUIDs.
	Id string `json:"id,omitempty"`

	// Kind: Type of the resource ("calendar#event").
	Kind string `json:"kind,omitempty"`

	// Location: Geographic location of the event as free-form text.
	// Optional.
	Location string `json:"location,omitempty"`

	// Locked: Whether this is a locked event copy where no changes can be
	// made to the main event fields "summary", "description", "location",
	// "start", "end" or "recurrence". The default is False. Read-Only.
	Locked bool `json:"locked,omitempty"`

	// Organizer: The organizer of the event. If the organizer is also an
	// attendee, this is indicated with a separate entry in attendees with
	// the organizer field set to True. To change the organizer, use the
	// move operation. Read-only, except when importing an event.
	Organizer *EventOrganizer `json:"organizer,omitempty"`

	// OriginalStartTime: For an instance of a recurring event, this is the
	// time at which this event would start according to the recurrence data
	// in the recurring event identified by recurringEventId. Immutable.
	OriginalStartTime *EventDateTime `json:"originalStartTime,omitempty"`

	// PrivateCopy: Whether this is a private event copy where changes are
	// not shared with other copies on other calendars. Optional. Immutable.
	// The default is False.
	PrivateCopy bool `json:"privateCopy,omitempty"`

	// Recurrence: List of RRULE, EXRULE, RDATE and EXDATE lines for a
	// recurring event, as specified in RFC5545. Note that DTSTART and DTEND
	// lines are not allowed in this field; event start and end times are
	// specified in the start and end fields. This field is omitted for
	// single events or instances of recurring events.
	Recurrence []string `json:"recurrence,omitempty"`

	// RecurringEventId: For an instance of a recurring event, this is the
	// id of the recurring event to which this instance belongs. Immutable.
	RecurringEventId string `json:"recurringEventId,omitempty"`

	// Reminders: Information about the event's reminders for the
	// authenticated user.
	Reminders *EventReminders `json:"reminders,omitempty"`

	// Sequence: Sequence number as per iCalendar.
	Sequence int64 `json:"sequence,omitempty"`

	// Source: Source from which the event was created. For example, a web
	// page, an email message or any document identifiable by an URL with
	// HTTP or HTTPS scheme. Can only be seen or modified by the creator of
	// the event.
	Source *EventSource `json:"source,omitempty"`

	// Start: The (inclusive) start time of the event. For a recurring
	// event, this is the start time of the first instance.
	Start *EventDateTime `json:"start,omitempty"`

	// Status: Status of the event. Optional. Possible values are:
	// - "confirmed" - The event is confirmed. This is the default status.
	//
	// - "tentative" - The event is tentatively confirmed.
	// - "cancelled" - The event is cancelled.
	Status string `json:"status,omitempty"`

	// Summary: Title of the event.
	Summary string `json:"summary,omitempty"`

	// Transparency: Whether the event blocks time on the calendar.
	// Optional. Possible values are:
	// - "opaque" - The event blocks time on the calendar. This is the
	// default value.
	// - "transparent" - The event does not block time on the calendar.
	Transparency string `json:"transparency,omitempty"`

	// Updated: Last modification time of the event (as a RFC3339
	// timestamp). Read-only.
	Updated string `json:"updated,omitempty"`

	// Visibility: Visibility of the event. Optional. Possible values are:
	//
	// - "default" - Uses the default visibility for events on the calendar.
	// This is the default value.
	// - "public" - The event is public and event details are visible to all
	// readers of the calendar.
	// - "private" - The event is private and only event attendees may view
	// event details.
	// - "confidential" - The event is private. This value is provided for
	// compatibility reasons.
	Visibility string `json:"visibility,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AnyoneCanAddSelf") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AnyoneCanAddSelf") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Event) MarshalJSON() ([]byte, error) {
	type noMethod Event
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventCreator: The creator of the event. Read-only.
type EventCreator struct {
	// DisplayName: The creator's name, if available.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The creator's email address, if available.
	Email string `json:"email,omitempty"`

	// Id: The creator's Profile ID, if available. It corresponds to theid
	// field in the People collection of the Google+ API
	Id string `json:"id,omitempty"`

	// Self: Whether the creator corresponds to the calendar on which this
	// copy of the event appears. Read-only. The default is False.
	Self bool `json:"self,omitempty"`

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

func (s *EventCreator) MarshalJSON() ([]byte, error) {
	type noMethod EventCreator
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventExtendedProperties: Extended properties of the event.
type EventExtendedProperties struct {
	// Private: Properties that are private to the copy of the event that
	// appears on this calendar.
	Private map[string]string `json:"private,omitempty"`

	// Shared: Properties that are shared between copies of the event on
	// other attendees' calendars.
	Shared map[string]string `json:"shared,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Private") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Private") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventExtendedProperties) MarshalJSON() ([]byte, error) {
	type noMethod EventExtendedProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventGadget: A gadget that extends this event.
type EventGadget struct {
	// Display: The gadget's display mode. Optional. Possible values are:
	//
	// - "icon" - The gadget displays next to the event's title in the
	// calendar view.
	// - "chip" - The gadget displays when the event is clicked.
	Display string `json:"display,omitempty"`

	// Height: The gadget's height in pixels. The height must be an integer
	// greater than 0. Optional.
	Height int64 `json:"height,omitempty"`

	// IconLink: The gadget's icon URL. The URL scheme must be HTTPS.
	IconLink string `json:"iconLink,omitempty"`

	// Link: The gadget's URL. The URL scheme must be HTTPS.
	Link string `json:"link,omitempty"`

	// Preferences: Preferences.
	Preferences map[string]string `json:"preferences,omitempty"`

	// Title: The gadget's title.
	Title string `json:"title,omitempty"`

	// Type: The gadget's type.
	Type string `json:"type,omitempty"`

	// Width: The gadget's width in pixels. The width must be an integer
	// greater than 0. Optional.
	Width int64 `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Display") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Display") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventGadget) MarshalJSON() ([]byte, error) {
	type noMethod EventGadget
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventOrganizer: The organizer of the event. If the organizer is also
// an attendee, this is indicated with a separate entry in attendees
// with the organizer field set to True. To change the organizer, use
// the move operation. Read-only, except when importing an event.
type EventOrganizer struct {
	// DisplayName: The organizer's name, if available.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The organizer's email address, if available. It must be a
	// valid email address as per RFC5322.
	Email string `json:"email,omitempty"`

	// Id: The organizer's Profile ID, if available. It corresponds to theid
	// field in the People collection of the Google+ API
	Id string `json:"id,omitempty"`

	// Self: Whether the organizer corresponds to the calendar on which this
	// copy of the event appears. Read-only. The default is False.
	Self bool `json:"self,omitempty"`

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

func (s *EventOrganizer) MarshalJSON() ([]byte, error) {
	type noMethod EventOrganizer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventReminders: Information about the event's reminders for the
// authenticated user.
type EventReminders struct {
	// Overrides: If the event doesn't use the default reminders, this lists
	// the reminders specific to the event, or, if not set, indicates that
	// no reminders are set for this event. The maximum number of override
	// reminders is 5.
	Overrides []*EventReminder `json:"overrides,omitempty"`

	// UseDefault: Whether the default reminders of the calendar apply to
	// the event.
	UseDefault bool `json:"useDefault,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Overrides") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Overrides") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventReminders) MarshalJSON() ([]byte, error) {
	type noMethod EventReminders
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EventSource: Source from which the event was created. For example, a
// web page, an email message or any document identifiable by an URL
// with HTTP or HTTPS scheme. Can only be seen or modified by the
// creator of the event.
type EventSource struct {
	// Title: Title of the source; for example a title of a web page or an
	// email subject.
	Title string `json:"title,omitempty"`

	// Url: URL of the source pointing to a resource. The URL scheme must be
	// HTTP or HTTPS.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Title") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Title") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventSource) MarshalJSON() ([]byte, error) {
	type noMethod EventSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type EventAttachment struct {
	// FileId: ID of the attached file. Read-only.
	// For Google Drive files, this is the ID of the corresponding Files
	// resource entry in the Drive API.
	FileId string `json:"fileId,omitempty"`

	// FileUrl: URL link to the attachment.
	// For adding Google Drive file attachments use the same format as in
	// alternateLink property of the Files resource in the Drive API.
	FileUrl string `json:"fileUrl,omitempty"`

	// IconLink: URL link to the attachment's icon. Read-only.
	IconLink string `json:"iconLink,omitempty"`

	// MimeType: Internet media type (MIME type) of the attachment.
	MimeType string `json:"mimeType,omitempty"`

	// Title: Attachment title.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FileId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FileId") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventAttachment) MarshalJSON() ([]byte, error) {
	type noMethod EventAttachment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type EventAttendee struct {
	// AdditionalGuests: Number of additional guests. Optional. The default
	// is 0.
	AdditionalGuests int64 `json:"additionalGuests,omitempty"`

	// Comment: The attendee's response comment. Optional.
	Comment string `json:"comment,omitempty"`

	// DisplayName: The attendee's name, if available. Optional.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The attendee's email address, if available. This field must be
	// present when adding an attendee. It must be a valid email address as
	// per RFC5322.
	Email string `json:"email,omitempty"`

	// Id: The attendee's Profile ID, if available. It corresponds to theid
	// field in the People collection of the Google+ API
	Id string `json:"id,omitempty"`

	// Optional: Whether this is an optional attendee. Optional. The default
	// is False.
	Optional bool `json:"optional,omitempty"`

	// Organizer: Whether the attendee is the organizer of the event.
	// Read-only. The default is False.
	Organizer bool `json:"organizer,omitempty"`

	// Resource: Whether the attendee is a resource. Read-only. The default
	// is False.
	Resource bool `json:"resource,omitempty"`

	// ResponseStatus: The attendee's response status. Possible values are:
	//
	// - "needsAction" - The attendee has not responded to the invitation.
	//
	// - "declined" - The attendee has declined the invitation.
	// - "tentative" - The attendee has tentatively accepted the invitation.
	//
	// - "accepted" - The attendee has accepted the invitation.
	ResponseStatus string `json:"responseStatus,omitempty"`

	// Self: Whether this entry represents the calendar on which this copy
	// of the event appears. Read-only. The default is False.
	Self bool `json:"self,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AdditionalGuests") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AdditionalGuests") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *EventAttendee) MarshalJSON() ([]byte, error) {
	type noMethod EventAttendee
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type EventDateTime struct {
	// Date: The date, in the format "yyyy-mm-dd", if this is an all-day
	// event.
	Date string `json:"date,omitempty"`

	// DateTime: The time, as a combined date-time value (formatted
	// according to RFC3339). A time zone offset is required unless a time
	// zone is explicitly specified in timeZone.
	DateTime string `json:"dateTime,omitempty"`

	// TimeZone: The time zone in which the time is specified. (Formatted as
	// an IANA Time Zone Database name, e.g. "Europe/Zurich".) For recurring
	// events this field is required and specifies the time zone in which
	// the recurrence is expanded. For single events this field is optional
	// and indicates a custom time zone for the event start/end.
	TimeZone string `json:"timeZone,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Date") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Date") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventDateTime) MarshalJSON() ([]byte, error) {
	type noMethod EventDateTime
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type EventReminder struct {
	// Method: The method used by this reminder. Possible values are:
	// - "email" - Reminders are sent via email.
	// - "sms" - Reminders are sent via SMS. These are only available for
	// Google Apps for Work, Education, and Government customers. Requests
	// to set SMS reminders for other account types are ignored.
	// - "popup" - Reminders are sent via a UI popup.
	Method string `json:"method,omitempty"`

	// Minutes: Number of minutes before the start of the event when the
	// reminder should trigger. Valid values are between 0 and 40320 (4
	// weeks in minutes).
	Minutes int64 `json:"minutes,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Method") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Method") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EventReminder) MarshalJSON() ([]byte, error) {
	type noMethod EventReminder
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Events struct {
	// AccessRole: The user's access role for this calendar. Read-only.
	// Possible values are:
	// - "none" - The user has no access.
	// - "freeBusyReader" - The user has read access to free/busy
	// information.
	// - "reader" - The user has read access to the calendar. Private events
	// will appear to users with reader access, but event details will be
	// hidden.
	// - "writer" - The user has read and write access to the calendar.
	// Private events will appear to users with writer access, and event
	// details will be visible.
	// - "owner" - The user has ownership of the calendar. This role has all
	// of the permissions of the writer role with the additional ability to
	// see and manipulate ACLs.
	AccessRole string `json:"accessRole,omitempty"`

	// DefaultReminders: The default reminders on the calendar for the
	// authenticated user. These reminders apply to all events on this
	// calendar that do not explicitly override them (i.e. do not have
	// reminders.useDefault set to True).
	DefaultReminders []*EventReminder `json:"defaultReminders,omitempty"`

	// Description: Description of the calendar. Read-only.
	Description string `json:"description,omitempty"`

	// Etag: ETag of the collection.
	Etag string `json:"etag,omitempty"`

	// Items: List of events on the calendar.
	Items []*Event `json:"items,omitempty"`

	// Kind: Type of the collection ("calendar#events").
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result.
	// Omitted if no further results are available, in which case
	// nextSyncToken is provided.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// NextSyncToken: Token used at a later point in time to retrieve only
	// the entries that have changed since this result was returned. Omitted
	// if further results are available, in which case nextPageToken is
	// provided.
	NextSyncToken string `json:"nextSyncToken,omitempty"`

	// Summary: Title of the calendar. Read-only.
	Summary string `json:"summary,omitempty"`

	// TimeZone: The time zone of the calendar. Read-only.
	TimeZone string `json:"timeZone,omitempty"`

	// Updated: Last modification time of the calendar (as a RFC3339
	// timestamp). Read-only.
	Updated string `json:"updated,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AccessRole") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccessRole") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Events) MarshalJSON() ([]byte, error) {
	type noMethod Events
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type FreeBusyCalendar struct {
	// Busy: List of time ranges during which this calendar should be
	// regarded as busy.
	Busy []*TimePeriod `json:"busy,omitempty"`

	// Errors: Optional error(s) (if computation for the calendar failed).
	Errors []*Error `json:"errors,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Busy") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Busy") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FreeBusyCalendar) MarshalJSON() ([]byte, error) {
	type noMethod FreeBusyCalendar
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type FreeBusyGroup struct {
	// Calendars: List of calendars' identifiers within a group.
	Calendars []string `json:"calendars,omitempty"`

	// Errors: Optional error(s) (if computation for the group failed).
	Errors []*Error `json:"errors,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Calendars") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Calendars") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FreeBusyGroup) MarshalJSON() ([]byte, error) {
	type noMethod FreeBusyGroup
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type FreeBusyRequest struct {
	// CalendarExpansionMax: Maximal number of calendars for which FreeBusy
	// information is to be provided. Optional.
	CalendarExpansionMax int64 `json:"calendarExpansionMax,omitempty"`

	// GroupExpansionMax: Maximal number of calendar identifiers to be
	// provided for a single group. Optional. An error will be returned for
	// a group with more members than this value.
	GroupExpansionMax int64 `json:"groupExpansionMax,omitempty"`

	// Items: List of calendars and/or groups to query.
	Items []*FreeBusyRequestItem `json:"items,omitempty"`

	// TimeMax: The end of the interval for the query.
	TimeMax string `json:"timeMax,omitempty"`

	// TimeMin: The start of the interval for the query.
	TimeMin string `json:"timeMin,omitempty"`

	// TimeZone: Time zone used in the response. Optional. The default is
	// UTC.
	TimeZone string `json:"timeZone,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "CalendarExpansionMax") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CalendarExpansionMax") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *FreeBusyRequest) MarshalJSON() ([]byte, error) {
	type noMethod FreeBusyRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type FreeBusyRequestItem struct {
	// Id: The identifier of a calendar or a group.
	Id string `json:"id,omitempty"`

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

func (s *FreeBusyRequestItem) MarshalJSON() ([]byte, error) {
	type noMethod FreeBusyRequestItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type FreeBusyResponse struct {
	// Calendars: List of free/busy information for calendars.
	Calendars map[string]FreeBusyCalendar `json:"calendars,omitempty"`

	// Groups: Expansion of groups.
	Groups map[string]FreeBusyGroup `json:"groups,omitempty"`

	// Kind: Type of the resource ("calendar#freeBusy").
	Kind string `json:"kind,omitempty"`

	// TimeMax: The end of the interval.
	TimeMax string `json:"timeMax,omitempty"`

	// TimeMin: The start of the interval.
	TimeMin string `json:"timeMin,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Calendars") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Calendars") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FreeBusyResponse) MarshalJSON() ([]byte, error) {
	type noMethod FreeBusyResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Setting struct {
	// Etag: ETag of the resource.
	Etag string `json:"etag,omitempty"`

	// Id: The id of the user setting.
	Id string `json:"id,omitempty"`

	// Kind: Type of the resource ("calendar#setting").
	Kind string `json:"kind,omitempty"`

	// Value: Value of the user setting. The format of the value depends on
	// the ID of the setting. It must always be a UTF-8 string of length up
	// to 1024 characters.
	Value string `json:"value,omitempty"`

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

func (s *Setting) MarshalJSON() ([]byte, error) {
	type noMethod Setting
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Settings struct {
	// Etag: Etag of the collection.
	Etag string `json:"etag,omitempty"`

	// Items: List of user settings.
	Items []*Setting `json:"items,omitempty"`

	// Kind: Type of the collection ("calendar#settings").
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result.
	// Omitted if no further results are available, in which case
	// nextSyncToken is provided.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// NextSyncToken: Token used at a later point in time to retrieve only
	// the entries that have changed since this result was returned. Omitted
	// if further results are available, in which case nextPageToken is
	// provided.
	NextSyncToken string `json:"nextSyncToken,omitempty"`

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

func (s *Settings) MarshalJSON() ([]byte, error) {
	type noMethod Settings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TimePeriod struct {
	// End: The (exclusive) end of the time period.
	End string `json:"end,omitempty"`

	// Start: The (inclusive) start of the time period.
	Start string `json:"start,omitempty"`

	// ForceSendFields is a list of field names (e.g. "End") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "End") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TimePeriod) MarshalJSON() ([]byte, error) {
	type noMethod TimePeriod
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "calendar.acl.delete":

type AclDeleteCall struct {
	s          *Service
	calendarId string
	ruleId     string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an access control rule.
func (r *AclService) Delete(calendarId string, ruleId string) *AclDeleteCall {
	c := &AclDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.ruleId = ruleId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclDeleteCall) Fields(s ...googleapi.Field) *AclDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclDeleteCall) Context(ctx context.Context) *AclDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl/{ruleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"ruleId":     c.ruleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.delete" call.
func (c *AclDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes an access control rule.",
	//   "httpMethod": "DELETE",
	//   "id": "calendar.acl.delete",
	//   "parameterOrder": [
	//     "calendarId",
	//     "ruleId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "ruleId": {
	//       "description": "ACL rule identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl/{ruleId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.acl.get":

type AclGetCall struct {
	s            *Service
	calendarId   string
	ruleId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns an access control rule.
func (r *AclService) Get(calendarId string, ruleId string) *AclGetCall {
	c := &AclGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.ruleId = ruleId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclGetCall) Fields(s ...googleapi.Field) *AclGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AclGetCall) IfNoneMatch(entityTag string) *AclGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclGetCall) Context(ctx context.Context) *AclGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl/{ruleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"ruleId":     c.ruleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.get" call.
// Exactly one of *AclRule or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *AclRule.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AclGetCall) Do(opts ...googleapi.CallOption) (*AclRule, error) {
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
	ret := &AclRule{
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
	//   "description": "Returns an access control rule.",
	//   "httpMethod": "GET",
	//   "id": "calendar.acl.get",
	//   "parameterOrder": [
	//     "calendarId",
	//     "ruleId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "ruleId": {
	//       "description": "ACL rule identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl/{ruleId}",
	//   "response": {
	//     "$ref": "AclRule"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.acl.insert":

type AclInsertCall struct {
	s          *Service
	calendarId string
	aclrule    *AclRule
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates an access control rule.
func (r *AclService) Insert(calendarId string, aclrule *AclRule) *AclInsertCall {
	c := &AclInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.aclrule = aclrule
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclInsertCall) Fields(s ...googleapi.Field) *AclInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclInsertCall) Context(ctx context.Context) *AclInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.aclrule)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.insert" call.
// Exactly one of *AclRule or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *AclRule.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AclInsertCall) Do(opts ...googleapi.CallOption) (*AclRule, error) {
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
	ret := &AclRule{
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
	//   "description": "Creates an access control rule.",
	//   "httpMethod": "POST",
	//   "id": "calendar.acl.insert",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl",
	//   "request": {
	//     "$ref": "AclRule"
	//   },
	//   "response": {
	//     "$ref": "AclRule"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.acl.list":

type AclListCall struct {
	s            *Service
	calendarId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns the rules in the access control list for the calendar.
func (r *AclService) List(calendarId string) *AclListCall {
	c := &AclListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. By default the value is 100
// entries. The page size can never be larger than 250 entries.
func (c *AclListCall) MaxResults(maxResults int64) *AclListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *AclListCall) PageToken(pageToken string) *AclListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted ACLs in the result. Deleted ACLs are represented by
// role equal to "none". Deleted ACLs will always be included if
// syncToken is provided.  The default is False.
func (c *AclListCall) ShowDeleted(showDeleted bool) *AclListCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then. All
// entries deleted since the previous list request will always be in the
// result set and it is not allowed to set showDeleted to False.
// If the syncToken expires, the server will respond with a 410 GONE
// response code and the client should clear its storage and perform a
// full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *AclListCall) SyncToken(syncToken string) *AclListCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclListCall) Fields(s ...googleapi.Field) *AclListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AclListCall) IfNoneMatch(entityTag string) *AclListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclListCall) Context(ctx context.Context) *AclListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.list" call.
// Exactly one of *Acl or error will be non-nil. Any non-2xx status code
// is an error. Response headers are in either
// *Acl.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *AclListCall) Do(opts ...googleapi.CallOption) (*Acl, error) {
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
	ret := &Acl{
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
	//   "description": "Returns the rules in the access control list for the calendar.",
	//   "httpMethod": "GET",
	//   "id": "calendar.acl.list",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. By default the value is 100 entries. The page size can never be larger than 250 entries. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted ACLs in the result. Deleted ACLs are represented by role equal to \"none\". Deleted ACLs will always be included if syncToken is provided. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then. All entries deleted since the previous list request will always be in the result set and it is not allowed to set showDeleted to False.\nIf the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl",
	//   "response": {
	//     "$ref": "Acl"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ],
	//   "supportsSubscription": true
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *AclListCall) Pages(ctx context.Context, f func(*Acl) error) error {
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

// method id "calendar.acl.patch":

type AclPatchCall struct {
	s          *Service
	calendarId string
	ruleId     string
	aclrule    *AclRule
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates an access control rule. This method supports patch
// semantics.
func (r *AclService) Patch(calendarId string, ruleId string, aclrule *AclRule) *AclPatchCall {
	c := &AclPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.ruleId = ruleId
	c.aclrule = aclrule
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclPatchCall) Fields(s ...googleapi.Field) *AclPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclPatchCall) Context(ctx context.Context) *AclPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.aclrule)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl/{ruleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"ruleId":     c.ruleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.patch" call.
// Exactly one of *AclRule or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *AclRule.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AclPatchCall) Do(opts ...googleapi.CallOption) (*AclRule, error) {
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
	ret := &AclRule{
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
	//   "description": "Updates an access control rule. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "calendar.acl.patch",
	//   "parameterOrder": [
	//     "calendarId",
	//     "ruleId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "ruleId": {
	//       "description": "ACL rule identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl/{ruleId}",
	//   "request": {
	//     "$ref": "AclRule"
	//   },
	//   "response": {
	//     "$ref": "AclRule"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.acl.update":

type AclUpdateCall struct {
	s          *Service
	calendarId string
	ruleId     string
	aclrule    *AclRule
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates an access control rule.
func (r *AclService) Update(calendarId string, ruleId string, aclrule *AclRule) *AclUpdateCall {
	c := &AclUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.ruleId = ruleId
	c.aclrule = aclrule
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclUpdateCall) Fields(s ...googleapi.Field) *AclUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclUpdateCall) Context(ctx context.Context) *AclUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.aclrule)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl/{ruleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"ruleId":     c.ruleId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.update" call.
// Exactly one of *AclRule or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *AclRule.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AclUpdateCall) Do(opts ...googleapi.CallOption) (*AclRule, error) {
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
	ret := &AclRule{
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
	//   "description": "Updates an access control rule.",
	//   "httpMethod": "PUT",
	//   "id": "calendar.acl.update",
	//   "parameterOrder": [
	//     "calendarId",
	//     "ruleId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "ruleId": {
	//       "description": "ACL rule identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl/{ruleId}",
	//   "request": {
	//     "$ref": "AclRule"
	//   },
	//   "response": {
	//     "$ref": "AclRule"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.acl.watch":

type AclWatchCall struct {
	s          *Service
	calendarId string
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Watch: Watch for changes to ACL resources.
func (r *AclService) Watch(calendarId string, channel *Channel) *AclWatchCall {
	c := &AclWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.channel = channel
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. By default the value is 100
// entries. The page size can never be larger than 250 entries.
func (c *AclWatchCall) MaxResults(maxResults int64) *AclWatchCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *AclWatchCall) PageToken(pageToken string) *AclWatchCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted ACLs in the result. Deleted ACLs are represented by
// role equal to "none". Deleted ACLs will always be included if
// syncToken is provided.  The default is False.
func (c *AclWatchCall) ShowDeleted(showDeleted bool) *AclWatchCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then. All
// entries deleted since the previous list request will always be in the
// result set and it is not allowed to set showDeleted to False.
// If the syncToken expires, the server will respond with a 410 GONE
// response code and the client should clear its storage and perform a
// full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *AclWatchCall) SyncToken(syncToken string) *AclWatchCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AclWatchCall) Fields(s ...googleapi.Field) *AclWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AclWatchCall) Context(ctx context.Context) *AclWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AclWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AclWatchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/acl/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.acl.watch" call.
// Exactly one of *Channel or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Channel.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AclWatchCall) Do(opts ...googleapi.CallOption) (*Channel, error) {
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
	//   "description": "Watch for changes to ACL resources.",
	//   "httpMethod": "POST",
	//   "id": "calendar.acl.watch",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. By default the value is 100 entries. The page size can never be larger than 250 entries. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted ACLs in the result. Deleted ACLs are represented by role equal to \"none\". Deleted ACLs will always be included if syncToken is provided. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then. All entries deleted since the previous list request will always be in the result set and it is not allowed to set showDeleted to False.\nIf the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/acl/watch",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "response": {
	//     "$ref": "Channel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "calendar.calendarList.delete":

type CalendarListDeleteCall struct {
	s          *Service
	calendarId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an entry on the user's calendar list.
func (r *CalendarListService) Delete(calendarId string) *CalendarListDeleteCall {
	c := &CalendarListDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListDeleteCall) Fields(s ...googleapi.Field) *CalendarListDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListDeleteCall) Context(ctx context.Context) *CalendarListDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.delete" call.
func (c *CalendarListDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes an entry on the user's calendar list.",
	//   "httpMethod": "DELETE",
	//   "id": "calendar.calendarList.delete",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/calendarList/{calendarId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendarList.get":

type CalendarListGetCall struct {
	s            *Service
	calendarId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns an entry on the user's calendar list.
func (r *CalendarListService) Get(calendarId string) *CalendarListGetCall {
	c := &CalendarListGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListGetCall) Fields(s ...googleapi.Field) *CalendarListGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CalendarListGetCall) IfNoneMatch(entityTag string) *CalendarListGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListGetCall) Context(ctx context.Context) *CalendarListGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.get" call.
// Exactly one of *CalendarListEntry or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarListEntry.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CalendarListGetCall) Do(opts ...googleapi.CallOption) (*CalendarListEntry, error) {
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
	ret := &CalendarListEntry{
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
	//   "description": "Returns an entry on the user's calendar list.",
	//   "httpMethod": "GET",
	//   "id": "calendar.calendarList.get",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/calendarList/{calendarId}",
	//   "response": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.calendarList.insert":

type CalendarListInsertCall struct {
	s                 *Service
	calendarlistentry *CalendarListEntry
	urlParams_        gensupport.URLParams
	ctx_              context.Context
	header_           http.Header
}

// Insert: Adds an entry to the user's calendar list.
func (r *CalendarListService) Insert(calendarlistentry *CalendarListEntry) *CalendarListInsertCall {
	c := &CalendarListInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarlistentry = calendarlistentry
	return c
}

// ColorRgbFormat sets the optional parameter "colorRgbFormat": Whether
// to use the foregroundColor and backgroundColor fields to write the
// calendar colors (RGB). If this feature is used, the index-based
// colorId field will be set to the best matching option automatically.
// The default is False.
func (c *CalendarListInsertCall) ColorRgbFormat(colorRgbFormat bool) *CalendarListInsertCall {
	c.urlParams_.Set("colorRgbFormat", fmt.Sprint(colorRgbFormat))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListInsertCall) Fields(s ...googleapi.Field) *CalendarListInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListInsertCall) Context(ctx context.Context) *CalendarListInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendarlistentry)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.insert" call.
// Exactly one of *CalendarListEntry or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarListEntry.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CalendarListInsertCall) Do(opts ...googleapi.CallOption) (*CalendarListEntry, error) {
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
	ret := &CalendarListEntry{
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
	//   "description": "Adds an entry to the user's calendar list.",
	//   "httpMethod": "POST",
	//   "id": "calendar.calendarList.insert",
	//   "parameters": {
	//     "colorRgbFormat": {
	//       "description": "Whether to use the foregroundColor and backgroundColor fields to write the calendar colors (RGB). If this feature is used, the index-based colorId field will be set to the best matching option automatically. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "users/me/calendarList",
	//   "request": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "response": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendarList.list":

type CalendarListListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns entries on the user's calendar list.
func (r *CalendarListService) List() *CalendarListListCall {
	c := &CalendarListListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. By default the value is 100
// entries. The page size can never be larger than 250 entries.
func (c *CalendarListListCall) MaxResults(maxResults int64) *CalendarListListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// MinAccessRole sets the optional parameter "minAccessRole": The
// minimum access role for the user in the returned entries.  The
// default is no restriction.
//
// Possible values:
//   "freeBusyReader" - The user can read free/busy information.
//   "owner" - The user can read and modify events and access control
// lists.
//   "reader" - The user can read events that are not private.
//   "writer" - The user can read and modify events.
func (c *CalendarListListCall) MinAccessRole(minAccessRole string) *CalendarListListCall {
	c.urlParams_.Set("minAccessRole", minAccessRole)
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *CalendarListListCall) PageToken(pageToken string) *CalendarListListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted calendar list entries in the result.  The default is
// False.
func (c *CalendarListListCall) ShowDeleted(showDeleted bool) *CalendarListListCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// ShowHidden sets the optional parameter "showHidden": Whether to show
// hidden entries.  The default is False.
func (c *CalendarListListCall) ShowHidden(showHidden bool) *CalendarListListCall {
	c.urlParams_.Set("showHidden", fmt.Sprint(showHidden))
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then. If only
// read-only fields such as calendar properties or ACLs have changed,
// the entry won't be returned. All entries deleted and hidden since the
// previous list request will always be in the result set and it is not
// allowed to set showDeleted neither showHidden to False.
// To ensure client state consistency minAccessRole query parameter
// cannot be specified together with nextSyncToken.
// If the syncToken expires, the server will respond with a 410 GONE
// response code and the client should clear its storage and perform a
// full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *CalendarListListCall) SyncToken(syncToken string) *CalendarListListCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListListCall) Fields(s ...googleapi.Field) *CalendarListListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CalendarListListCall) IfNoneMatch(entityTag string) *CalendarListListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListListCall) Context(ctx context.Context) *CalendarListListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.list" call.
// Exactly one of *CalendarList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CalendarList.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CalendarListListCall) Do(opts ...googleapi.CallOption) (*CalendarList, error) {
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
	ret := &CalendarList{
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
	//   "description": "Returns entries on the user's calendar list.",
	//   "httpMethod": "GET",
	//   "id": "calendar.calendarList.list",
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. By default the value is 100 entries. The page size can never be larger than 250 entries. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "minAccessRole": {
	//       "description": "The minimum access role for the user in the returned entries. Optional. The default is no restriction.",
	//       "enum": [
	//         "freeBusyReader",
	//         "owner",
	//         "reader",
	//         "writer"
	//       ],
	//       "enumDescriptions": [
	//         "The user can read free/busy information.",
	//         "The user can read and modify events and access control lists.",
	//         "The user can read events that are not private.",
	//         "The user can read and modify events."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted calendar list entries in the result. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "showHidden": {
	//       "description": "Whether to show hidden entries. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then. If only read-only fields such as calendar properties or ACLs have changed, the entry won't be returned. All entries deleted and hidden since the previous list request will always be in the result set and it is not allowed to set showDeleted neither showHidden to False.\nTo ensure client state consistency minAccessRole query parameter cannot be specified together with nextSyncToken.\nIf the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/calendarList",
	//   "response": {
	//     "$ref": "CalendarList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CalendarListListCall) Pages(ctx context.Context, f func(*CalendarList) error) error {
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

// method id "calendar.calendarList.patch":

type CalendarListPatchCall struct {
	s                 *Service
	calendarId        string
	calendarlistentry *CalendarListEntry
	urlParams_        gensupport.URLParams
	ctx_              context.Context
	header_           http.Header
}

// Patch: Updates an entry on the user's calendar list. This method
// supports patch semantics.
func (r *CalendarListService) Patch(calendarId string, calendarlistentry *CalendarListEntry) *CalendarListPatchCall {
	c := &CalendarListPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.calendarlistentry = calendarlistentry
	return c
}

// ColorRgbFormat sets the optional parameter "colorRgbFormat": Whether
// to use the foregroundColor and backgroundColor fields to write the
// calendar colors (RGB). If this feature is used, the index-based
// colorId field will be set to the best matching option automatically.
// The default is False.
func (c *CalendarListPatchCall) ColorRgbFormat(colorRgbFormat bool) *CalendarListPatchCall {
	c.urlParams_.Set("colorRgbFormat", fmt.Sprint(colorRgbFormat))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListPatchCall) Fields(s ...googleapi.Field) *CalendarListPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListPatchCall) Context(ctx context.Context) *CalendarListPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendarlistentry)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.patch" call.
// Exactly one of *CalendarListEntry or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarListEntry.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CalendarListPatchCall) Do(opts ...googleapi.CallOption) (*CalendarListEntry, error) {
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
	ret := &CalendarListEntry{
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
	//   "description": "Updates an entry on the user's calendar list. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "calendar.calendarList.patch",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "colorRgbFormat": {
	//       "description": "Whether to use the foregroundColor and backgroundColor fields to write the calendar colors (RGB). If this feature is used, the index-based colorId field will be set to the best matching option automatically. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "users/me/calendarList/{calendarId}",
	//   "request": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "response": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendarList.update":

type CalendarListUpdateCall struct {
	s                 *Service
	calendarId        string
	calendarlistentry *CalendarListEntry
	urlParams_        gensupport.URLParams
	ctx_              context.Context
	header_           http.Header
}

// Update: Updates an entry on the user's calendar list.
func (r *CalendarListService) Update(calendarId string, calendarlistentry *CalendarListEntry) *CalendarListUpdateCall {
	c := &CalendarListUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.calendarlistentry = calendarlistentry
	return c
}

// ColorRgbFormat sets the optional parameter "colorRgbFormat": Whether
// to use the foregroundColor and backgroundColor fields to write the
// calendar colors (RGB). If this feature is used, the index-based
// colorId field will be set to the best matching option automatically.
// The default is False.
func (c *CalendarListUpdateCall) ColorRgbFormat(colorRgbFormat bool) *CalendarListUpdateCall {
	c.urlParams_.Set("colorRgbFormat", fmt.Sprint(colorRgbFormat))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListUpdateCall) Fields(s ...googleapi.Field) *CalendarListUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListUpdateCall) Context(ctx context.Context) *CalendarListUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendarlistentry)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.update" call.
// Exactly one of *CalendarListEntry or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CalendarListEntry.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CalendarListUpdateCall) Do(opts ...googleapi.CallOption) (*CalendarListEntry, error) {
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
	ret := &CalendarListEntry{
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
	//   "description": "Updates an entry on the user's calendar list.",
	//   "httpMethod": "PUT",
	//   "id": "calendar.calendarList.update",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "colorRgbFormat": {
	//       "description": "Whether to use the foregroundColor and backgroundColor fields to write the calendar colors (RGB). If this feature is used, the index-based colorId field will be set to the best matching option automatically. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "users/me/calendarList/{calendarId}",
	//   "request": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "response": {
	//     "$ref": "CalendarListEntry"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendarList.watch":

type CalendarListWatchCall struct {
	s          *Service
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Watch: Watch for changes to CalendarList resources.
func (r *CalendarListService) Watch(channel *Channel) *CalendarListWatchCall {
	c := &CalendarListWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.channel = channel
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. By default the value is 100
// entries. The page size can never be larger than 250 entries.
func (c *CalendarListWatchCall) MaxResults(maxResults int64) *CalendarListWatchCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// MinAccessRole sets the optional parameter "minAccessRole": The
// minimum access role for the user in the returned entries.  The
// default is no restriction.
//
// Possible values:
//   "freeBusyReader" - The user can read free/busy information.
//   "owner" - The user can read and modify events and access control
// lists.
//   "reader" - The user can read events that are not private.
//   "writer" - The user can read and modify events.
func (c *CalendarListWatchCall) MinAccessRole(minAccessRole string) *CalendarListWatchCall {
	c.urlParams_.Set("minAccessRole", minAccessRole)
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *CalendarListWatchCall) PageToken(pageToken string) *CalendarListWatchCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted calendar list entries in the result.  The default is
// False.
func (c *CalendarListWatchCall) ShowDeleted(showDeleted bool) *CalendarListWatchCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// ShowHidden sets the optional parameter "showHidden": Whether to show
// hidden entries.  The default is False.
func (c *CalendarListWatchCall) ShowHidden(showHidden bool) *CalendarListWatchCall {
	c.urlParams_.Set("showHidden", fmt.Sprint(showHidden))
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then. If only
// read-only fields such as calendar properties or ACLs have changed,
// the entry won't be returned. All entries deleted and hidden since the
// previous list request will always be in the result set and it is not
// allowed to set showDeleted neither showHidden to False.
// To ensure client state consistency minAccessRole query parameter
// cannot be specified together with nextSyncToken.
// If the syncToken expires, the server will respond with a 410 GONE
// response code and the client should clear its storage and perform a
// full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *CalendarListWatchCall) SyncToken(syncToken string) *CalendarListWatchCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarListWatchCall) Fields(s ...googleapi.Field) *CalendarListWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarListWatchCall) Context(ctx context.Context) *CalendarListWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarListWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarListWatchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/calendarList/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendarList.watch" call.
// Exactly one of *Channel or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Channel.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CalendarListWatchCall) Do(opts ...googleapi.CallOption) (*Channel, error) {
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
	//   "description": "Watch for changes to CalendarList resources.",
	//   "httpMethod": "POST",
	//   "id": "calendar.calendarList.watch",
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. By default the value is 100 entries. The page size can never be larger than 250 entries. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "minAccessRole": {
	//       "description": "The minimum access role for the user in the returned entries. Optional. The default is no restriction.",
	//       "enum": [
	//         "freeBusyReader",
	//         "owner",
	//         "reader",
	//         "writer"
	//       ],
	//       "enumDescriptions": [
	//         "The user can read free/busy information.",
	//         "The user can read and modify events and access control lists.",
	//         "The user can read events that are not private.",
	//         "The user can read and modify events."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted calendar list entries in the result. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "showHidden": {
	//       "description": "Whether to show hidden entries. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then. If only read-only fields such as calendar properties or ACLs have changed, the entry won't be returned. All entries deleted and hidden since the previous list request will always be in the result set and it is not allowed to set showDeleted neither showHidden to False.\nTo ensure client state consistency minAccessRole query parameter cannot be specified together with nextSyncToken.\nIf the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/calendarList/watch",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "response": {
	//     "$ref": "Channel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "calendar.calendars.clear":

type CalendarsClearCall struct {
	s          *Service
	calendarId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Clear: Clears a primary calendar. This operation deletes all events
// associated with the primary calendar of an account.
func (r *CalendarsService) Clear(calendarId string) *CalendarsClearCall {
	c := &CalendarsClearCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarsClearCall) Fields(s ...googleapi.Field) *CalendarsClearCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarsClearCall) Context(ctx context.Context) *CalendarsClearCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarsClearCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarsClearCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/clear")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendars.clear" call.
func (c *CalendarsClearCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Clears a primary calendar. This operation deletes all events associated with the primary calendar of an account.",
	//   "httpMethod": "POST",
	//   "id": "calendar.calendars.clear",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/clear",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendars.delete":

type CalendarsDeleteCall struct {
	s          *Service
	calendarId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a secondary calendar. Use calendars.clear for
// clearing all events on primary calendars.
func (r *CalendarsService) Delete(calendarId string) *CalendarsDeleteCall {
	c := &CalendarsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarsDeleteCall) Fields(s ...googleapi.Field) *CalendarsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarsDeleteCall) Context(ctx context.Context) *CalendarsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendars.delete" call.
func (c *CalendarsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a secondary calendar. Use calendars.clear for clearing all events on primary calendars.",
	//   "httpMethod": "DELETE",
	//   "id": "calendar.calendars.delete",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendars.get":

type CalendarsGetCall struct {
	s            *Service
	calendarId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns metadata for a calendar.
func (r *CalendarsService) Get(calendarId string) *CalendarsGetCall {
	c := &CalendarsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarsGetCall) Fields(s ...googleapi.Field) *CalendarsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CalendarsGetCall) IfNoneMatch(entityTag string) *CalendarsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarsGetCall) Context(ctx context.Context) *CalendarsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendars.get" call.
// Exactly one of *Calendar or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Calendar.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CalendarsGetCall) Do(opts ...googleapi.CallOption) (*Calendar, error) {
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
	ret := &Calendar{
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
	//   "description": "Returns metadata for a calendar.",
	//   "httpMethod": "GET",
	//   "id": "calendar.calendars.get",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}",
	//   "response": {
	//     "$ref": "Calendar"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.calendars.insert":

type CalendarsInsertCall struct {
	s          *Service
	calendar   *Calendar
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates a secondary calendar.
func (r *CalendarsService) Insert(calendar *Calendar) *CalendarsInsertCall {
	c := &CalendarsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendar = calendar
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarsInsertCall) Fields(s ...googleapi.Field) *CalendarsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarsInsertCall) Context(ctx context.Context) *CalendarsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendar)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendars.insert" call.
// Exactly one of *Calendar or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Calendar.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CalendarsInsertCall) Do(opts ...googleapi.CallOption) (*Calendar, error) {
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
	ret := &Calendar{
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
	//   "description": "Creates a secondary calendar.",
	//   "httpMethod": "POST",
	//   "id": "calendar.calendars.insert",
	//   "path": "calendars",
	//   "request": {
	//     "$ref": "Calendar"
	//   },
	//   "response": {
	//     "$ref": "Calendar"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendars.patch":

type CalendarsPatchCall struct {
	s          *Service
	calendarId string
	calendar   *Calendar
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates metadata for a calendar. This method supports patch
// semantics.
func (r *CalendarsService) Patch(calendarId string, calendar *Calendar) *CalendarsPatchCall {
	c := &CalendarsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.calendar = calendar
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarsPatchCall) Fields(s ...googleapi.Field) *CalendarsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarsPatchCall) Context(ctx context.Context) *CalendarsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendar)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendars.patch" call.
// Exactly one of *Calendar or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Calendar.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CalendarsPatchCall) Do(opts ...googleapi.CallOption) (*Calendar, error) {
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
	ret := &Calendar{
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
	//   "description": "Updates metadata for a calendar. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "calendar.calendars.patch",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}",
	//   "request": {
	//     "$ref": "Calendar"
	//   },
	//   "response": {
	//     "$ref": "Calendar"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.calendars.update":

type CalendarsUpdateCall struct {
	s          *Service
	calendarId string
	calendar   *Calendar
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates metadata for a calendar.
func (r *CalendarsService) Update(calendarId string, calendar *Calendar) *CalendarsUpdateCall {
	c := &CalendarsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.calendar = calendar
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CalendarsUpdateCall) Fields(s ...googleapi.Field) *CalendarsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CalendarsUpdateCall) Context(ctx context.Context) *CalendarsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CalendarsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CalendarsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.calendar)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.calendars.update" call.
// Exactly one of *Calendar or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Calendar.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CalendarsUpdateCall) Do(opts ...googleapi.CallOption) (*Calendar, error) {
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
	ret := &Calendar{
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
	//   "description": "Updates metadata for a calendar.",
	//   "httpMethod": "PUT",
	//   "id": "calendar.calendars.update",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}",
	//   "request": {
	//     "$ref": "Calendar"
	//   },
	//   "response": {
	//     "$ref": "Calendar"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.channels.stop":

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
	urls := googleapi.ResolveRelative(c.s.BasePath, "channels/stop")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.channels.stop" call.
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
	//   "id": "calendar.channels.stop",
	//   "path": "channels/stop",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.colors.get":

type ColorsGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns the color definitions for calendars and events.
func (r *ColorsService) Get() *ColorsGetCall {
	c := &ColorsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColorsGetCall) Fields(s ...googleapi.Field) *ColorsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ColorsGetCall) IfNoneMatch(entityTag string) *ColorsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColorsGetCall) Context(ctx context.Context) *ColorsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColorsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColorsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "colors")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.colors.get" call.
// Exactly one of *Colors or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Colors.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ColorsGetCall) Do(opts ...googleapi.CallOption) (*Colors, error) {
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
	ret := &Colors{
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
	//   "description": "Returns the color definitions for calendars and events.",
	//   "httpMethod": "GET",
	//   "id": "calendar.colors.get",
	//   "path": "colors",
	//   "response": {
	//     "$ref": "Colors"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.events.delete":

type EventsDeleteCall struct {
	s          *Service
	calendarId string
	eventId    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an event.
func (r *EventsService) Delete(calendarId string, eventId string) *EventsDeleteCall {
	c := &EventsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.eventId = eventId
	return c
}

// SendNotifications sets the optional parameter "sendNotifications":
// Whether to send notifications about the deletion of the event.  The
// default is False.
func (c *EventsDeleteCall) SendNotifications(sendNotifications bool) *EventsDeleteCall {
	c.urlParams_.Set("sendNotifications", fmt.Sprint(sendNotifications))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsDeleteCall) Fields(s ...googleapi.Field) *EventsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsDeleteCall) Context(ctx context.Context) *EventsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/{eventId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"eventId":    c.eventId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.delete" call.
func (c *EventsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes an event.",
	//   "httpMethod": "DELETE",
	//   "id": "calendar.events.delete",
	//   "parameterOrder": [
	//     "calendarId",
	//     "eventId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "eventId": {
	//       "description": "Event identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "sendNotifications": {
	//       "description": "Whether to send notifications about the deletion of the event. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/{eventId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.get":

type EventsGetCall struct {
	s            *Service
	calendarId   string
	eventId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns an event.
func (r *EventsService) Get(calendarId string, eventId string) *EventsGetCall {
	c := &EventsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.eventId = eventId
	return c
}

// AlwaysIncludeEmail sets the optional parameter "alwaysIncludeEmail":
// Whether to always include a value in the email field for the
// organizer, creator and attendees, even if no real email is available
// (i.e. a generated, non-working value will be provided). The use of
// this option is discouraged and should only be used by clients which
// cannot handle the absence of an email address value in the mentioned
// places.  The default is False.
func (c *EventsGetCall) AlwaysIncludeEmail(alwaysIncludeEmail bool) *EventsGetCall {
	c.urlParams_.Set("alwaysIncludeEmail", fmt.Sprint(alwaysIncludeEmail))
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsGetCall) MaxAttendees(maxAttendees int64) *EventsGetCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// TimeZone sets the optional parameter "timeZone": Time zone used in
// the response.  The default is the time zone of the calendar.
func (c *EventsGetCall) TimeZone(timeZone string) *EventsGetCall {
	c.urlParams_.Set("timeZone", timeZone)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsGetCall) Fields(s ...googleapi.Field) *EventsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *EventsGetCall) IfNoneMatch(entityTag string) *EventsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsGetCall) Context(ctx context.Context) *EventsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/{eventId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"eventId":    c.eventId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.get" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsGetCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Returns an event.",
	//   "httpMethod": "GET",
	//   "id": "calendar.events.get",
	//   "parameterOrder": [
	//     "calendarId",
	//     "eventId"
	//   ],
	//   "parameters": {
	//     "alwaysIncludeEmail": {
	//       "description": "Whether to always include a value in the email field for the organizer, creator and attendees, even if no real email is available (i.e. a generated, non-working value will be provided). The use of this option is discouraged and should only be used by clients which cannot handle the absence of an email address value in the mentioned places. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "eventId": {
	//       "description": "Event identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "timeZone": {
	//       "description": "Time zone used in the response. Optional. The default is the time zone of the calendar.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/{eventId}",
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.events.import":

type EventsImportCall struct {
	s          *Service
	calendarId string
	event      *Event
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Import: Imports an event. This operation is used to add a private
// copy of an existing event to a calendar.
func (r *EventsService) Import(calendarId string, event *Event) *EventsImportCall {
	c := &EventsImportCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.event = event
	return c
}

// SupportsAttachments sets the optional parameter
// "supportsAttachments": Whether API client performing operation
// supports event attachments.  The default is False.
func (c *EventsImportCall) SupportsAttachments(supportsAttachments bool) *EventsImportCall {
	c.urlParams_.Set("supportsAttachments", fmt.Sprint(supportsAttachments))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsImportCall) Fields(s ...googleapi.Field) *EventsImportCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsImportCall) Context(ctx context.Context) *EventsImportCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsImportCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsImportCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.event)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/import")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.import" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsImportCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Imports an event. This operation is used to add a private copy of an existing event to a calendar.",
	//   "httpMethod": "POST",
	//   "id": "calendar.events.import",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "supportsAttachments": {
	//       "description": "Whether API client performing operation supports event attachments. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/import",
	//   "request": {
	//     "$ref": "Event"
	//   },
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.insert":

type EventsInsertCall struct {
	s          *Service
	calendarId string
	event      *Event
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates an event.
func (r *EventsService) Insert(calendarId string, event *Event) *EventsInsertCall {
	c := &EventsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.event = event
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsInsertCall) MaxAttendees(maxAttendees int64) *EventsInsertCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// SendNotifications sets the optional parameter "sendNotifications":
// Whether to send notifications about the creation of the new event.
// The default is False.
func (c *EventsInsertCall) SendNotifications(sendNotifications bool) *EventsInsertCall {
	c.urlParams_.Set("sendNotifications", fmt.Sprint(sendNotifications))
	return c
}

// SupportsAttachments sets the optional parameter
// "supportsAttachments": Whether API client performing operation
// supports event attachments.  The default is False.
func (c *EventsInsertCall) SupportsAttachments(supportsAttachments bool) *EventsInsertCall {
	c.urlParams_.Set("supportsAttachments", fmt.Sprint(supportsAttachments))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsInsertCall) Fields(s ...googleapi.Field) *EventsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsInsertCall) Context(ctx context.Context) *EventsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.event)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.insert" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsInsertCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Creates an event.",
	//   "httpMethod": "POST",
	//   "id": "calendar.events.insert",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "sendNotifications": {
	//       "description": "Whether to send notifications about the creation of the new event. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "supportsAttachments": {
	//       "description": "Whether API client performing operation supports event attachments. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events",
	//   "request": {
	//     "$ref": "Event"
	//   },
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.instances":

type EventsInstancesCall struct {
	s            *Service
	calendarId   string
	eventId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Instances: Returns instances of the specified recurring event.
func (r *EventsService) Instances(calendarId string, eventId string) *EventsInstancesCall {
	c := &EventsInstancesCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.eventId = eventId
	return c
}

// AlwaysIncludeEmail sets the optional parameter "alwaysIncludeEmail":
// Whether to always include a value in the email field for the
// organizer, creator and attendees, even if no real email is available
// (i.e. a generated, non-working value will be provided). The use of
// this option is discouraged and should only be used by clients which
// cannot handle the absence of an email address value in the mentioned
// places.  The default is False.
func (c *EventsInstancesCall) AlwaysIncludeEmail(alwaysIncludeEmail bool) *EventsInstancesCall {
	c.urlParams_.Set("alwaysIncludeEmail", fmt.Sprint(alwaysIncludeEmail))
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsInstancesCall) MaxAttendees(maxAttendees int64) *EventsInstancesCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of events returned on one result page. By default the value is 250
// events. The page size can never be larger than 2500 events.
func (c *EventsInstancesCall) MaxResults(maxResults int64) *EventsInstancesCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OriginalStart sets the optional parameter "originalStart": The
// original start time of the instance in the result.
func (c *EventsInstancesCall) OriginalStart(originalStart string) *EventsInstancesCall {
	c.urlParams_.Set("originalStart", originalStart)
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *EventsInstancesCall) PageToken(pageToken string) *EventsInstancesCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted events (with status equals "cancelled") in the
// result. Cancelled instances of recurring events will still be
// included if singleEvents is False.  The default is False.
func (c *EventsInstancesCall) ShowDeleted(showDeleted bool) *EventsInstancesCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// TimeMax sets the optional parameter "timeMax": Upper bound
// (exclusive) for an event's start time to filter by.  The default is
// not to filter by start time. Must be an RFC3339 timestamp with
// mandatory time zone offset.
func (c *EventsInstancesCall) TimeMax(timeMax string) *EventsInstancesCall {
	c.urlParams_.Set("timeMax", timeMax)
	return c
}

// TimeMin sets the optional parameter "timeMin": Lower bound
// (inclusive) for an event's end time to filter by.  The default is not
// to filter by end time. Must be an RFC3339 timestamp with mandatory
// time zone offset.
func (c *EventsInstancesCall) TimeMin(timeMin string) *EventsInstancesCall {
	c.urlParams_.Set("timeMin", timeMin)
	return c
}

// TimeZone sets the optional parameter "timeZone": Time zone used in
// the response.  The default is the time zone of the calendar.
func (c *EventsInstancesCall) TimeZone(timeZone string) *EventsInstancesCall {
	c.urlParams_.Set("timeZone", timeZone)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsInstancesCall) Fields(s ...googleapi.Field) *EventsInstancesCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *EventsInstancesCall) IfNoneMatch(entityTag string) *EventsInstancesCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsInstancesCall) Context(ctx context.Context) *EventsInstancesCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsInstancesCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsInstancesCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/{eventId}/instances")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"eventId":    c.eventId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.instances" call.
// Exactly one of *Events or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Events.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsInstancesCall) Do(opts ...googleapi.CallOption) (*Events, error) {
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
	ret := &Events{
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
	//   "description": "Returns instances of the specified recurring event.",
	//   "httpMethod": "GET",
	//   "id": "calendar.events.instances",
	//   "parameterOrder": [
	//     "calendarId",
	//     "eventId"
	//   ],
	//   "parameters": {
	//     "alwaysIncludeEmail": {
	//       "description": "Whether to always include a value in the email field for the organizer, creator and attendees, even if no real email is available (i.e. a generated, non-working value will be provided). The use of this option is discouraged and should only be used by clients which cannot handle the absence of an email address value in the mentioned places. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "eventId": {
	//       "description": "Recurring event identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of events returned on one result page. By default the value is 250 events. The page size can never be larger than 2500 events. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "originalStart": {
	//       "description": "The original start time of the instance in the result. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted events (with status equals \"cancelled\") in the result. Cancelled instances of recurring events will still be included if singleEvents is False. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "timeMax": {
	//       "description": "Upper bound (exclusive) for an event's start time to filter by. Optional. The default is not to filter by start time. Must be an RFC3339 timestamp with mandatory time zone offset.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeMin": {
	//       "description": "Lower bound (inclusive) for an event's end time to filter by. Optional. The default is not to filter by end time. Must be an RFC3339 timestamp with mandatory time zone offset.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeZone": {
	//       "description": "Time zone used in the response. Optional. The default is the time zone of the calendar.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/{eventId}/instances",
	//   "response": {
	//     "$ref": "Events"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *EventsInstancesCall) Pages(ctx context.Context, f func(*Events) error) error {
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

// method id "calendar.events.list":

type EventsListCall struct {
	s            *Service
	calendarId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns events on the specified calendar.
func (r *EventsService) List(calendarId string) *EventsListCall {
	c := &EventsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	return c
}

// AlwaysIncludeEmail sets the optional parameter "alwaysIncludeEmail":
// Whether to always include a value in the email field for the
// organizer, creator and attendees, even if no real email is available
// (i.e. a generated, non-working value will be provided). The use of
// this option is discouraged and should only be used by clients which
// cannot handle the absence of an email address value in the mentioned
// places.  The default is False.
func (c *EventsListCall) AlwaysIncludeEmail(alwaysIncludeEmail bool) *EventsListCall {
	c.urlParams_.Set("alwaysIncludeEmail", fmt.Sprint(alwaysIncludeEmail))
	return c
}

// ICalUID sets the optional parameter "iCalUID": Specifies event ID in
// the iCalendar format to be included in the response.
func (c *EventsListCall) ICalUID(iCalUID string) *EventsListCall {
	c.urlParams_.Set("iCalUID", iCalUID)
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsListCall) MaxAttendees(maxAttendees int64) *EventsListCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of events returned on one result page. By default the value is 250
// events. The page size can never be larger than 2500 events.
func (c *EventsListCall) MaxResults(maxResults int64) *EventsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": The order of the
// events returned in the result.  The default is an unspecified, stable
// order.
//
// Possible values:
//   "startTime" - Order by the start date/time (ascending). This is
// only available when querying single events (i.e. the parameter
// singleEvents is True)
//   "updated" - Order by last modification time (ascending).
func (c *EventsListCall) OrderBy(orderBy string) *EventsListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *EventsListCall) PageToken(pageToken string) *EventsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// PrivateExtendedProperty sets the optional parameter
// "privateExtendedProperty": Extended properties constraint specified
// as propertyName=value. Matches only private properties. This
// parameter might be repeated multiple times to return events that
// match all given constraints.
func (c *EventsListCall) PrivateExtendedProperty(privateExtendedProperty ...string) *EventsListCall {
	c.urlParams_.SetMulti("privateExtendedProperty", append([]string{}, privateExtendedProperty...))
	return c
}

// Q sets the optional parameter "q": Free text search terms to find
// events that match these terms in any field, except for extended
// properties.
func (c *EventsListCall) Q(q string) *EventsListCall {
	c.urlParams_.Set("q", q)
	return c
}

// SharedExtendedProperty sets the optional parameter
// "sharedExtendedProperty": Extended properties constraint specified as
// propertyName=value. Matches only shared properties. This parameter
// might be repeated multiple times to return events that match all
// given constraints.
func (c *EventsListCall) SharedExtendedProperty(sharedExtendedProperty ...string) *EventsListCall {
	c.urlParams_.SetMulti("sharedExtendedProperty", append([]string{}, sharedExtendedProperty...))
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted events (with status equals "cancelled") in the
// result. Cancelled instances of recurring events (but not the
// underlying recurring event) will still be included if showDeleted and
// singleEvents are both False. If showDeleted and singleEvents are both
// True, only single instances of deleted events (but not the underlying
// recurring events) are returned.  The default is False.
func (c *EventsListCall) ShowDeleted(showDeleted bool) *EventsListCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// ShowHiddenInvitations sets the optional parameter
// "showHiddenInvitations": Whether to include hidden invitations in the
// result.  The default is False.
func (c *EventsListCall) ShowHiddenInvitations(showHiddenInvitations bool) *EventsListCall {
	c.urlParams_.Set("showHiddenInvitations", fmt.Sprint(showHiddenInvitations))
	return c
}

// SingleEvents sets the optional parameter "singleEvents": Whether to
// expand recurring events into instances and only return single one-off
// events and instances of recurring events, but not the underlying
// recurring events themselves.  The default is False.
func (c *EventsListCall) SingleEvents(singleEvents bool) *EventsListCall {
	c.urlParams_.Set("singleEvents", fmt.Sprint(singleEvents))
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then. All events
// deleted since the previous list request will always be in the result
// set and it is not allowed to set showDeleted to False.
// There are several query parameters that cannot be specified together
// with nextSyncToken to ensure consistency of the client state.
//
// These are:
// - iCalUID
// - orderBy
// - privateExtendedProperty
// - q
// - sharedExtendedProperty
// - timeMin
// - timeMax
// - updatedMin If the syncToken expires, the server will respond with a
// 410 GONE response code and the client should clear its storage and
// perform a full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *EventsListCall) SyncToken(syncToken string) *EventsListCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// TimeMax sets the optional parameter "timeMax": Upper bound
// (exclusive) for an event's start time to filter by.  The default is
// not to filter by start time. Must be an RFC3339 timestamp with
// mandatory time zone offset, e.g., 2011-06-03T10:00:00-07:00,
// 2011-06-03T10:00:00Z. Milliseconds may be provided but will be
// ignored.
func (c *EventsListCall) TimeMax(timeMax string) *EventsListCall {
	c.urlParams_.Set("timeMax", timeMax)
	return c
}

// TimeMin sets the optional parameter "timeMin": Lower bound
// (inclusive) for an event's end time to filter by.  The default is not
// to filter by end time. Must be an RFC3339 timestamp with mandatory
// time zone offset, e.g., 2011-06-03T10:00:00-07:00,
// 2011-06-03T10:00:00Z. Milliseconds may be provided but will be
// ignored.
func (c *EventsListCall) TimeMin(timeMin string) *EventsListCall {
	c.urlParams_.Set("timeMin", timeMin)
	return c
}

// TimeZone sets the optional parameter "timeZone": Time zone used in
// the response.  The default is the time zone of the calendar.
func (c *EventsListCall) TimeZone(timeZone string) *EventsListCall {
	c.urlParams_.Set("timeZone", timeZone)
	return c
}

// UpdatedMin sets the optional parameter "updatedMin": Lower bound for
// an event's last modification time (as a RFC3339 timestamp) to filter
// by. When specified, entries deleted since this time will always be
// included regardless of showDeleted.  The default is not to filter by
// last modification time.
func (c *EventsListCall) UpdatedMin(updatedMin string) *EventsListCall {
	c.urlParams_.Set("updatedMin", updatedMin)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsListCall) Fields(s ...googleapi.Field) *EventsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *EventsListCall) IfNoneMatch(entityTag string) *EventsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsListCall) Context(ctx context.Context) *EventsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.list" call.
// Exactly one of *Events or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Events.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsListCall) Do(opts ...googleapi.CallOption) (*Events, error) {
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
	ret := &Events{
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
	//   "description": "Returns events on the specified calendar.",
	//   "httpMethod": "GET",
	//   "id": "calendar.events.list",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "alwaysIncludeEmail": {
	//       "description": "Whether to always include a value in the email field for the organizer, creator and attendees, even if no real email is available (i.e. a generated, non-working value will be provided). The use of this option is discouraged and should only be used by clients which cannot handle the absence of an email address value in the mentioned places. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "iCalUID": {
	//       "description": "Specifies event ID in the iCalendar format to be included in the response. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "maxResults": {
	//       "default": "250",
	//       "description": "Maximum number of events returned on one result page. By default the value is 250 events. The page size can never be larger than 2500 events. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "The order of the events returned in the result. Optional. The default is an unspecified, stable order.",
	//       "enum": [
	//         "startTime",
	//         "updated"
	//       ],
	//       "enumDescriptions": [
	//         "Order by the start date/time (ascending). This is only available when querying single events (i.e. the parameter singleEvents is True)",
	//         "Order by last modification time (ascending)."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "privateExtendedProperty": {
	//       "description": "Extended properties constraint specified as propertyName=value. Matches only private properties. This parameter might be repeated multiple times to return events that match all given constraints.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Free text search terms to find events that match these terms in any field, except for extended properties. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sharedExtendedProperty": {
	//       "description": "Extended properties constraint specified as propertyName=value. Matches only shared properties. This parameter might be repeated multiple times to return events that match all given constraints.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted events (with status equals \"cancelled\") in the result. Cancelled instances of recurring events (but not the underlying recurring event) will still be included if showDeleted and singleEvents are both False. If showDeleted and singleEvents are both True, only single instances of deleted events (but not the underlying recurring events) are returned. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "showHiddenInvitations": {
	//       "description": "Whether to include hidden invitations in the result. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "singleEvents": {
	//       "description": "Whether to expand recurring events into instances and only return single one-off events and instances of recurring events, but not the underlying recurring events themselves. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then. All events deleted since the previous list request will always be in the result set and it is not allowed to set showDeleted to False.\nThere are several query parameters that cannot be specified together with nextSyncToken to ensure consistency of the client state.\n\nThese are: \n- iCalUID \n- orderBy \n- privateExtendedProperty \n- q \n- sharedExtendedProperty \n- timeMin \n- timeMax \n- updatedMin If the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeMax": {
	//       "description": "Upper bound (exclusive) for an event's start time to filter by. Optional. The default is not to filter by start time. Must be an RFC3339 timestamp with mandatory time zone offset, e.g., 2011-06-03T10:00:00-07:00, 2011-06-03T10:00:00Z. Milliseconds may be provided but will be ignored.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeMin": {
	//       "description": "Lower bound (inclusive) for an event's end time to filter by. Optional. The default is not to filter by end time. Must be an RFC3339 timestamp with mandatory time zone offset, e.g., 2011-06-03T10:00:00-07:00, 2011-06-03T10:00:00Z. Milliseconds may be provided but will be ignored.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeZone": {
	//       "description": "Time zone used in the response. Optional. The default is the time zone of the calendar.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMin": {
	//       "description": "Lower bound for an event's last modification time (as a RFC3339 timestamp) to filter by. When specified, entries deleted since this time will always be included regardless of showDeleted. Optional. The default is not to filter by last modification time.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events",
	//   "response": {
	//     "$ref": "Events"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *EventsListCall) Pages(ctx context.Context, f func(*Events) error) error {
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

// method id "calendar.events.move":

type EventsMoveCall struct {
	s          *Service
	calendarId string
	eventId    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Move: Moves an event to another calendar, i.e. changes an event's
// organizer.
func (r *EventsService) Move(calendarId string, eventId string, destinationid string) *EventsMoveCall {
	c := &EventsMoveCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.eventId = eventId
	c.urlParams_.Set("destination", destinationid)
	return c
}

// SendNotifications sets the optional parameter "sendNotifications":
// Whether to send notifications about the change of the event's
// organizer.  The default is False.
func (c *EventsMoveCall) SendNotifications(sendNotifications bool) *EventsMoveCall {
	c.urlParams_.Set("sendNotifications", fmt.Sprint(sendNotifications))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsMoveCall) Fields(s ...googleapi.Field) *EventsMoveCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsMoveCall) Context(ctx context.Context) *EventsMoveCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsMoveCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsMoveCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/{eventId}/move")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"eventId":    c.eventId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.move" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsMoveCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Moves an event to another calendar, i.e. changes an event's organizer.",
	//   "httpMethod": "POST",
	//   "id": "calendar.events.move",
	//   "parameterOrder": [
	//     "calendarId",
	//     "eventId",
	//     "destination"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier of the source calendar where the event currently is on.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "destination": {
	//       "description": "Calendar identifier of the target calendar where the event is to be moved to.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "eventId": {
	//       "description": "Event identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "sendNotifications": {
	//       "description": "Whether to send notifications about the change of the event's organizer. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/{eventId}/move",
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.patch":

type EventsPatchCall struct {
	s          *Service
	calendarId string
	eventId    string
	event      *Event
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates an event. This method supports patch semantics.
func (r *EventsService) Patch(calendarId string, eventId string, event *Event) *EventsPatchCall {
	c := &EventsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.eventId = eventId
	c.event = event
	return c
}

// AlwaysIncludeEmail sets the optional parameter "alwaysIncludeEmail":
// Whether to always include a value in the email field for the
// organizer, creator and attendees, even if no real email is available
// (i.e. a generated, non-working value will be provided). The use of
// this option is discouraged and should only be used by clients which
// cannot handle the absence of an email address value in the mentioned
// places.  The default is False.
func (c *EventsPatchCall) AlwaysIncludeEmail(alwaysIncludeEmail bool) *EventsPatchCall {
	c.urlParams_.Set("alwaysIncludeEmail", fmt.Sprint(alwaysIncludeEmail))
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsPatchCall) MaxAttendees(maxAttendees int64) *EventsPatchCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// SendNotifications sets the optional parameter "sendNotifications":
// Whether to send notifications about the event update (e.g. attendee's
// responses, title changes, etc.).  The default is False.
func (c *EventsPatchCall) SendNotifications(sendNotifications bool) *EventsPatchCall {
	c.urlParams_.Set("sendNotifications", fmt.Sprint(sendNotifications))
	return c
}

// SupportsAttachments sets the optional parameter
// "supportsAttachments": Whether API client performing operation
// supports event attachments.  The default is False.
func (c *EventsPatchCall) SupportsAttachments(supportsAttachments bool) *EventsPatchCall {
	c.urlParams_.Set("supportsAttachments", fmt.Sprint(supportsAttachments))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsPatchCall) Fields(s ...googleapi.Field) *EventsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsPatchCall) Context(ctx context.Context) *EventsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.event)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/{eventId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"eventId":    c.eventId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.patch" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsPatchCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Updates an event. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "calendar.events.patch",
	//   "parameterOrder": [
	//     "calendarId",
	//     "eventId"
	//   ],
	//   "parameters": {
	//     "alwaysIncludeEmail": {
	//       "description": "Whether to always include a value in the email field for the organizer, creator and attendees, even if no real email is available (i.e. a generated, non-working value will be provided). The use of this option is discouraged and should only be used by clients which cannot handle the absence of an email address value in the mentioned places. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "eventId": {
	//       "description": "Event identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "sendNotifications": {
	//       "description": "Whether to send notifications about the event update (e.g. attendee's responses, title changes, etc.). Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "supportsAttachments": {
	//       "description": "Whether API client performing operation supports event attachments. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/{eventId}",
	//   "request": {
	//     "$ref": "Event"
	//   },
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.quickAdd":

type EventsQuickAddCall struct {
	s          *Service
	calendarId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// QuickAdd: Creates an event based on a simple text string.
func (r *EventsService) QuickAdd(calendarId string, text string) *EventsQuickAddCall {
	c := &EventsQuickAddCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.urlParams_.Set("text", text)
	return c
}

// SendNotifications sets the optional parameter "sendNotifications":
// Whether to send notifications about the creation of the event.  The
// default is False.
func (c *EventsQuickAddCall) SendNotifications(sendNotifications bool) *EventsQuickAddCall {
	c.urlParams_.Set("sendNotifications", fmt.Sprint(sendNotifications))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsQuickAddCall) Fields(s ...googleapi.Field) *EventsQuickAddCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsQuickAddCall) Context(ctx context.Context) *EventsQuickAddCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsQuickAddCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsQuickAddCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/quickAdd")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.quickAdd" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsQuickAddCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Creates an event based on a simple text string.",
	//   "httpMethod": "POST",
	//   "id": "calendar.events.quickAdd",
	//   "parameterOrder": [
	//     "calendarId",
	//     "text"
	//   ],
	//   "parameters": {
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "sendNotifications": {
	//       "description": "Whether to send notifications about the creation of the event. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "text": {
	//       "description": "The text describing the event to be created.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/quickAdd",
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.update":

type EventsUpdateCall struct {
	s          *Service
	calendarId string
	eventId    string
	event      *Event
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates an event.
func (r *EventsService) Update(calendarId string, eventId string, event *Event) *EventsUpdateCall {
	c := &EventsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.eventId = eventId
	c.event = event
	return c
}

// AlwaysIncludeEmail sets the optional parameter "alwaysIncludeEmail":
// Whether to always include a value in the email field for the
// organizer, creator and attendees, even if no real email is available
// (i.e. a generated, non-working value will be provided). The use of
// this option is discouraged and should only be used by clients which
// cannot handle the absence of an email address value in the mentioned
// places.  The default is False.
func (c *EventsUpdateCall) AlwaysIncludeEmail(alwaysIncludeEmail bool) *EventsUpdateCall {
	c.urlParams_.Set("alwaysIncludeEmail", fmt.Sprint(alwaysIncludeEmail))
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsUpdateCall) MaxAttendees(maxAttendees int64) *EventsUpdateCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// SendNotifications sets the optional parameter "sendNotifications":
// Whether to send notifications about the event update (e.g. attendee's
// responses, title changes, etc.).  The default is False.
func (c *EventsUpdateCall) SendNotifications(sendNotifications bool) *EventsUpdateCall {
	c.urlParams_.Set("sendNotifications", fmt.Sprint(sendNotifications))
	return c
}

// SupportsAttachments sets the optional parameter
// "supportsAttachments": Whether API client performing operation
// supports event attachments.  The default is False.
func (c *EventsUpdateCall) SupportsAttachments(supportsAttachments bool) *EventsUpdateCall {
	c.urlParams_.Set("supportsAttachments", fmt.Sprint(supportsAttachments))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsUpdateCall) Fields(s ...googleapi.Field) *EventsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsUpdateCall) Context(ctx context.Context) *EventsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.event)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/{eventId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
		"eventId":    c.eventId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.update" call.
// Exactly one of *Event or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Event.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsUpdateCall) Do(opts ...googleapi.CallOption) (*Event, error) {
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
	ret := &Event{
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
	//   "description": "Updates an event.",
	//   "httpMethod": "PUT",
	//   "id": "calendar.events.update",
	//   "parameterOrder": [
	//     "calendarId",
	//     "eventId"
	//   ],
	//   "parameters": {
	//     "alwaysIncludeEmail": {
	//       "description": "Whether to always include a value in the email field for the organizer, creator and attendees, even if no real email is available (i.e. a generated, non-working value will be provided). The use of this option is discouraged and should only be used by clients which cannot handle the absence of an email address value in the mentioned places. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "eventId": {
	//       "description": "Event identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "sendNotifications": {
	//       "description": "Whether to send notifications about the event update (e.g. attendee's responses, title changes, etc.). Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "supportsAttachments": {
	//       "description": "Whether API client performing operation supports event attachments. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/{eventId}",
	//   "request": {
	//     "$ref": "Event"
	//   },
	//   "response": {
	//     "$ref": "Event"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar"
	//   ]
	// }

}

// method id "calendar.events.watch":

type EventsWatchCall struct {
	s          *Service
	calendarId string
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Watch: Watch for changes to Events resources.
func (r *EventsService) Watch(calendarId string, channel *Channel) *EventsWatchCall {
	c := &EventsWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.calendarId = calendarId
	c.channel = channel
	return c
}

// AlwaysIncludeEmail sets the optional parameter "alwaysIncludeEmail":
// Whether to always include a value in the email field for the
// organizer, creator and attendees, even if no real email is available
// (i.e. a generated, non-working value will be provided). The use of
// this option is discouraged and should only be used by clients which
// cannot handle the absence of an email address value in the mentioned
// places.  The default is False.
func (c *EventsWatchCall) AlwaysIncludeEmail(alwaysIncludeEmail bool) *EventsWatchCall {
	c.urlParams_.Set("alwaysIncludeEmail", fmt.Sprint(alwaysIncludeEmail))
	return c
}

// ICalUID sets the optional parameter "iCalUID": Specifies event ID in
// the iCalendar format to be included in the response.
func (c *EventsWatchCall) ICalUID(iCalUID string) *EventsWatchCall {
	c.urlParams_.Set("iCalUID", iCalUID)
	return c
}

// MaxAttendees sets the optional parameter "maxAttendees": The maximum
// number of attendees to include in the response. If there are more
// than the specified number of attendees, only the participant is
// returned.
func (c *EventsWatchCall) MaxAttendees(maxAttendees int64) *EventsWatchCall {
	c.urlParams_.Set("maxAttendees", fmt.Sprint(maxAttendees))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of events returned on one result page. By default the value is 250
// events. The page size can never be larger than 2500 events.
func (c *EventsWatchCall) MaxResults(maxResults int64) *EventsWatchCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": The order of the
// events returned in the result.  The default is an unspecified, stable
// order.
//
// Possible values:
//   "startTime" - Order by the start date/time (ascending). This is
// only available when querying single events (i.e. the parameter
// singleEvents is True)
//   "updated" - Order by last modification time (ascending).
func (c *EventsWatchCall) OrderBy(orderBy string) *EventsWatchCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *EventsWatchCall) PageToken(pageToken string) *EventsWatchCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// PrivateExtendedProperty sets the optional parameter
// "privateExtendedProperty": Extended properties constraint specified
// as propertyName=value. Matches only private properties. This
// parameter might be repeated multiple times to return events that
// match all given constraints.
func (c *EventsWatchCall) PrivateExtendedProperty(privateExtendedProperty ...string) *EventsWatchCall {
	c.urlParams_.SetMulti("privateExtendedProperty", append([]string{}, privateExtendedProperty...))
	return c
}

// Q sets the optional parameter "q": Free text search terms to find
// events that match these terms in any field, except for extended
// properties.
func (c *EventsWatchCall) Q(q string) *EventsWatchCall {
	c.urlParams_.Set("q", q)
	return c
}

// SharedExtendedProperty sets the optional parameter
// "sharedExtendedProperty": Extended properties constraint specified as
// propertyName=value. Matches only shared properties. This parameter
// might be repeated multiple times to return events that match all
// given constraints.
func (c *EventsWatchCall) SharedExtendedProperty(sharedExtendedProperty ...string) *EventsWatchCall {
	c.urlParams_.SetMulti("sharedExtendedProperty", append([]string{}, sharedExtendedProperty...))
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Whether to
// include deleted events (with status equals "cancelled") in the
// result. Cancelled instances of recurring events (but not the
// underlying recurring event) will still be included if showDeleted and
// singleEvents are both False. If showDeleted and singleEvents are both
// True, only single instances of deleted events (but not the underlying
// recurring events) are returned.  The default is False.
func (c *EventsWatchCall) ShowDeleted(showDeleted bool) *EventsWatchCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// ShowHiddenInvitations sets the optional parameter
// "showHiddenInvitations": Whether to include hidden invitations in the
// result.  The default is False.
func (c *EventsWatchCall) ShowHiddenInvitations(showHiddenInvitations bool) *EventsWatchCall {
	c.urlParams_.Set("showHiddenInvitations", fmt.Sprint(showHiddenInvitations))
	return c
}

// SingleEvents sets the optional parameter "singleEvents": Whether to
// expand recurring events into instances and only return single one-off
// events and instances of recurring events, but not the underlying
// recurring events themselves.  The default is False.
func (c *EventsWatchCall) SingleEvents(singleEvents bool) *EventsWatchCall {
	c.urlParams_.Set("singleEvents", fmt.Sprint(singleEvents))
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then. All events
// deleted since the previous list request will always be in the result
// set and it is not allowed to set showDeleted to False.
// There are several query parameters that cannot be specified together
// with nextSyncToken to ensure consistency of the client state.
//
// These are:
// - iCalUID
// - orderBy
// - privateExtendedProperty
// - q
// - sharedExtendedProperty
// - timeMin
// - timeMax
// - updatedMin If the syncToken expires, the server will respond with a
// 410 GONE response code and the client should clear its storage and
// perform a full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *EventsWatchCall) SyncToken(syncToken string) *EventsWatchCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// TimeMax sets the optional parameter "timeMax": Upper bound
// (exclusive) for an event's start time to filter by.  The default is
// not to filter by start time. Must be an RFC3339 timestamp with
// mandatory time zone offset, e.g., 2011-06-03T10:00:00-07:00,
// 2011-06-03T10:00:00Z. Milliseconds may be provided but will be
// ignored.
func (c *EventsWatchCall) TimeMax(timeMax string) *EventsWatchCall {
	c.urlParams_.Set("timeMax", timeMax)
	return c
}

// TimeMin sets the optional parameter "timeMin": Lower bound
// (inclusive) for an event's end time to filter by.  The default is not
// to filter by end time. Must be an RFC3339 timestamp with mandatory
// time zone offset, e.g., 2011-06-03T10:00:00-07:00,
// 2011-06-03T10:00:00Z. Milliseconds may be provided but will be
// ignored.
func (c *EventsWatchCall) TimeMin(timeMin string) *EventsWatchCall {
	c.urlParams_.Set("timeMin", timeMin)
	return c
}

// TimeZone sets the optional parameter "timeZone": Time zone used in
// the response.  The default is the time zone of the calendar.
func (c *EventsWatchCall) TimeZone(timeZone string) *EventsWatchCall {
	c.urlParams_.Set("timeZone", timeZone)
	return c
}

// UpdatedMin sets the optional parameter "updatedMin": Lower bound for
// an event's last modification time (as a RFC3339 timestamp) to filter
// by. When specified, entries deleted since this time will always be
// included regardless of showDeleted.  The default is not to filter by
// last modification time.
func (c *EventsWatchCall) UpdatedMin(updatedMin string) *EventsWatchCall {
	c.urlParams_.Set("updatedMin", updatedMin)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *EventsWatchCall) Fields(s ...googleapi.Field) *EventsWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *EventsWatchCall) Context(ctx context.Context) *EventsWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *EventsWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *EventsWatchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "calendars/{calendarId}/events/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"calendarId": c.calendarId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.events.watch" call.
// Exactly one of *Channel or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Channel.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *EventsWatchCall) Do(opts ...googleapi.CallOption) (*Channel, error) {
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
	//   "description": "Watch for changes to Events resources.",
	//   "httpMethod": "POST",
	//   "id": "calendar.events.watch",
	//   "parameterOrder": [
	//     "calendarId"
	//   ],
	//   "parameters": {
	//     "alwaysIncludeEmail": {
	//       "description": "Whether to always include a value in the email field for the organizer, creator and attendees, even if no real email is available (i.e. a generated, non-working value will be provided). The use of this option is discouraged and should only be used by clients which cannot handle the absence of an email address value in the mentioned places. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "calendarId": {
	//       "description": "Calendar identifier. To retrieve calendar IDs call the calendarList.list method. If you want to access the primary calendar of the currently logged in user, use the \"primary\" keyword.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "iCalUID": {
	//       "description": "Specifies event ID in the iCalendar format to be included in the response. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAttendees": {
	//       "description": "The maximum number of attendees to include in the response. If there are more than the specified number of attendees, only the participant is returned. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "maxResults": {
	//       "default": "250",
	//       "description": "Maximum number of events returned on one result page. By default the value is 250 events. The page size can never be larger than 2500 events. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "The order of the events returned in the result. Optional. The default is an unspecified, stable order.",
	//       "enum": [
	//         "startTime",
	//         "updated"
	//       ],
	//       "enumDescriptions": [
	//         "Order by the start date/time (ascending). This is only available when querying single events (i.e. the parameter singleEvents is True)",
	//         "Order by last modification time (ascending)."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "privateExtendedProperty": {
	//       "description": "Extended properties constraint specified as propertyName=value. Matches only private properties. This parameter might be repeated multiple times to return events that match all given constraints.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Free text search terms to find events that match these terms in any field, except for extended properties. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "sharedExtendedProperty": {
	//       "description": "Extended properties constraint specified as propertyName=value. Matches only shared properties. This parameter might be repeated multiple times to return events that match all given constraints.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Whether to include deleted events (with status equals \"cancelled\") in the result. Cancelled instances of recurring events (but not the underlying recurring event) will still be included if showDeleted and singleEvents are both False. If showDeleted and singleEvents are both True, only single instances of deleted events (but not the underlying recurring events) are returned. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "showHiddenInvitations": {
	//       "description": "Whether to include hidden invitations in the result. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "singleEvents": {
	//       "description": "Whether to expand recurring events into instances and only return single one-off events and instances of recurring events, but not the underlying recurring events themselves. Optional. The default is False.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then. All events deleted since the previous list request will always be in the result set and it is not allowed to set showDeleted to False.\nThere are several query parameters that cannot be specified together with nextSyncToken to ensure consistency of the client state.\n\nThese are: \n- iCalUID \n- orderBy \n- privateExtendedProperty \n- q \n- sharedExtendedProperty \n- timeMin \n- timeMax \n- updatedMin If the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeMax": {
	//       "description": "Upper bound (exclusive) for an event's start time to filter by. Optional. The default is not to filter by start time. Must be an RFC3339 timestamp with mandatory time zone offset, e.g., 2011-06-03T10:00:00-07:00, 2011-06-03T10:00:00Z. Milliseconds may be provided but will be ignored.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeMin": {
	//       "description": "Lower bound (inclusive) for an event's end time to filter by. Optional. The default is not to filter by end time. Must be an RFC3339 timestamp with mandatory time zone offset, e.g., 2011-06-03T10:00:00-07:00, 2011-06-03T10:00:00Z. Milliseconds may be provided but will be ignored.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timeZone": {
	//       "description": "Time zone used in the response. Optional. The default is the time zone of the calendar.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMin": {
	//       "description": "Lower bound for an event's last modification time (as a RFC3339 timestamp) to filter by. When specified, entries deleted since this time will always be included regardless of showDeleted. Optional. The default is not to filter by last modification time.",
	//       "format": "date-time",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "calendars/{calendarId}/events/watch",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "response": {
	//     "$ref": "Channel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// method id "calendar.freebusy.query":

type FreebusyQueryCall struct {
	s               *Service
	freebusyrequest *FreeBusyRequest
	urlParams_      gensupport.URLParams
	ctx_            context.Context
	header_         http.Header
}

// Query: Returns free/busy information for a set of calendars.
func (r *FreebusyService) Query(freebusyrequest *FreeBusyRequest) *FreebusyQueryCall {
	c := &FreebusyQueryCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.freebusyrequest = freebusyrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *FreebusyQueryCall) Fields(s ...googleapi.Field) *FreebusyQueryCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *FreebusyQueryCall) Context(ctx context.Context) *FreebusyQueryCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *FreebusyQueryCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *FreebusyQueryCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.freebusyrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "freeBusy")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.freebusy.query" call.
// Exactly one of *FreeBusyResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *FreeBusyResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *FreebusyQueryCall) Do(opts ...googleapi.CallOption) (*FreeBusyResponse, error) {
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
	ret := &FreeBusyResponse{
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
	//   "description": "Returns free/busy information for a set of calendars.",
	//   "httpMethod": "POST",
	//   "id": "calendar.freebusy.query",
	//   "path": "freeBusy",
	//   "request": {
	//     "$ref": "FreeBusyRequest"
	//   },
	//   "response": {
	//     "$ref": "FreeBusyResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.settings.get":

type SettingsGetCall struct {
	s            *Service
	setting      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a single user setting.
func (r *SettingsService) Get(setting string) *SettingsGetCall {
	c := &SettingsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.setting = setting
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SettingsGetCall) Fields(s ...googleapi.Field) *SettingsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SettingsGetCall) IfNoneMatch(entityTag string) *SettingsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SettingsGetCall) Context(ctx context.Context) *SettingsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SettingsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SettingsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/settings/{setting}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"setting": c.setting,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.settings.get" call.
// Exactly one of *Setting or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Setting.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SettingsGetCall) Do(opts ...googleapi.CallOption) (*Setting, error) {
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
	ret := &Setting{
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
	//   "description": "Returns a single user setting.",
	//   "httpMethod": "GET",
	//   "id": "calendar.settings.get",
	//   "parameterOrder": [
	//     "setting"
	//   ],
	//   "parameters": {
	//     "setting": {
	//       "description": "The id of the user setting.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/settings/{setting}",
	//   "response": {
	//     "$ref": "Setting"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ]
	// }

}

// method id "calendar.settings.list":

type SettingsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns all user settings for the authenticated user.
func (r *SettingsService) List() *SettingsListCall {
	c := &SettingsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. By default the value is 100
// entries. The page size can never be larger than 250 entries.
func (c *SettingsListCall) MaxResults(maxResults int64) *SettingsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *SettingsListCall) PageToken(pageToken string) *SettingsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then.
// If the syncToken expires, the server will respond with a 410 GONE
// response code and the client should clear its storage and perform a
// full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *SettingsListCall) SyncToken(syncToken string) *SettingsListCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SettingsListCall) Fields(s ...googleapi.Field) *SettingsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SettingsListCall) IfNoneMatch(entityTag string) *SettingsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SettingsListCall) Context(ctx context.Context) *SettingsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SettingsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SettingsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/settings")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.settings.list" call.
// Exactly one of *Settings or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Settings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *SettingsListCall) Do(opts ...googleapi.CallOption) (*Settings, error) {
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
	ret := &Settings{
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
	//   "description": "Returns all user settings for the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "calendar.settings.list",
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. By default the value is 100 entries. The page size can never be larger than 250 entries. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then.\nIf the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/settings",
	//   "response": {
	//     "$ref": "Settings"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *SettingsListCall) Pages(ctx context.Context, f func(*Settings) error) error {
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

// method id "calendar.settings.watch":

type SettingsWatchCall struct {
	s          *Service
	channel    *Channel
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Watch: Watch for changes to Settings resources.
func (r *SettingsService) Watch(channel *Channel) *SettingsWatchCall {
	c := &SettingsWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.channel = channel
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. By default the value is 100
// entries. The page size can never be larger than 250 entries.
func (c *SettingsWatchCall) MaxResults(maxResults int64) *SettingsWatchCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Token specifying
// which result page to return.
func (c *SettingsWatchCall) PageToken(pageToken string) *SettingsWatchCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// SyncToken sets the optional parameter "syncToken": Token obtained
// from the nextSyncToken field returned on the last page of results
// from the previous list request. It makes the result of this list
// request contain only entries that have changed since then.
// If the syncToken expires, the server will respond with a 410 GONE
// response code and the client should clear its storage and perform a
// full synchronization without any syncToken.
// Learn more about incremental synchronization.
//  The default is to return all entries.
func (c *SettingsWatchCall) SyncToken(syncToken string) *SettingsWatchCall {
	c.urlParams_.Set("syncToken", syncToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SettingsWatchCall) Fields(s ...googleapi.Field) *SettingsWatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SettingsWatchCall) Context(ctx context.Context) *SettingsWatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SettingsWatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SettingsWatchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/me/settings/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "calendar.settings.watch" call.
// Exactly one of *Channel or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Channel.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SettingsWatchCall) Do(opts ...googleapi.CallOption) (*Channel, error) {
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
	//   "description": "Watch for changes to Settings resources.",
	//   "httpMethod": "POST",
	//   "id": "calendar.settings.watch",
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. By default the value is 100 entries. The page size can never be larger than 250 entries. Optional.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "syncToken": {
	//       "description": "Token obtained from the nextSyncToken field returned on the last page of results from the previous list request. It makes the result of this list request contain only entries that have changed since then.\nIf the syncToken expires, the server will respond with a 410 GONE response code and the client should clear its storage and perform a full synchronization without any syncToken.\nLearn more about incremental synchronization.\nOptional. The default is to return all entries.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/me/settings/watch",
	//   "request": {
	//     "$ref": "Channel",
	//     "parameterName": "resource"
	//   },
	//   "response": {
	//     "$ref": "Channel"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/calendar",
	//     "https://www.googleapis.com/auth/calendar.readonly"
	//   ],
	//   "supportsSubscription": true
	// }

}

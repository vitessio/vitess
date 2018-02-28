// Package mirror provides access to the Google Mirror API.
//
// See https://developers.google.com/glass
//
// Usage example:
//
//   import "google.golang.org/api/mirror/v1"
//   ...
//   mirrorService, err := mirror.New(oauthHttpClient)
package mirror // import "google.golang.org/api/mirror/v1"

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

const apiId = "mirror:v1"
const apiName = "mirror"
const apiVersion = "v1"
const basePath = "https://www.googleapis.com/mirror/v1/"

// OAuth2 scopes used by this API.
const (
	// View your location
	GlassLocationScope = "https://www.googleapis.com/auth/glass.location"

	// View and manage your Glass timeline
	GlassTimelineScope = "https://www.googleapis.com/auth/glass.timeline"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Accounts = NewAccountsService(s)
	s.Contacts = NewContactsService(s)
	s.Locations = NewLocationsService(s)
	s.Settings = NewSettingsService(s)
	s.Subscriptions = NewSubscriptionsService(s)
	s.Timeline = NewTimelineService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Accounts *AccountsService

	Contacts *ContactsService

	Locations *LocationsService

	Settings *SettingsService

	Subscriptions *SubscriptionsService

	Timeline *TimelineService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewAccountsService(s *Service) *AccountsService {
	rs := &AccountsService{s: s}
	return rs
}

type AccountsService struct {
	s *Service
}

func NewContactsService(s *Service) *ContactsService {
	rs := &ContactsService{s: s}
	return rs
}

type ContactsService struct {
	s *Service
}

func NewLocationsService(s *Service) *LocationsService {
	rs := &LocationsService{s: s}
	return rs
}

type LocationsService struct {
	s *Service
}

func NewSettingsService(s *Service) *SettingsService {
	rs := &SettingsService{s: s}
	return rs
}

type SettingsService struct {
	s *Service
}

func NewSubscriptionsService(s *Service) *SubscriptionsService {
	rs := &SubscriptionsService{s: s}
	return rs
}

type SubscriptionsService struct {
	s *Service
}

func NewTimelineService(s *Service) *TimelineService {
	rs := &TimelineService{s: s}
	rs.Attachments = NewTimelineAttachmentsService(s)
	return rs
}

type TimelineService struct {
	s *Service

	Attachments *TimelineAttachmentsService
}

func NewTimelineAttachmentsService(s *Service) *TimelineAttachmentsService {
	rs := &TimelineAttachmentsService{s: s}
	return rs
}

type TimelineAttachmentsService struct {
	s *Service
}

// Account: Represents an account passed into the Account Manager on
// Glass.
type Account struct {
	AuthTokens []*AuthToken `json:"authTokens,omitempty"`

	Features []string `json:"features,omitempty"`

	Password string `json:"password,omitempty"`

	UserData []*UserData `json:"userData,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AuthTokens") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuthTokens") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Account) MarshalJSON() ([]byte, error) {
	type noMethod Account
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Attachment: Represents media content, such as a photo, that can be
// attached to a timeline item.
type Attachment struct {
	// ContentType: The MIME type of the attachment.
	ContentType string `json:"contentType,omitempty"`

	// ContentUrl: The URL for the content.
	ContentUrl string `json:"contentUrl,omitempty"`

	// Id: The ID of the attachment.
	Id string `json:"id,omitempty"`

	// IsProcessingContent: Indicates that the contentUrl is not available
	// because the attachment content is still being processed. If the
	// caller wishes to retrieve the content, it should try again later.
	IsProcessingContent bool `json:"isProcessingContent,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *Attachment) MarshalJSON() ([]byte, error) {
	type noMethod Attachment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AttachmentsListResponse: A list of Attachments. This is the response
// from the server to GET requests on the attachments collection.
type AttachmentsListResponse struct {
	// Items: The list of attachments.
	Items []*Attachment `json:"items,omitempty"`

	// Kind: The type of resource. This is always mirror#attachmentsList.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AttachmentsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod AttachmentsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AuthToken struct {
	AuthToken string `json:"authToken,omitempty"`

	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AuthToken") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuthToken") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AuthToken) MarshalJSON() ([]byte, error) {
	type noMethod AuthToken
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Command: A single menu command that is part of a Contact.
type Command struct {
	// Type: The type of operation this command corresponds to. Allowed
	// values are:
	// - TAKE_A_NOTE - Shares a timeline item with the transcription of user
	// speech from the "Take a note" voice menu command.
	// - POST_AN_UPDATE - Shares a timeline item with the transcription of
	// user speech from the "Post an update" voice menu command.
	Type string `json:"type,omitempty"`

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

func (s *Command) MarshalJSON() ([]byte, error) {
	type noMethod Command
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Contact: A person or group that can be used as a creator or a
// contact.
type Contact struct {
	// AcceptCommands: A list of voice menu commands that a contact can
	// handle. Glass shows up to three contacts for each voice menu command.
	// If there are more than that, the three contacts with the highest
	// priority are shown for that particular command.
	AcceptCommands []*Command `json:"acceptCommands,omitempty"`

	// AcceptTypes: A list of MIME types that a contact supports. The
	// contact will be shown to the user if any of its acceptTypes matches
	// any of the types of the attachments on the item. If no acceptTypes
	// are given, the contact will be shown for all items.
	AcceptTypes []string `json:"acceptTypes,omitempty"`

	// DisplayName: The name to display for this contact.
	DisplayName string `json:"displayName,omitempty"`

	// Id: An ID for this contact. This is generated by the application and
	// is treated as an opaque token.
	Id string `json:"id,omitempty"`

	// ImageUrls: Set of image URLs to display for a contact. Most contacts
	// will have a single image, but a "group" contact may include up to 8
	// image URLs and they will be resized and cropped into a mosaic on the
	// client.
	ImageUrls []string `json:"imageUrls,omitempty"`

	// Kind: The type of resource. This is always mirror#contact.
	Kind string `json:"kind,omitempty"`

	// PhoneNumber: Primary phone number for the contact. This can be a
	// fully-qualified number, with country calling code and area code, or a
	// local number.
	PhoneNumber string `json:"phoneNumber,omitempty"`

	// Priority: Priority for the contact to determine ordering in a list of
	// contacts. Contacts with higher priorities will be shown before ones
	// with lower priorities.
	Priority int64 `json:"priority,omitempty"`

	// SharingFeatures: A list of sharing features that a contact can
	// handle. Allowed values are:
	// - ADD_CAPTION
	SharingFeatures []string `json:"sharingFeatures,omitempty"`

	// Source: The ID of the application that created this contact. This is
	// populated by the API
	Source string `json:"source,omitempty"`

	// SpeakableName: Name of this contact as it should be pronounced. If
	// this contact's name must be spoken as part of a voice disambiguation
	// menu, this name is used as the expected pronunciation. This is useful
	// for contact names with unpronounceable characters or whose display
	// spelling is otherwise not phonetic.
	SpeakableName string `json:"speakableName,omitempty"`

	// Type: The type for this contact. This is used for sorting in UIs.
	// Allowed values are:
	// - INDIVIDUAL - Represents a single person. This is the default.
	// - GROUP - Represents more than a single person.
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AcceptCommands") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AcceptCommands") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Contact) MarshalJSON() ([]byte, error) {
	type noMethod Contact
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ContactsListResponse: A list of Contacts representing contacts. This
// is the response from the server to GET requests on the contacts
// collection.
type ContactsListResponse struct {
	// Items: Contact list.
	Items []*Contact `json:"items,omitempty"`

	// Kind: The type of resource. This is always mirror#contacts.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ContactsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod ContactsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Location: A geographic location that can be associated with a
// timeline item.
type Location struct {
	// Accuracy: The accuracy of the location fix in meters.
	Accuracy float64 `json:"accuracy,omitempty"`

	// Address: The full address of the location.
	Address string `json:"address,omitempty"`

	// DisplayName: The name to be displayed. This may be a business name or
	// a user-defined place, such as "Home".
	DisplayName string `json:"displayName,omitempty"`

	// Id: The ID of the location.
	Id string `json:"id,omitempty"`

	// Kind: The type of resource. This is always mirror#location.
	Kind string `json:"kind,omitempty"`

	// Latitude: The latitude, in degrees.
	Latitude float64 `json:"latitude,omitempty"`

	// Longitude: The longitude, in degrees.
	Longitude float64 `json:"longitude,omitempty"`

	// Timestamp: The time at which this location was captured, formatted
	// according to RFC 3339.
	Timestamp string `json:"timestamp,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Accuracy") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Accuracy") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Location) MarshalJSON() ([]byte, error) {
	type noMethod Location
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LocationsListResponse: A list of Locations. This is the response from
// the server to GET requests on the locations collection.
type LocationsListResponse struct {
	// Items: The list of locations.
	Items []*Location `json:"items,omitempty"`

	// Kind: The type of resource. This is always mirror#locationsList.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LocationsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod LocationsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MenuItem: A custom menu item that can be presented to the user by a
// timeline item.
type MenuItem struct {
	// Action: Controls the behavior when the user picks the menu option.
	// Allowed values are:
	// - CUSTOM - Custom action set by the service. When the user selects
	// this menuItem, the API triggers a notification to your callbackUrl
	// with the userActions.type set to CUSTOM and the userActions.payload
	// set to the ID of this menu item. This is the default value.
	// - Built-in actions:
	// - REPLY - Initiate a reply to the timeline item using the voice
	// recording UI. The creator attribute must be set in the timeline item
	// for this menu to be available.
	// - REPLY_ALL - Same behavior as REPLY. The original timeline item's
	// recipients will be added to the reply item.
	// - DELETE - Delete the timeline item.
	// - SHARE - Share the timeline item with the available contacts.
	// - READ_ALOUD - Read the timeline item's speakableText aloud; if this
	// field is not set, read the text field; if none of those fields are
	// set, this menu item is ignored.
	// - GET_MEDIA_INPUT - Allow users to provide media payloads to
	// Glassware from a menu item (currently, only transcribed text from
	// voice input is supported). Subscribe to notifications when users
	// invoke this menu item to receive the timeline item ID. Retrieve the
	// media from the timeline item in the payload property.
	// - VOICE_CALL - Initiate a phone call using the timeline item's
	// creator.phoneNumber attribute as recipient.
	// - NAVIGATE - Navigate to the timeline item's location.
	// - TOGGLE_PINNED - Toggle the isPinned state of the timeline item.
	// - OPEN_URI - Open the payload of the menu item in the browser.
	// - PLAY_VIDEO - Open the payload of the menu item in the Glass video
	// player.
	// - SEND_MESSAGE - Initiate sending a message to the timeline item's
	// creator:
	// - If the creator.phoneNumber is set and Glass is connected to an
	// Android phone, the message is an SMS.
	// - Otherwise, if the creator.email is set, the message is an email.
	Action string `json:"action,omitempty"`

	// ContextualCommand: The ContextualMenus.Command associated with this
	// MenuItem (e.g. READ_ALOUD). The voice label for this command will be
	// displayed in the voice menu and the touch label will be displayed in
	// the touch menu. Note that the default menu value's display name will
	// be overriden if you specify this property. Values that do not
	// correspond to a ContextualMenus.Command name will be ignored.
	ContextualCommand string `json:"contextual_command,omitempty"`

	// Id: The ID for this menu item. This is generated by the application
	// and is treated as an opaque token.
	Id string `json:"id,omitempty"`

	// Payload: A generic payload whose meaning changes depending on this
	// MenuItem's action.
	// - When the action is OPEN_URI, the payload is the URL of the website
	// to view.
	// - When the action is PLAY_VIDEO, the payload is the streaming URL of
	// the video
	// - When the action is GET_MEDIA_INPUT, the payload is the text
	// transcription of a user's speech input
	Payload string `json:"payload,omitempty"`

	// RemoveWhenSelected: If set to true on a CUSTOM menu item, that item
	// will be removed from the menu after it is selected.
	RemoveWhenSelected bool `json:"removeWhenSelected,omitempty"`

	// Values: For CUSTOM items, a list of values controlling the appearance
	// of the menu item in each of its states. A value for the DEFAULT state
	// must be provided. If the PENDING or CONFIRMED states are missing,
	// they will not be shown.
	Values []*MenuValue `json:"values,omitempty"`

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

func (s *MenuItem) MarshalJSON() ([]byte, error) {
	type noMethod MenuItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MenuValue: A single value that is part of a MenuItem.
type MenuValue struct {
	// DisplayName: The name to display for the menu item. If you specify
	// this property for a built-in menu item, the default contextual voice
	// command for that menu item is not shown.
	DisplayName string `json:"displayName,omitempty"`

	// IconUrl: URL of an icon to display with the menu item.
	IconUrl string `json:"iconUrl,omitempty"`

	// State: The state that this value applies to. Allowed values are:
	// - DEFAULT - Default value shown when displayed in the menuItems list.
	//
	// - PENDING - Value shown when the menuItem has been selected by the
	// user but can still be cancelled.
	// - CONFIRMED - Value shown when the menuItem has been selected by the
	// user and can no longer be cancelled.
	State string `json:"state,omitempty"`

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

func (s *MenuValue) MarshalJSON() ([]byte, error) {
	type noMethod MenuValue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Notification: A notification delivered by the API.
type Notification struct {
	// Collection: The collection that generated the notification.
	Collection string `json:"collection,omitempty"`

	// ItemId: The ID of the item that generated the notification.
	ItemId string `json:"itemId,omitempty"`

	// Operation: The type of operation that generated the notification.
	Operation string `json:"operation,omitempty"`

	// UserActions: A list of actions taken by the user that triggered the
	// notification.
	UserActions []*UserAction `json:"userActions,omitempty"`

	// UserToken: The user token provided by the service when it subscribed
	// for notifications.
	UserToken string `json:"userToken,omitempty"`

	// VerifyToken: The secret verify token provided by the service when it
	// subscribed for notifications.
	VerifyToken string `json:"verifyToken,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Collection") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Collection") to include in
	// API requests with the JSON null value. By default, fields with empty
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

// NotificationConfig: Controls how notifications for a timeline item
// are presented to the user.
type NotificationConfig struct {
	// DeliveryTime: The time at which the notification should be delivered.
	DeliveryTime string `json:"deliveryTime,omitempty"`

	// Level: Describes how important the notification is. Allowed values
	// are:
	// - DEFAULT - Notifications of default importance. A chime will be
	// played to alert users.
	Level string `json:"level,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeliveryTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeliveryTime") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NotificationConfig) MarshalJSON() ([]byte, error) {
	type noMethod NotificationConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Setting: A setting for Glass.
type Setting struct {
	// Id: The setting's ID. The following IDs are valid:
	// - locale - The key to the user’s language/locale (BCP 47
	// identifier) that Glassware should use to render localized content.
	//
	// - timezone - The key to the user’s current time zone region as
	// defined in the tz database. Example: America/Los_Angeles.
	Id string `json:"id,omitempty"`

	// Kind: The type of resource. This is always mirror#setting.
	Kind string `json:"kind,omitempty"`

	// Value: The setting value, as a string.
	Value string `json:"value,omitempty"`

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

func (s *Setting) MarshalJSON() ([]byte, error) {
	type noMethod Setting
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Subscription: A subscription to events on a collection.
type Subscription struct {
	// CallbackUrl: The URL where notifications should be delivered (must
	// start with https://).
	CallbackUrl string `json:"callbackUrl,omitempty"`

	// Collection: The collection to subscribe to. Allowed values are:
	// - timeline - Changes in the timeline including insertion, deletion,
	// and updates.
	// - locations - Location updates.
	// - settings - Settings updates.
	Collection string `json:"collection,omitempty"`

	// Id: The ID of the subscription.
	Id string `json:"id,omitempty"`

	// Kind: The type of resource. This is always mirror#subscription.
	Kind string `json:"kind,omitempty"`

	// Notification: Container object for notifications. This is not
	// populated in the Subscription resource.
	Notification *Notification `json:"notification,omitempty"`

	// Operation: A list of operations that should be subscribed to. An
	// empty list indicates that all operations on the collection should be
	// subscribed to. Allowed values are:
	// - UPDATE - The item has been updated.
	// - INSERT - A new item has been inserted.
	// - DELETE - The item has been deleted.
	// - MENU_ACTION - A custom menu item has been triggered by the user.
	Operation []string `json:"operation,omitempty"`

	// Updated: The time at which this subscription was last modified,
	// formatted according to RFC 3339.
	Updated string `json:"updated,omitempty"`

	// UserToken: An opaque token sent to the subscriber in notifications so
	// that it can determine the ID of the user.
	UserToken string `json:"userToken,omitempty"`

	// VerifyToken: A secret token sent to the subscriber in notifications
	// so that it can verify that the notification was generated by Google.
	VerifyToken string `json:"verifyToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CallbackUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CallbackUrl") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Subscription) MarshalJSON() ([]byte, error) {
	type noMethod Subscription
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SubscriptionsListResponse: A list of Subscriptions. This is the
// response from the server to GET requests on the subscription
// collection.
type SubscriptionsListResponse struct {
	// Items: The list of subscriptions.
	Items []*Subscription `json:"items,omitempty"`

	// Kind: The type of resource. This is always mirror#subscriptionsList.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SubscriptionsListResponse) MarshalJSON() ([]byte, error) {
	type noMethod SubscriptionsListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TimelineItem: Each item in the user's timeline is represented as a
// TimelineItem JSON structure, described below.
type TimelineItem struct {
	// Attachments: A list of media attachments associated with this item.
	// As a convenience, you can refer to attachments in your HTML payloads
	// with the attachment or cid scheme. For example:
	// - attachment: <img src="attachment:attachment_index"> where
	// attachment_index is the 0-based index of this array.
	// - cid: <img src="cid:attachment_id"> where attachment_id is the ID of
	// the attachment.
	Attachments []*Attachment `json:"attachments,omitempty"`

	// BundleId: The bundle ID for this item. Services can specify a
	// bundleId to group many items together. They appear under a single
	// top-level item on the device.
	BundleId string `json:"bundleId,omitempty"`

	// CanonicalUrl: A canonical URL pointing to the canonical/high quality
	// version of the data represented by the timeline item.
	CanonicalUrl string `json:"canonicalUrl,omitempty"`

	// Created: The time at which this item was created, formatted according
	// to RFC 3339.
	Created string `json:"created,omitempty"`

	// Creator: The user or group that created this item.
	Creator *Contact `json:"creator,omitempty"`

	// DisplayTime: The time that should be displayed when this item is
	// viewed in the timeline, formatted according to RFC 3339. This user's
	// timeline is sorted chronologically on display time, so this will also
	// determine where the item is displayed in the timeline. If not set by
	// the service, the display time defaults to the updated time.
	DisplayTime string `json:"displayTime,omitempty"`

	// Etag: ETag for this item.
	Etag string `json:"etag,omitempty"`

	// Html: HTML content for this item. If both text and html are provided
	// for an item, the html will be rendered in the timeline.
	// Allowed HTML elements - You can use these elements in your timeline
	// cards.
	//
	// - Headers: h1, h2, h3, h4, h5, h6
	// - Images: img
	// - Lists: li, ol, ul
	// - HTML5 semantics: article, aside, details, figure, figcaption,
	// footer, header, nav, section, summary, time
	// - Structural: blockquote, br, div, hr, p, span
	// - Style: b, big, center, em, i, u, s, small, strike, strong, style,
	// sub, sup
	// - Tables: table, tbody, td, tfoot, th, thead, tr
	// Blocked HTML elements: These elements and their contents are removed
	// from HTML payloads.
	//
	// - Document headers: head, title
	// - Embeds: audio, embed, object, source, video
	// - Frames: frame, frameset
	// - Scripting: applet, script
	// Other elements: Any elements that aren't listed are removed, but
	// their contents are preserved.
	Html string `json:"html,omitempty"`

	// Id: The ID of the timeline item. This is unique within a user's
	// timeline.
	Id string `json:"id,omitempty"`

	// InReplyTo: If this item was generated as a reply to another item,
	// this field will be set to the ID of the item being replied to. This
	// can be used to attach a reply to the appropriate conversation or
	// post.
	InReplyTo string `json:"inReplyTo,omitempty"`

	// IsBundleCover: Whether this item is a bundle cover.
	//
	// If an item is marked as a bundle cover, it will be the entry point to
	// the bundle of items that have the same bundleId as that item. It will
	// be shown only on the main timeline — not within the opened
	// bundle.
	//
	// On the main timeline, items that are shown are:
	// - Items that have isBundleCover set to true
	// - Items that do not have a bundleId  In a bundle sub-timeline, items
	// that are shown are:
	// - Items that have the bundleId in question AND isBundleCover set to
	// false
	IsBundleCover bool `json:"isBundleCover,omitempty"`

	// IsDeleted: When true, indicates this item is deleted, and only the ID
	// property is set.
	IsDeleted bool `json:"isDeleted,omitempty"`

	// IsPinned: When true, indicates this item is pinned, which means it's
	// grouped alongside "active" items like navigation and hangouts, on the
	// opposite side of the home screen from historical (non-pinned)
	// timeline items. You can allow the user to toggle the value of this
	// property with the TOGGLE_PINNED built-in menu item.
	IsPinned bool `json:"isPinned,omitempty"`

	// Kind: The type of resource. This is always mirror#timelineItem.
	Kind string `json:"kind,omitempty"`

	// Location: The geographic location associated with this item.
	Location *Location `json:"location,omitempty"`

	// MenuItems: A list of menu items that will be presented to the user
	// when this item is selected in the timeline.
	MenuItems []*MenuItem `json:"menuItems,omitempty"`

	// Notification: Controls how notifications for this item are presented
	// on the device. If this is missing, no notification will be generated.
	Notification *NotificationConfig `json:"notification,omitempty"`

	// PinScore: For pinned items, this determines the order in which the
	// item is displayed in the timeline, with a higher score appearing
	// closer to the clock. Note: setting this field is currently not
	// supported.
	PinScore int64 `json:"pinScore,omitempty"`

	// Recipients: A list of users or groups that this item has been shared
	// with.
	Recipients []*Contact `json:"recipients,omitempty"`

	// SelfLink: A URL that can be used to retrieve this item.
	SelfLink string `json:"selfLink,omitempty"`

	// SourceItemId: Opaque string you can use to map a timeline item to
	// data in your own service.
	SourceItemId string `json:"sourceItemId,omitempty"`

	// SpeakableText: The speakable version of the content of this item.
	// Along with the READ_ALOUD menu item, use this field to provide text
	// that would be clearer when read aloud, or to provide extended
	// information to what is displayed visually on Glass.
	//
	// Glassware should also specify the speakableType field, which will be
	// spoken before this text in cases where the additional context is
	// useful, for example when the user requests that the item be read
	// aloud following a notification.
	SpeakableText string `json:"speakableText,omitempty"`

	// SpeakableType: A speakable description of the type of this item. This
	// will be announced to the user prior to reading the content of the
	// item in cases where the additional context is useful, for example
	// when the user requests that the item be read aloud following a
	// notification.
	//
	// This should be a short, simple noun phrase such as "Email", "Text
	// message", or "Daily Planet News Update".
	//
	// Glassware are encouraged to populate this field for every timeline
	// item, even if the item does not contain speakableText or text so that
	// the user can learn the type of the item without looking at the
	// screen.
	SpeakableType string `json:"speakableType,omitempty"`

	// Text: Text content of this item.
	Text string `json:"text,omitempty"`

	// Title: The title of this item.
	Title string `json:"title,omitempty"`

	// Updated: The time at which this item was last modified, formatted
	// according to RFC 3339.
	Updated string `json:"updated,omitempty"`

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

func (s *TimelineItem) MarshalJSON() ([]byte, error) {
	type noMethod TimelineItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TimelineListResponse: A list of timeline items. This is the response
// from the server to GET requests on the timeline collection.
type TimelineListResponse struct {
	// Items: Items in the timeline.
	Items []*TimelineItem `json:"items,omitempty"`

	// Kind: The type of resource. This is always mirror#timeline.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The next page token. Provide this as the pageToken
	// parameter in the request to retrieve the next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TimelineListResponse) MarshalJSON() ([]byte, error) {
	type noMethod TimelineListResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserAction: Represents an action taken by the user that triggered a
// notification.
type UserAction struct {
	// Payload: An optional payload for the action.
	//
	// For actions of type CUSTOM, this is the ID of the custom menu item
	// that was selected.
	Payload string `json:"payload,omitempty"`

	// Type: The type of action. The value of this can be:
	// - SHARE - the user shared an item.
	// - REPLY - the user replied to an item.
	// - REPLY_ALL - the user replied to all recipients of an item.
	// - CUSTOM - the user selected a custom menu item on the timeline item.
	//
	// - DELETE - the user deleted the item.
	// - PIN - the user pinned the item.
	// - UNPIN - the user unpinned the item.
	// - LAUNCH - the user initiated a voice command.  In the future,
	// additional types may be added. UserActions with unrecognized types
	// should be ignored.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Payload") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Payload") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserAction) MarshalJSON() ([]byte, error) {
	type noMethod UserAction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UserData struct {
	Key string `json:"key,omitempty"`

	Value string `json:"value,omitempty"`

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

func (s *UserData) MarshalJSON() ([]byte, error) {
	type noMethod UserData
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "mirror.accounts.insert":

type AccountsInsertCall struct {
	s           *Service
	userToken   string
	accountType string
	accountName string
	account     *Account
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Insert: Inserts a new account for a user
func (r *AccountsService) Insert(userToken string, accountType string, accountName string, account *Account) *AccountsInsertCall {
	c := &AccountsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userToken = userToken
	c.accountType = accountType
	c.accountName = accountName
	c.account = account
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsInsertCall) Fields(s ...googleapi.Field) *AccountsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsInsertCall) Context(ctx context.Context) *AccountsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.account)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "accounts/{userToken}/{accountType}/{accountName}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userToken":   c.userToken,
		"accountType": c.accountType,
		"accountName": c.accountName,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.accounts.insert" call.
// Exactly one of *Account or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Account.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AccountsInsertCall) Do(opts ...googleapi.CallOption) (*Account, error) {
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
	ret := &Account{
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
	//   "description": "Inserts a new account for a user",
	//   "httpMethod": "POST",
	//   "id": "mirror.accounts.insert",
	//   "parameterOrder": [
	//     "userToken",
	//     "accountType",
	//     "accountName"
	//   ],
	//   "parameters": {
	//     "accountName": {
	//       "description": "The name of the account to be passed to the Android Account Manager.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "accountType": {
	//       "description": "Account type to be passed to Android Account Manager.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userToken": {
	//       "description": "The ID for the user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "accounts/{userToken}/{accountType}/{accountName}",
	//   "request": {
	//     "$ref": "Account"
	//   },
	//   "response": {
	//     "$ref": "Account"
	//   }
	// }

}

// method id "mirror.contacts.delete":

type ContactsDeleteCall struct {
	s          *Service
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a contact.
func (r *ContactsService) Delete(id string) *ContactsDeleteCall {
	c := &ContactsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ContactsDeleteCall) Fields(s ...googleapi.Field) *ContactsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ContactsDeleteCall) Context(ctx context.Context) *ContactsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ContactsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ContactsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "contacts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.contacts.delete" call.
func (c *ContactsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a contact.",
	//   "httpMethod": "DELETE",
	//   "id": "mirror.contacts.delete",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the contact.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "contacts/{id}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.contacts.get":

type ContactsGetCall struct {
	s            *Service
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a single contact by ID.
func (r *ContactsService) Get(id string) *ContactsGetCall {
	c := &ContactsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ContactsGetCall) Fields(s ...googleapi.Field) *ContactsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ContactsGetCall) IfNoneMatch(entityTag string) *ContactsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ContactsGetCall) Context(ctx context.Context) *ContactsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ContactsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ContactsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "contacts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.contacts.get" call.
// Exactly one of *Contact or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Contact.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ContactsGetCall) Do(opts ...googleapi.CallOption) (*Contact, error) {
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
	ret := &Contact{
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
	//   "description": "Gets a single contact by ID.",
	//   "httpMethod": "GET",
	//   "id": "mirror.contacts.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the contact.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "contacts/{id}",
	//   "response": {
	//     "$ref": "Contact"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.contacts.insert":

type ContactsInsertCall struct {
	s          *Service
	contact    *Contact
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Inserts a new contact.
func (r *ContactsService) Insert(contact *Contact) *ContactsInsertCall {
	c := &ContactsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.contact = contact
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ContactsInsertCall) Fields(s ...googleapi.Field) *ContactsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ContactsInsertCall) Context(ctx context.Context) *ContactsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ContactsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ContactsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.contact)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "contacts")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.contacts.insert" call.
// Exactly one of *Contact or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Contact.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ContactsInsertCall) Do(opts ...googleapi.CallOption) (*Contact, error) {
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
	ret := &Contact{
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
	//   "description": "Inserts a new contact.",
	//   "httpMethod": "POST",
	//   "id": "mirror.contacts.insert",
	//   "path": "contacts",
	//   "request": {
	//     "$ref": "Contact"
	//   },
	//   "response": {
	//     "$ref": "Contact"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.contacts.list":

type ContactsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of contacts for the authenticated user.
func (r *ContactsService) List() *ContactsListCall {
	c := &ContactsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ContactsListCall) Fields(s ...googleapi.Field) *ContactsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ContactsListCall) IfNoneMatch(entityTag string) *ContactsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ContactsListCall) Context(ctx context.Context) *ContactsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ContactsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ContactsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "contacts")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.contacts.list" call.
// Exactly one of *ContactsListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ContactsListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ContactsListCall) Do(opts ...googleapi.CallOption) (*ContactsListResponse, error) {
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
	ret := &ContactsListResponse{
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
	//   "description": "Retrieves a list of contacts for the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "mirror.contacts.list",
	//   "path": "contacts",
	//   "response": {
	//     "$ref": "ContactsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.contacts.patch":

type ContactsPatchCall struct {
	s          *Service
	id         string
	contact    *Contact
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates a contact in place. This method supports patch
// semantics.
func (r *ContactsService) Patch(id string, contact *Contact) *ContactsPatchCall {
	c := &ContactsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.contact = contact
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ContactsPatchCall) Fields(s ...googleapi.Field) *ContactsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ContactsPatchCall) Context(ctx context.Context) *ContactsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ContactsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ContactsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.contact)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "contacts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.contacts.patch" call.
// Exactly one of *Contact or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Contact.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ContactsPatchCall) Do(opts ...googleapi.CallOption) (*Contact, error) {
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
	ret := &Contact{
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
	//   "description": "Updates a contact in place. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "mirror.contacts.patch",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the contact.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "contacts/{id}",
	//   "request": {
	//     "$ref": "Contact"
	//   },
	//   "response": {
	//     "$ref": "Contact"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.contacts.update":

type ContactsUpdateCall struct {
	s          *Service
	id         string
	contact    *Contact
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates a contact in place.
func (r *ContactsService) Update(id string, contact *Contact) *ContactsUpdateCall {
	c := &ContactsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.contact = contact
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ContactsUpdateCall) Fields(s ...googleapi.Field) *ContactsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ContactsUpdateCall) Context(ctx context.Context) *ContactsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ContactsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ContactsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.contact)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "contacts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.contacts.update" call.
// Exactly one of *Contact or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Contact.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ContactsUpdateCall) Do(opts ...googleapi.CallOption) (*Contact, error) {
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
	ret := &Contact{
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
	//   "description": "Updates a contact in place.",
	//   "httpMethod": "PUT",
	//   "id": "mirror.contacts.update",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the contact.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "contacts/{id}",
	//   "request": {
	//     "$ref": "Contact"
	//   },
	//   "response": {
	//     "$ref": "Contact"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.locations.get":

type LocationsGetCall struct {
	s            *Service
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a single location by ID.
func (r *LocationsService) Get(id string) *LocationsGetCall {
	c := &LocationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LocationsGetCall) Fields(s ...googleapi.Field) *LocationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LocationsGetCall) IfNoneMatch(entityTag string) *LocationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LocationsGetCall) Context(ctx context.Context) *LocationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LocationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LocationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "locations/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.locations.get" call.
// Exactly one of *Location or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Location.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *LocationsGetCall) Do(opts ...googleapi.CallOption) (*Location, error) {
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
	ret := &Location{
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
	//   "description": "Gets a single location by ID.",
	//   "httpMethod": "GET",
	//   "id": "mirror.locations.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the location or latest for the last known location.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "locations/{id}",
	//   "response": {
	//     "$ref": "Location"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.locations.list":

type LocationsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of locations for the user.
func (r *LocationsService) List() *LocationsListCall {
	c := &LocationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LocationsListCall) Fields(s ...googleapi.Field) *LocationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LocationsListCall) IfNoneMatch(entityTag string) *LocationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LocationsListCall) Context(ctx context.Context) *LocationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LocationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LocationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "locations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.locations.list" call.
// Exactly one of *LocationsListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *LocationsListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *LocationsListCall) Do(opts ...googleapi.CallOption) (*LocationsListResponse, error) {
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
	ret := &LocationsListResponse{
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
	//   "description": "Retrieves a list of locations for the user.",
	//   "httpMethod": "GET",
	//   "id": "mirror.locations.list",
	//   "path": "locations",
	//   "response": {
	//     "$ref": "LocationsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.settings.get":

type SettingsGetCall struct {
	s            *Service
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a single setting by ID.
func (r *SettingsService) Get(id string) *SettingsGetCall {
	c := &SettingsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "settings/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.settings.get" call.
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
	//   "description": "Gets a single setting by ID.",
	//   "httpMethod": "GET",
	//   "id": "mirror.settings.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the setting. The following IDs are valid: \n- locale - The key to the user’s language/locale (BCP 47 identifier) that Glassware should use to render localized content. \n- timezone - The key to the user’s current time zone region as defined in the tz database. Example: America/Los_Angeles.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "settings/{id}",
	//   "response": {
	//     "$ref": "Setting"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.subscriptions.delete":

type SubscriptionsDeleteCall struct {
	s          *Service
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a subscription.
func (r *SubscriptionsService) Delete(id string) *SubscriptionsDeleteCall {
	c := &SubscriptionsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SubscriptionsDeleteCall) Fields(s ...googleapi.Field) *SubscriptionsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SubscriptionsDeleteCall) Context(ctx context.Context) *SubscriptionsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SubscriptionsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SubscriptionsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "subscriptions/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.subscriptions.delete" call.
func (c *SubscriptionsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a subscription.",
	//   "httpMethod": "DELETE",
	//   "id": "mirror.subscriptions.delete",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the subscription.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "subscriptions/{id}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.subscriptions.insert":

type SubscriptionsInsertCall struct {
	s            *Service
	subscription *Subscription
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Insert: Creates a new subscription.
func (r *SubscriptionsService) Insert(subscription *Subscription) *SubscriptionsInsertCall {
	c := &SubscriptionsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.subscription = subscription
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SubscriptionsInsertCall) Fields(s ...googleapi.Field) *SubscriptionsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SubscriptionsInsertCall) Context(ctx context.Context) *SubscriptionsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SubscriptionsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SubscriptionsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.subscription)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "subscriptions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.subscriptions.insert" call.
// Exactly one of *Subscription or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Subscription.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *SubscriptionsInsertCall) Do(opts ...googleapi.CallOption) (*Subscription, error) {
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
	ret := &Subscription{
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
	//   "description": "Creates a new subscription.",
	//   "httpMethod": "POST",
	//   "id": "mirror.subscriptions.insert",
	//   "path": "subscriptions",
	//   "request": {
	//     "$ref": "Subscription"
	//   },
	//   "response": {
	//     "$ref": "Subscription"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.subscriptions.list":

type SubscriptionsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of subscriptions for the authenticated user
// and service.
func (r *SubscriptionsService) List() *SubscriptionsListCall {
	c := &SubscriptionsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SubscriptionsListCall) Fields(s ...googleapi.Field) *SubscriptionsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SubscriptionsListCall) IfNoneMatch(entityTag string) *SubscriptionsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SubscriptionsListCall) Context(ctx context.Context) *SubscriptionsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SubscriptionsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SubscriptionsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "subscriptions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.subscriptions.list" call.
// Exactly one of *SubscriptionsListResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *SubscriptionsListResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SubscriptionsListCall) Do(opts ...googleapi.CallOption) (*SubscriptionsListResponse, error) {
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
	ret := &SubscriptionsListResponse{
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
	//   "description": "Retrieves a list of subscriptions for the authenticated user and service.",
	//   "httpMethod": "GET",
	//   "id": "mirror.subscriptions.list",
	//   "path": "subscriptions",
	//   "response": {
	//     "$ref": "SubscriptionsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.subscriptions.update":

type SubscriptionsUpdateCall struct {
	s            *Service
	id           string
	subscription *Subscription
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Update: Updates an existing subscription in place.
func (r *SubscriptionsService) Update(id string, subscription *Subscription) *SubscriptionsUpdateCall {
	c := &SubscriptionsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.subscription = subscription
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SubscriptionsUpdateCall) Fields(s ...googleapi.Field) *SubscriptionsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SubscriptionsUpdateCall) Context(ctx context.Context) *SubscriptionsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SubscriptionsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SubscriptionsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.subscription)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "subscriptions/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.subscriptions.update" call.
// Exactly one of *Subscription or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Subscription.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *SubscriptionsUpdateCall) Do(opts ...googleapi.CallOption) (*Subscription, error) {
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
	ret := &Subscription{
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
	//   "description": "Updates an existing subscription in place.",
	//   "httpMethod": "PUT",
	//   "id": "mirror.subscriptions.update",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the subscription.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "subscriptions/{id}",
	//   "request": {
	//     "$ref": "Subscription"
	//   },
	//   "response": {
	//     "$ref": "Subscription"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.timeline.delete":

type TimelineDeleteCall struct {
	s          *Service
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a timeline item.
func (r *TimelineService) Delete(id string) *TimelineDeleteCall {
	c := &TimelineDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineDeleteCall) Fields(s ...googleapi.Field) *TimelineDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TimelineDeleteCall) Context(ctx context.Context) *TimelineDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.delete" call.
func (c *TimelineDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a timeline item.",
	//   "httpMethod": "DELETE",
	//   "id": "mirror.timeline.delete",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the timeline item.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{id}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.timeline.get":

type TimelineGetCall struct {
	s            *Service
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a single timeline item by ID.
func (r *TimelineService) Get(id string) *TimelineGetCall {
	c := &TimelineGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineGetCall) Fields(s ...googleapi.Field) *TimelineGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TimelineGetCall) IfNoneMatch(entityTag string) *TimelineGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TimelineGetCall) Context(ctx context.Context) *TimelineGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.get" call.
// Exactly one of *TimelineItem or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *TimelineItem.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TimelineGetCall) Do(opts ...googleapi.CallOption) (*TimelineItem, error) {
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
	ret := &TimelineItem{
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
	//   "description": "Gets a single timeline item by ID.",
	//   "httpMethod": "GET",
	//   "id": "mirror.timeline.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the timeline item.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{id}",
	//   "response": {
	//     "$ref": "TimelineItem"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.timeline.insert":

type TimelineInsertCall struct {
	s                *Service
	timelineitem     *TimelineItem
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Insert: Inserts a new item into the timeline.
func (r *TimelineService) Insert(timelineitem *TimelineItem) *TimelineInsertCall {
	c := &TimelineInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.timelineitem = timelineitem
	return c
}

// Media specifies the media to upload in one or more chunks. The chunk
// size may be controlled by supplying a MediaOption generated by
// googleapi.ChunkSize. The chunk size defaults to
// googleapi.DefaultUploadChunkSize.The Content-Type header used in the
// upload request will be determined by sniffing the contents of r,
// unless a MediaOption generated by googleapi.ContentType is
// supplied.
// At most one of Media and ResumableMedia may be set.
func (c *TimelineInsertCall) Media(r io.Reader, options ...googleapi.MediaOption) *TimelineInsertCall {
	opts := googleapi.ProcessMediaOptions(options)
	chunkSize := opts.ChunkSize
	if !opts.ForceEmptyContentType {
		r, c.mediaType_ = gensupport.DetermineContentType(r, opts.ContentType)
	}
	c.media_, c.mediaBuffer_ = gensupport.PrepareUpload(r, chunkSize)
	return c
}

// ResumableMedia specifies the media to upload in chunks and can be
// canceled with ctx.
//
// Deprecated: use Media instead.
//
// At most one of Media and ResumableMedia may be set. mediaType
// identifies the MIME media type of the upload, such as "image/png". If
// mediaType is "", it will be auto-detected. The provided ctx will
// supersede any context previously provided to the Context method.
func (c *TimelineInsertCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *TimelineInsertCall {
	c.ctx_ = ctx
	rdr := gensupport.ReaderAtToReader(r, size)
	rdr, c.mediaType_ = gensupport.DetermineContentType(rdr, mediaType)
	c.mediaBuffer_ = gensupport.NewMediaBuffer(rdr, googleapi.DefaultUploadChunkSize)
	c.media_ = nil
	c.mediaSize_ = size
	return c
}

// ProgressUpdater provides a callback function that will be called
// after every chunk. It should be a low-latency function in order to
// not slow down the upload operation. This should only be called when
// using ResumableMedia (as opposed to Media).
func (c *TimelineInsertCall) ProgressUpdater(pu googleapi.ProgressUpdater) *TimelineInsertCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineInsertCall) Fields(s ...googleapi.Field) *TimelineInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *TimelineInsertCall) Context(ctx context.Context) *TimelineInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.timelineitem)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline")
	if c.media_ != nil || c.mediaBuffer_ != nil {
		urls = strings.Replace(urls, "https://www.googleapis.com/", "https://www.googleapis.com/upload/", 1)
		protocol := "multipart"
		if c.mediaBuffer_ != nil {
			protocol = "resumable"
		}
		c.urlParams_.Set("uploadType", protocol)
	}
	if body == nil {
		body = new(bytes.Buffer)
		reqHeaders.Set("Content-Type", "application/json")
	}
	if c.media_ != nil {
		combined, ctype := gensupport.CombineBodyMedia(body, "application/json", c.media_, c.mediaType_)
		defer combined.Close()
		reqHeaders.Set("Content-Type", ctype)
		body = combined
	}
	if c.mediaBuffer_ != nil && c.mediaType_ != "" {
		reqHeaders.Set("X-Upload-Content-Type", c.mediaType_)
	}
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.insert" call.
// Exactly one of *TimelineItem or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *TimelineItem.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TimelineInsertCall) Do(opts ...googleapi.CallOption) (*TimelineItem, error) {
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
	if c.mediaBuffer_ != nil {
		loc := res.Header.Get("Location")
		rx := &gensupport.ResumableUpload{
			Client:    c.s.client,
			UserAgent: c.s.userAgent(),
			URI:       loc,
			Media:     c.mediaBuffer_,
			MediaType: c.mediaType_,
			Callback: func(curr int64) {
				if c.progressUpdater_ != nil {
					c.progressUpdater_(curr, c.mediaSize_)
				}
			},
		}
		ctx := c.ctx_
		if ctx == nil {
			ctx = context.TODO()
		}
		res, err = rx.Upload(ctx)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if err := googleapi.CheckResponse(res); err != nil {
			return nil, err
		}
	}
	ret := &TimelineItem{
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
	//   "description": "Inserts a new item into the timeline.",
	//   "httpMethod": "POST",
	//   "id": "mirror.timeline.insert",
	//   "mediaUpload": {
	//     "accept": [
	//       "audio/*",
	//       "image/*",
	//       "video/*"
	//     ],
	//     "maxSize": "10MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/mirror/v1/timeline"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/mirror/v1/timeline"
	//       }
	//     }
	//   },
	//   "path": "timeline",
	//   "request": {
	//     "$ref": "TimelineItem"
	//   },
	//   "response": {
	//     "$ref": "TimelineItem"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "mirror.timeline.list":

type TimelineListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of timeline items for the authenticated user.
func (r *TimelineService) List() *TimelineListCall {
	c := &TimelineListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// BundleId sets the optional parameter "bundleId": If provided, only
// items with the given bundleId will be returned.
func (c *TimelineListCall) BundleId(bundleId string) *TimelineListCall {
	c.urlParams_.Set("bundleId", bundleId)
	return c
}

// IncludeDeleted sets the optional parameter "includeDeleted": If true,
// tombstone records for deleted items will be returned.
func (c *TimelineListCall) IncludeDeleted(includeDeleted bool) *TimelineListCall {
	c.urlParams_.Set("includeDeleted", fmt.Sprint(includeDeleted))
	return c
}

// MaxResults sets the optional parameter "maxResults": The maximum
// number of items to include in the response, used for paging.
func (c *TimelineListCall) MaxResults(maxResults int64) *TimelineListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": Controls the order in
// which timeline items are returned.
//
// Possible values:
//   "displayTime" - Results will be ordered by displayTime (default).
// This is the same ordering as is used in the timeline on the device.
//   "writeTime" - Results will be ordered by the time at which they
// were last written to the data store.
func (c *TimelineListCall) OrderBy(orderBy string) *TimelineListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageToken sets the optional parameter "pageToken": Token for the page
// of results to return.
func (c *TimelineListCall) PageToken(pageToken string) *TimelineListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// PinnedOnly sets the optional parameter "pinnedOnly": If true, only
// pinned items will be returned.
func (c *TimelineListCall) PinnedOnly(pinnedOnly bool) *TimelineListCall {
	c.urlParams_.Set("pinnedOnly", fmt.Sprint(pinnedOnly))
	return c
}

// SourceItemId sets the optional parameter "sourceItemId": If provided,
// only items with the given sourceItemId will be returned.
func (c *TimelineListCall) SourceItemId(sourceItemId string) *TimelineListCall {
	c.urlParams_.Set("sourceItemId", sourceItemId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineListCall) Fields(s ...googleapi.Field) *TimelineListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TimelineListCall) IfNoneMatch(entityTag string) *TimelineListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TimelineListCall) Context(ctx context.Context) *TimelineListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.list" call.
// Exactly one of *TimelineListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *TimelineListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *TimelineListCall) Do(opts ...googleapi.CallOption) (*TimelineListResponse, error) {
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
	ret := &TimelineListResponse{
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
	//   "description": "Retrieves a list of timeline items for the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "mirror.timeline.list",
	//   "parameters": {
	//     "bundleId": {
	//       "description": "If provided, only items with the given bundleId will be returned.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "includeDeleted": {
	//       "description": "If true, tombstone records for deleted items will be returned.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "maxResults": {
	//       "description": "The maximum number of items to include in the response, used for paging.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "Controls the order in which timeline items are returned.",
	//       "enum": [
	//         "displayTime",
	//         "writeTime"
	//       ],
	//       "enumDescriptions": [
	//         "Results will be ordered by displayTime (default). This is the same ordering as is used in the timeline on the device.",
	//         "Results will be ordered by the time at which they were last written to the data store."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "Token for the page of results to return.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pinnedOnly": {
	//       "description": "If true, only pinned items will be returned.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "sourceItemId": {
	//       "description": "If provided, only items with the given sourceItemId will be returned.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline",
	//   "response": {
	//     "$ref": "TimelineListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *TimelineListCall) Pages(ctx context.Context, f func(*TimelineListResponse) error) error {
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

// method id "mirror.timeline.patch":

type TimelinePatchCall struct {
	s            *Service
	id           string
	timelineitem *TimelineItem
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Patch: Updates a timeline item in place. This method supports patch
// semantics.
func (r *TimelineService) Patch(id string, timelineitem *TimelineItem) *TimelinePatchCall {
	c := &TimelinePatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.timelineitem = timelineitem
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelinePatchCall) Fields(s ...googleapi.Field) *TimelinePatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TimelinePatchCall) Context(ctx context.Context) *TimelinePatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelinePatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelinePatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.timelineitem)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.patch" call.
// Exactly one of *TimelineItem or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *TimelineItem.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TimelinePatchCall) Do(opts ...googleapi.CallOption) (*TimelineItem, error) {
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
	ret := &TimelineItem{
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
	//   "description": "Updates a timeline item in place. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "mirror.timeline.patch",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the timeline item.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{id}",
	//   "request": {
	//     "$ref": "TimelineItem"
	//   },
	//   "response": {
	//     "$ref": "TimelineItem"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.timeline.update":

type TimelineUpdateCall struct {
	s                *Service
	id               string
	timelineitem     *TimelineItem
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Update: Updates a timeline item in place.
func (r *TimelineService) Update(id string, timelineitem *TimelineItem) *TimelineUpdateCall {
	c := &TimelineUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.timelineitem = timelineitem
	return c
}

// Media specifies the media to upload in one or more chunks. The chunk
// size may be controlled by supplying a MediaOption generated by
// googleapi.ChunkSize. The chunk size defaults to
// googleapi.DefaultUploadChunkSize.The Content-Type header used in the
// upload request will be determined by sniffing the contents of r,
// unless a MediaOption generated by googleapi.ContentType is
// supplied.
// At most one of Media and ResumableMedia may be set.
func (c *TimelineUpdateCall) Media(r io.Reader, options ...googleapi.MediaOption) *TimelineUpdateCall {
	opts := googleapi.ProcessMediaOptions(options)
	chunkSize := opts.ChunkSize
	if !opts.ForceEmptyContentType {
		r, c.mediaType_ = gensupport.DetermineContentType(r, opts.ContentType)
	}
	c.media_, c.mediaBuffer_ = gensupport.PrepareUpload(r, chunkSize)
	return c
}

// ResumableMedia specifies the media to upload in chunks and can be
// canceled with ctx.
//
// Deprecated: use Media instead.
//
// At most one of Media and ResumableMedia may be set. mediaType
// identifies the MIME media type of the upload, such as "image/png". If
// mediaType is "", it will be auto-detected. The provided ctx will
// supersede any context previously provided to the Context method.
func (c *TimelineUpdateCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *TimelineUpdateCall {
	c.ctx_ = ctx
	rdr := gensupport.ReaderAtToReader(r, size)
	rdr, c.mediaType_ = gensupport.DetermineContentType(rdr, mediaType)
	c.mediaBuffer_ = gensupport.NewMediaBuffer(rdr, googleapi.DefaultUploadChunkSize)
	c.media_ = nil
	c.mediaSize_ = size
	return c
}

// ProgressUpdater provides a callback function that will be called
// after every chunk. It should be a low-latency function in order to
// not slow down the upload operation. This should only be called when
// using ResumableMedia (as opposed to Media).
func (c *TimelineUpdateCall) ProgressUpdater(pu googleapi.ProgressUpdater) *TimelineUpdateCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineUpdateCall) Fields(s ...googleapi.Field) *TimelineUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *TimelineUpdateCall) Context(ctx context.Context) *TimelineUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.timelineitem)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{id}")
	if c.media_ != nil || c.mediaBuffer_ != nil {
		urls = strings.Replace(urls, "https://www.googleapis.com/", "https://www.googleapis.com/upload/", 1)
		protocol := "multipart"
		if c.mediaBuffer_ != nil {
			protocol = "resumable"
		}
		c.urlParams_.Set("uploadType", protocol)
	}
	if body == nil {
		body = new(bytes.Buffer)
		reqHeaders.Set("Content-Type", "application/json")
	}
	if c.media_ != nil {
		combined, ctype := gensupport.CombineBodyMedia(body, "application/json", c.media_, c.mediaType_)
		defer combined.Close()
		reqHeaders.Set("Content-Type", ctype)
		body = combined
	}
	if c.mediaBuffer_ != nil && c.mediaType_ != "" {
		reqHeaders.Set("X-Upload-Content-Type", c.mediaType_)
	}
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.update" call.
// Exactly one of *TimelineItem or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *TimelineItem.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TimelineUpdateCall) Do(opts ...googleapi.CallOption) (*TimelineItem, error) {
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
	if c.mediaBuffer_ != nil {
		loc := res.Header.Get("Location")
		rx := &gensupport.ResumableUpload{
			Client:    c.s.client,
			UserAgent: c.s.userAgent(),
			URI:       loc,
			Media:     c.mediaBuffer_,
			MediaType: c.mediaType_,
			Callback: func(curr int64) {
				if c.progressUpdater_ != nil {
					c.progressUpdater_(curr, c.mediaSize_)
				}
			},
		}
		ctx := c.ctx_
		if ctx == nil {
			ctx = context.TODO()
		}
		res, err = rx.Upload(ctx)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if err := googleapi.CheckResponse(res); err != nil {
			return nil, err
		}
	}
	ret := &TimelineItem{
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
	//   "description": "Updates a timeline item in place.",
	//   "httpMethod": "PUT",
	//   "id": "mirror.timeline.update",
	//   "mediaUpload": {
	//     "accept": [
	//       "audio/*",
	//       "image/*",
	//       "video/*"
	//     ],
	//     "maxSize": "10MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/mirror/v1/timeline/{id}"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/mirror/v1/timeline/{id}"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the timeline item.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{id}",
	//   "request": {
	//     "$ref": "TimelineItem"
	//   },
	//   "response": {
	//     "$ref": "TimelineItem"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.location",
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "mirror.timeline.attachments.delete":

type TimelineAttachmentsDeleteCall struct {
	s            *Service
	itemId       string
	attachmentId string
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Delete: Deletes an attachment from a timeline item.
func (r *TimelineAttachmentsService) Delete(itemId string, attachmentId string) *TimelineAttachmentsDeleteCall {
	c := &TimelineAttachmentsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.itemId = itemId
	c.attachmentId = attachmentId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineAttachmentsDeleteCall) Fields(s ...googleapi.Field) *TimelineAttachmentsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TimelineAttachmentsDeleteCall) Context(ctx context.Context) *TimelineAttachmentsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineAttachmentsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineAttachmentsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{itemId}/attachments/{attachmentId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"itemId":       c.itemId,
		"attachmentId": c.attachmentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.attachments.delete" call.
func (c *TimelineAttachmentsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes an attachment from a timeline item.",
	//   "httpMethod": "DELETE",
	//   "id": "mirror.timeline.attachments.delete",
	//   "parameterOrder": [
	//     "itemId",
	//     "attachmentId"
	//   ],
	//   "parameters": {
	//     "attachmentId": {
	//       "description": "The ID of the attachment.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "itemId": {
	//       "description": "The ID of the timeline item the attachment belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{itemId}/attachments/{attachmentId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

// method id "mirror.timeline.attachments.get":

type TimelineAttachmentsGetCall struct {
	s            *Service
	itemId       string
	attachmentId string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves an attachment on a timeline item by item ID and
// attachment ID.
func (r *TimelineAttachmentsService) Get(itemId string, attachmentId string) *TimelineAttachmentsGetCall {
	c := &TimelineAttachmentsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.itemId = itemId
	c.attachmentId = attachmentId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineAttachmentsGetCall) Fields(s ...googleapi.Field) *TimelineAttachmentsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TimelineAttachmentsGetCall) IfNoneMatch(entityTag string) *TimelineAttachmentsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do and Download
// methods. Any pending HTTP request will be aborted if the provided
// context is canceled.
func (c *TimelineAttachmentsGetCall) Context(ctx context.Context) *TimelineAttachmentsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineAttachmentsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineAttachmentsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{itemId}/attachments/{attachmentId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"itemId":       c.itemId,
		"attachmentId": c.attachmentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Download fetches the API endpoint's "media" value, instead of the normal
// API response value. If the returned error is nil, the Response is guaranteed to
// have a 2xx status code. Callers must close the Response.Body as usual.
func (c *TimelineAttachmentsGetCall) Download(opts ...googleapi.CallOption) (*http.Response, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("media")
	if err != nil {
		return nil, err
	}
	if err := googleapi.CheckMediaResponse(res); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res, nil
}

// Do executes the "mirror.timeline.attachments.get" call.
// Exactly one of *Attachment or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Attachment.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TimelineAttachmentsGetCall) Do(opts ...googleapi.CallOption) (*Attachment, error) {
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
	ret := &Attachment{
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
	//   "description": "Retrieves an attachment on a timeline item by item ID and attachment ID.",
	//   "httpMethod": "GET",
	//   "id": "mirror.timeline.attachments.get",
	//   "parameterOrder": [
	//     "itemId",
	//     "attachmentId"
	//   ],
	//   "parameters": {
	//     "attachmentId": {
	//       "description": "The ID of the attachment.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "itemId": {
	//       "description": "The ID of the timeline item the attachment belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{itemId}/attachments/{attachmentId}",
	//   "response": {
	//     "$ref": "Attachment"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ],
	//   "supportsMediaDownload": true
	// }

}

// method id "mirror.timeline.attachments.insert":

type TimelineAttachmentsInsertCall struct {
	s                *Service
	itemId           string
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Insert: Adds a new attachment to a timeline item.
func (r *TimelineAttachmentsService) Insert(itemId string) *TimelineAttachmentsInsertCall {
	c := &TimelineAttachmentsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.itemId = itemId
	return c
}

// Media specifies the media to upload in one or more chunks. The chunk
// size may be controlled by supplying a MediaOption generated by
// googleapi.ChunkSize. The chunk size defaults to
// googleapi.DefaultUploadChunkSize.The Content-Type header used in the
// upload request will be determined by sniffing the contents of r,
// unless a MediaOption generated by googleapi.ContentType is
// supplied.
// At most one of Media and ResumableMedia may be set.
func (c *TimelineAttachmentsInsertCall) Media(r io.Reader, options ...googleapi.MediaOption) *TimelineAttachmentsInsertCall {
	opts := googleapi.ProcessMediaOptions(options)
	chunkSize := opts.ChunkSize
	if !opts.ForceEmptyContentType {
		r, c.mediaType_ = gensupport.DetermineContentType(r, opts.ContentType)
	}
	c.media_, c.mediaBuffer_ = gensupport.PrepareUpload(r, chunkSize)
	return c
}

// ResumableMedia specifies the media to upload in chunks and can be
// canceled with ctx.
//
// Deprecated: use Media instead.
//
// At most one of Media and ResumableMedia may be set. mediaType
// identifies the MIME media type of the upload, such as "image/png". If
// mediaType is "", it will be auto-detected. The provided ctx will
// supersede any context previously provided to the Context method.
func (c *TimelineAttachmentsInsertCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *TimelineAttachmentsInsertCall {
	c.ctx_ = ctx
	rdr := gensupport.ReaderAtToReader(r, size)
	rdr, c.mediaType_ = gensupport.DetermineContentType(rdr, mediaType)
	c.mediaBuffer_ = gensupport.NewMediaBuffer(rdr, googleapi.DefaultUploadChunkSize)
	c.media_ = nil
	c.mediaSize_ = size
	return c
}

// ProgressUpdater provides a callback function that will be called
// after every chunk. It should be a low-latency function in order to
// not slow down the upload operation. This should only be called when
// using ResumableMedia (as opposed to Media).
func (c *TimelineAttachmentsInsertCall) ProgressUpdater(pu googleapi.ProgressUpdater) *TimelineAttachmentsInsertCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineAttachmentsInsertCall) Fields(s ...googleapi.Field) *TimelineAttachmentsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *TimelineAttachmentsInsertCall) Context(ctx context.Context) *TimelineAttachmentsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineAttachmentsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineAttachmentsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{itemId}/attachments")
	if c.media_ != nil || c.mediaBuffer_ != nil {
		urls = strings.Replace(urls, "https://www.googleapis.com/", "https://www.googleapis.com/upload/", 1)
		protocol := "multipart"
		if c.mediaBuffer_ != nil {
			protocol = "resumable"
		}
		c.urlParams_.Set("uploadType", protocol)
	}
	if body == nil {
		body = new(bytes.Buffer)
		reqHeaders.Set("Content-Type", "application/json")
	}
	if c.media_ != nil {
		combined, ctype := gensupport.CombineBodyMedia(body, "application/json", c.media_, c.mediaType_)
		defer combined.Close()
		reqHeaders.Set("Content-Type", ctype)
		body = combined
	}
	if c.mediaBuffer_ != nil && c.mediaType_ != "" {
		reqHeaders.Set("X-Upload-Content-Type", c.mediaType_)
	}
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"itemId": c.itemId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.attachments.insert" call.
// Exactly one of *Attachment or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Attachment.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TimelineAttachmentsInsertCall) Do(opts ...googleapi.CallOption) (*Attachment, error) {
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
	if c.mediaBuffer_ != nil {
		loc := res.Header.Get("Location")
		rx := &gensupport.ResumableUpload{
			Client:    c.s.client,
			UserAgent: c.s.userAgent(),
			URI:       loc,
			Media:     c.mediaBuffer_,
			MediaType: c.mediaType_,
			Callback: func(curr int64) {
				if c.progressUpdater_ != nil {
					c.progressUpdater_(curr, c.mediaSize_)
				}
			},
		}
		ctx := c.ctx_
		if ctx == nil {
			ctx = context.TODO()
		}
		res, err = rx.Upload(ctx)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if err := googleapi.CheckResponse(res); err != nil {
			return nil, err
		}
	}
	ret := &Attachment{
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
	//   "description": "Adds a new attachment to a timeline item.",
	//   "httpMethod": "POST",
	//   "id": "mirror.timeline.attachments.insert",
	//   "mediaUpload": {
	//     "accept": [
	//       "audio/*",
	//       "image/*",
	//       "video/*"
	//     ],
	//     "maxSize": "10MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/mirror/v1/timeline/{itemId}/attachments"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/mirror/v1/timeline/{itemId}/attachments"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "itemId"
	//   ],
	//   "parameters": {
	//     "itemId": {
	//       "description": "The ID of the timeline item the attachment belongs to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{itemId}/attachments",
	//   "response": {
	//     "$ref": "Attachment"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "mirror.timeline.attachments.list":

type TimelineAttachmentsListCall struct {
	s            *Service
	itemId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of attachments for a timeline item.
func (r *TimelineAttachmentsService) List(itemId string) *TimelineAttachmentsListCall {
	c := &TimelineAttachmentsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.itemId = itemId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TimelineAttachmentsListCall) Fields(s ...googleapi.Field) *TimelineAttachmentsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TimelineAttachmentsListCall) IfNoneMatch(entityTag string) *TimelineAttachmentsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TimelineAttachmentsListCall) Context(ctx context.Context) *TimelineAttachmentsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TimelineAttachmentsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TimelineAttachmentsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "timeline/{itemId}/attachments")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"itemId": c.itemId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "mirror.timeline.attachments.list" call.
// Exactly one of *AttachmentsListResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *AttachmentsListResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *TimelineAttachmentsListCall) Do(opts ...googleapi.CallOption) (*AttachmentsListResponse, error) {
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
	ret := &AttachmentsListResponse{
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
	//   "description": "Returns a list of attachments for a timeline item.",
	//   "httpMethod": "GET",
	//   "id": "mirror.timeline.attachments.list",
	//   "parameterOrder": [
	//     "itemId"
	//   ],
	//   "parameters": {
	//     "itemId": {
	//       "description": "The ID of the timeline item whose attachments should be listed.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "timeline/{itemId}/attachments",
	//   "response": {
	//     "$ref": "AttachmentsListResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/glass.timeline"
	//   ]
	// }

}

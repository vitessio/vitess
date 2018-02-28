// Package gmail provides access to the Gmail API.
//
// See https://developers.google.com/gmail/api/
//
// Usage example:
//
//   import "google.golang.org/api/gmail/v1"
//   ...
//   gmailService, err := gmail.New(oauthHttpClient)
package gmail // import "google.golang.org/api/gmail/v1"

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

const apiId = "gmail:v1"
const apiName = "gmail"
const apiVersion = "v1"
const basePath = "https://www.googleapis.com/gmail/v1/users/"

// OAuth2 scopes used by this API.
const (
	// View and manage your mail
	MailGoogleComScope = "https://mail.google.com/"

	// Manage drafts and send emails
	GmailComposeScope = "https://www.googleapis.com/auth/gmail.compose"

	// Insert mail into your mailbox
	GmailInsertScope = "https://www.googleapis.com/auth/gmail.insert"

	// Manage mailbox labels
	GmailLabelsScope = "https://www.googleapis.com/auth/gmail.labels"

	// View your email message metadata such as labels and headers, but not
	// the email body
	GmailMetadataScope = "https://www.googleapis.com/auth/gmail.metadata"

	// View and modify but not delete your email
	GmailModifyScope = "https://www.googleapis.com/auth/gmail.modify"

	// View your emails messages and settings
	GmailReadonlyScope = "https://www.googleapis.com/auth/gmail.readonly"

	// Send email on your behalf
	GmailSendScope = "https://www.googleapis.com/auth/gmail.send"

	// Manage your basic mail settings
	GmailSettingsBasicScope = "https://www.googleapis.com/auth/gmail.settings.basic"

	// Manage your sensitive mail settings, including who can manage your
	// mail
	GmailSettingsSharingScope = "https://www.googleapis.com/auth/gmail.settings.sharing"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Users = NewUsersService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Users *UsersService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewUsersService(s *Service) *UsersService {
	rs := &UsersService{s: s}
	rs.Drafts = NewUsersDraftsService(s)
	rs.History = NewUsersHistoryService(s)
	rs.Labels = NewUsersLabelsService(s)
	rs.Messages = NewUsersMessagesService(s)
	rs.Settings = NewUsersSettingsService(s)
	rs.Threads = NewUsersThreadsService(s)
	return rs
}

type UsersService struct {
	s *Service

	Drafts *UsersDraftsService

	History *UsersHistoryService

	Labels *UsersLabelsService

	Messages *UsersMessagesService

	Settings *UsersSettingsService

	Threads *UsersThreadsService
}

func NewUsersDraftsService(s *Service) *UsersDraftsService {
	rs := &UsersDraftsService{s: s}
	return rs
}

type UsersDraftsService struct {
	s *Service
}

func NewUsersHistoryService(s *Service) *UsersHistoryService {
	rs := &UsersHistoryService{s: s}
	return rs
}

type UsersHistoryService struct {
	s *Service
}

func NewUsersLabelsService(s *Service) *UsersLabelsService {
	rs := &UsersLabelsService{s: s}
	return rs
}

type UsersLabelsService struct {
	s *Service
}

func NewUsersMessagesService(s *Service) *UsersMessagesService {
	rs := &UsersMessagesService{s: s}
	rs.Attachments = NewUsersMessagesAttachmentsService(s)
	return rs
}

type UsersMessagesService struct {
	s *Service

	Attachments *UsersMessagesAttachmentsService
}

func NewUsersMessagesAttachmentsService(s *Service) *UsersMessagesAttachmentsService {
	rs := &UsersMessagesAttachmentsService{s: s}
	return rs
}

type UsersMessagesAttachmentsService struct {
	s *Service
}

func NewUsersSettingsService(s *Service) *UsersSettingsService {
	rs := &UsersSettingsService{s: s}
	rs.Filters = NewUsersSettingsFiltersService(s)
	rs.ForwardingAddresses = NewUsersSettingsForwardingAddressesService(s)
	rs.SendAs = NewUsersSettingsSendAsService(s)
	return rs
}

type UsersSettingsService struct {
	s *Service

	Filters *UsersSettingsFiltersService

	ForwardingAddresses *UsersSettingsForwardingAddressesService

	SendAs *UsersSettingsSendAsService
}

func NewUsersSettingsFiltersService(s *Service) *UsersSettingsFiltersService {
	rs := &UsersSettingsFiltersService{s: s}
	return rs
}

type UsersSettingsFiltersService struct {
	s *Service
}

func NewUsersSettingsForwardingAddressesService(s *Service) *UsersSettingsForwardingAddressesService {
	rs := &UsersSettingsForwardingAddressesService{s: s}
	return rs
}

type UsersSettingsForwardingAddressesService struct {
	s *Service
}

func NewUsersSettingsSendAsService(s *Service) *UsersSettingsSendAsService {
	rs := &UsersSettingsSendAsService{s: s}
	return rs
}

type UsersSettingsSendAsService struct {
	s *Service
}

func NewUsersThreadsService(s *Service) *UsersThreadsService {
	rs := &UsersThreadsService{s: s}
	return rs
}

type UsersThreadsService struct {
	s *Service
}

// AutoForwarding: Auto-forwarding settings for an account.
type AutoForwarding struct {
	// Disposition: The state that a message should be left in after it has
	// been forwarded.
	//
	// Possible values:
	//   "archive"
	//   "dispositionUnspecified"
	//   "leaveInInbox"
	//   "markRead"
	//   "trash"
	Disposition string `json:"disposition,omitempty"`

	// EmailAddress: Email address to which all incoming messages are
	// forwarded. This email address must be a verified member of the
	// forwarding addresses.
	EmailAddress string `json:"emailAddress,omitempty"`

	// Enabled: Whether all incoming mail is automatically forwarded to
	// another address.
	Enabled bool `json:"enabled,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Disposition") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Disposition") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AutoForwarding) MarshalJSON() ([]byte, error) {
	type noMethod AutoForwarding
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type BatchDeleteMessagesRequest struct {
	// Ids: The IDs of the messages to delete.
	Ids []string `json:"ids,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Ids") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Ids") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BatchDeleteMessagesRequest) MarshalJSON() ([]byte, error) {
	type noMethod BatchDeleteMessagesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Draft: A draft email in the user's mailbox.
type Draft struct {
	// Id: The immutable ID of the draft.
	Id string `json:"id,omitempty"`

	// Message: The message content of the draft.
	Message *Message `json:"message,omitempty"`

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

func (s *Draft) MarshalJSON() ([]byte, error) {
	type noMethod Draft
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Filter: Resource definition for Gmail filters. Filters apply to
// specific messages instead of an entire email thread.
type Filter struct {
	// Action: Action that the filter performs.
	Action *FilterAction `json:"action,omitempty"`

	// Criteria: Matching criteria for the filter.
	Criteria *FilterCriteria `json:"criteria,omitempty"`

	// Id: The server assigned ID of the filter.
	Id string `json:"id,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *Filter) MarshalJSON() ([]byte, error) {
	type noMethod Filter
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FilterAction: A set of actions to perform on a message.
type FilterAction struct {
	// AddLabelIds: List of labels to add to the message.
	AddLabelIds []string `json:"addLabelIds,omitempty"`

	// Forward: Email address that the message should be forwarded to.
	Forward string `json:"forward,omitempty"`

	// RemoveLabelIds: List of labels to remove from the message.
	RemoveLabelIds []string `json:"removeLabelIds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AddLabelIds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AddLabelIds") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FilterAction) MarshalJSON() ([]byte, error) {
	type noMethod FilterAction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// FilterCriteria: Message matching criteria.
type FilterCriteria struct {
	// ExcludeChats: Whether the response should exclude chats.
	ExcludeChats bool `json:"excludeChats,omitempty"`

	// From: The sender's display name or email address.
	From string `json:"from,omitempty"`

	// HasAttachment: Whether the message has any attachment.
	HasAttachment bool `json:"hasAttachment,omitempty"`

	// NegatedQuery: Only return messages not matching the specified query.
	// Supports the same query format as the Gmail search box. For example,
	// "from:someuser@example.com rfc822msgid: is:unread".
	NegatedQuery string `json:"negatedQuery,omitempty"`

	// Query: Only return messages matching the specified query. Supports
	// the same query format as the Gmail search box. For example,
	// "from:someuser@example.com rfc822msgid: is:unread".
	Query string `json:"query,omitempty"`

	// Size: The size of the entire RFC822 message in bytes, including all
	// headers and attachments.
	Size int64 `json:"size,omitempty"`

	// SizeComparison: How the message size in bytes should be in relation
	// to the size field.
	//
	// Possible values:
	//   "larger"
	//   "smaller"
	//   "unspecified"
	SizeComparison string `json:"sizeComparison,omitempty"`

	// Subject: Case-insensitive phrase found in the message's subject.
	// Trailing and leading whitespace are be trimmed and adjacent spaces
	// are collapsed.
	Subject string `json:"subject,omitempty"`

	// To: The recipient's display name or email address. Includes
	// recipients in the "to", "cc", and "bcc" header fields. You can use
	// simply the local part of the email address. For example, "example"
	// and "example@" both match "example@gmail.com". This field is
	// case-insensitive.
	To string `json:"to,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ExcludeChats") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ExcludeChats") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *FilterCriteria) MarshalJSON() ([]byte, error) {
	type noMethod FilterCriteria
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ForwardingAddress: Settings for a forwarding address.
type ForwardingAddress struct {
	// ForwardingEmail: An email address to which messages can be forwarded.
	ForwardingEmail string `json:"forwardingEmail,omitempty"`

	// VerificationStatus: Indicates whether this address has been verified
	// and is usable for forwarding. Read-only.
	//
	// Possible values:
	//   "accepted"
	//   "pending"
	//   "verificationStatusUnspecified"
	VerificationStatus string `json:"verificationStatus,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ForwardingEmail") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ForwardingEmail") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ForwardingAddress) MarshalJSON() ([]byte, error) {
	type noMethod ForwardingAddress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// History: A record of a change to the user's mailbox. Each history
// change may affect multiple messages in multiple ways.
type History struct {
	// Id: The mailbox sequence ID.
	Id uint64 `json:"id,omitempty,string"`

	// LabelsAdded: Labels added to messages in this history record.
	LabelsAdded []*HistoryLabelAdded `json:"labelsAdded,omitempty"`

	// LabelsRemoved: Labels removed from messages in this history record.
	LabelsRemoved []*HistoryLabelRemoved `json:"labelsRemoved,omitempty"`

	// Messages: List of messages changed in this history record. The fields
	// for specific change types, such as messagesAdded may duplicate
	// messages in this field. We recommend using the specific change-type
	// fields instead of this.
	Messages []*Message `json:"messages,omitempty"`

	// MessagesAdded: Messages added to the mailbox in this history record.
	MessagesAdded []*HistoryMessageAdded `json:"messagesAdded,omitempty"`

	// MessagesDeleted: Messages deleted (not Trashed) from the mailbox in
	// this history record.
	MessagesDeleted []*HistoryMessageDeleted `json:"messagesDeleted,omitempty"`

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

func (s *History) MarshalJSON() ([]byte, error) {
	type noMethod History
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type HistoryLabelAdded struct {
	// LabelIds: Label IDs added to the message.
	LabelIds []string `json:"labelIds,omitempty"`

	Message *Message `json:"message,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LabelIds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LabelIds") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *HistoryLabelAdded) MarshalJSON() ([]byte, error) {
	type noMethod HistoryLabelAdded
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type HistoryLabelRemoved struct {
	// LabelIds: Label IDs removed from the message.
	LabelIds []string `json:"labelIds,omitempty"`

	Message *Message `json:"message,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LabelIds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LabelIds") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *HistoryLabelRemoved) MarshalJSON() ([]byte, error) {
	type noMethod HistoryLabelRemoved
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type HistoryMessageAdded struct {
	Message *Message `json:"message,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Message") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Message") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *HistoryMessageAdded) MarshalJSON() ([]byte, error) {
	type noMethod HistoryMessageAdded
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type HistoryMessageDeleted struct {
	Message *Message `json:"message,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Message") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Message") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *HistoryMessageDeleted) MarshalJSON() ([]byte, error) {
	type noMethod HistoryMessageDeleted
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ImapSettings: IMAP settings for an account.
type ImapSettings struct {
	// AutoExpunge: If this value is true, Gmail will immediately expunge a
	// message when it is marked as deleted in IMAP. Otherwise, Gmail will
	// wait for an update from the client before expunging messages marked
	// as deleted.
	AutoExpunge bool `json:"autoExpunge,omitempty"`

	// Enabled: Whether IMAP is enabled for the account.
	Enabled bool `json:"enabled,omitempty"`

	// ExpungeBehavior: The action that will be executed on a message when
	// it is marked as deleted and expunged from the last visible IMAP
	// folder.
	//
	// Possible values:
	//   "archive"
	//   "deleteForever"
	//   "expungeBehaviorUnspecified"
	//   "trash"
	ExpungeBehavior string `json:"expungeBehavior,omitempty"`

	// MaxFolderSize: An optional limit on the number of messages that an
	// IMAP folder may contain. Legal values are 0, 1000, 2000, 5000 or
	// 10000. A value of zero is interpreted to mean that there is no limit.
	MaxFolderSize int64 `json:"maxFolderSize,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AutoExpunge") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AutoExpunge") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ImapSettings) MarshalJSON() ([]byte, error) {
	type noMethod ImapSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Label: Labels are used to categorize messages and threads within the
// user's mailbox.
type Label struct {
	// Id: The immutable ID of the label.
	Id string `json:"id,omitempty"`

	// LabelListVisibility: The visibility of the label in the label list in
	// the Gmail web interface.
	//
	// Possible values:
	//   "labelHide"
	//   "labelShow"
	//   "labelShowIfUnread"
	LabelListVisibility string `json:"labelListVisibility,omitempty"`

	// MessageListVisibility: The visibility of the label in the message
	// list in the Gmail web interface.
	//
	// Possible values:
	//   "hide"
	//   "show"
	MessageListVisibility string `json:"messageListVisibility,omitempty"`

	// MessagesTotal: The total number of messages with the label.
	MessagesTotal int64 `json:"messagesTotal,omitempty"`

	// MessagesUnread: The number of unread messages with the label.
	MessagesUnread int64 `json:"messagesUnread,omitempty"`

	// Name: The display name of the label.
	Name string `json:"name,omitempty"`

	// ThreadsTotal: The total number of threads with the label.
	ThreadsTotal int64 `json:"threadsTotal,omitempty"`

	// ThreadsUnread: The number of unread threads with the label.
	ThreadsUnread int64 `json:"threadsUnread,omitempty"`

	// Type: The owner type for the label. User labels are created by the
	// user and can be modified and deleted by the user and can be applied
	// to any message or thread. System labels are internally created and
	// cannot be added, modified, or deleted. System labels may be able to
	// be applied to or removed from messages and threads under some
	// circumstances but this is not guaranteed. For example, users can
	// apply and remove the INBOX and UNREAD labels from messages and
	// threads, but cannot apply or remove the DRAFTS or SENT labels from
	// messages or threads.
	//
	// Possible values:
	//   "system"
	//   "user"
	Type string `json:"type,omitempty"`

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

func (s *Label) MarshalJSON() ([]byte, error) {
	type noMethod Label
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListDraftsResponse struct {
	// Drafts: List of drafts.
	Drafts []*Draft `json:"drafts,omitempty"`

	// NextPageToken: Token to retrieve the next page of results in the
	// list.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ResultSizeEstimate: Estimated total number of results.
	ResultSizeEstimate int64 `json:"resultSizeEstimate,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Drafts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Drafts") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListDraftsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListDraftsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListFiltersResponse: Response for the ListFilters method.
type ListFiltersResponse struct {
	// Filter: List of a user's filters.
	Filter []*Filter `json:"filter,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Filter") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Filter") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListFiltersResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListFiltersResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListForwardingAddressesResponse: Response for the
// ListForwardingAddresses method.
type ListForwardingAddressesResponse struct {
	// ForwardingAddresses: List of addresses that may be used for
	// forwarding.
	ForwardingAddresses []*ForwardingAddress `json:"forwardingAddresses,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ForwardingAddresses")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ForwardingAddresses") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ListForwardingAddressesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListForwardingAddressesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListHistoryResponse struct {
	// History: List of history records. Any messages contained in the
	// response will typically only have id and threadId fields populated.
	History []*History `json:"history,omitempty"`

	// HistoryId: The ID of the mailbox's current history record.
	HistoryId uint64 `json:"historyId,omitempty,string"`

	// NextPageToken: Page token to retrieve the next page of results in the
	// list.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "History") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "History") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListHistoryResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListHistoryResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListLabelsResponse struct {
	// Labels: List of labels.
	Labels []*Label `json:"labels,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Labels") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Labels") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListLabelsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListLabelsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListMessagesResponse struct {
	// Messages: List of messages.
	Messages []*Message `json:"messages,omitempty"`

	// NextPageToken: Token to retrieve the next page of results in the
	// list.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ResultSizeEstimate: Estimated total number of results.
	ResultSizeEstimate int64 `json:"resultSizeEstimate,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Messages") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Messages") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListMessagesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListMessagesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListSendAsResponse: Response for the ListSendAs method.
type ListSendAsResponse struct {
	// SendAs: List of send-as aliases.
	SendAs []*SendAs `json:"sendAs,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "SendAs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "SendAs") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListSendAsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListSendAsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListThreadsResponse struct {
	// NextPageToken: Page token to retrieve the next page of results in the
	// list.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ResultSizeEstimate: Estimated total number of results.
	ResultSizeEstimate int64 `json:"resultSizeEstimate,omitempty"`

	// Threads: List of threads.
	Threads []*Thread `json:"threads,omitempty"`

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

func (s *ListThreadsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListThreadsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Message: An email message.
type Message struct {
	// HistoryId: The ID of the last history record that modified this
	// message.
	HistoryId uint64 `json:"historyId,omitempty,string"`

	// Id: The immutable ID of the message.
	Id string `json:"id,omitempty"`

	// InternalDate: The internal message creation timestamp (epoch ms),
	// which determines ordering in the inbox. For normal SMTP-received
	// email, this represents the time the message was originally accepted
	// by Google, which is more reliable than the Date header. However, for
	// API-migrated mail, it can be configured by client to be based on the
	// Date header.
	InternalDate int64 `json:"internalDate,omitempty,string"`

	// LabelIds: List of IDs of labels applied to this message.
	LabelIds []string `json:"labelIds,omitempty"`

	// Payload: The parsed email structure in the message parts.
	Payload *MessagePart `json:"payload,omitempty"`

	// Raw: The entire email message in an RFC 2822 formatted and base64url
	// encoded string. Returned in messages.get and drafts.get responses
	// when the format=RAW parameter is supplied.
	Raw string `json:"raw,omitempty"`

	// SizeEstimate: Estimated size in bytes of the message.
	SizeEstimate int64 `json:"sizeEstimate,omitempty"`

	// Snippet: A short part of the message text.
	Snippet string `json:"snippet,omitempty"`

	// ThreadId: The ID of the thread the message belongs to. To add a
	// message or draft to a thread, the following criteria must be met:
	// - The requested threadId must be specified on the Message or
	// Draft.Message you supply with your request.
	// - The References and In-Reply-To headers must be set in compliance
	// with the RFC 2822 standard.
	// - The Subject headers must match.
	ThreadId string `json:"threadId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "HistoryId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HistoryId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Message) MarshalJSON() ([]byte, error) {
	type noMethod Message
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MessagePart: A single MIME message part.
type MessagePart struct {
	// Body: The message part body for this part, which may be empty for
	// container MIME message parts.
	Body *MessagePartBody `json:"body,omitempty"`

	// Filename: The filename of the attachment. Only present if this
	// message part represents an attachment.
	Filename string `json:"filename,omitempty"`

	// Headers: List of headers on this message part. For the top-level
	// message part, representing the entire message payload, it will
	// contain the standard RFC 2822 email headers such as To, From, and
	// Subject.
	Headers []*MessagePartHeader `json:"headers,omitempty"`

	// MimeType: The MIME type of the message part.
	MimeType string `json:"mimeType,omitempty"`

	// PartId: The immutable ID of the message part.
	PartId string `json:"partId,omitempty"`

	// Parts: The child MIME message parts of this part. This only applies
	// to container MIME message parts, for example multipart/*. For non-
	// container MIME message part types, such as text/plain, this field is
	// empty. For more information, see RFC 1521.
	Parts []*MessagePart `json:"parts,omitempty"`

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

func (s *MessagePart) MarshalJSON() ([]byte, error) {
	type noMethod MessagePart
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MessagePartBody: The body of a single MIME message part.
type MessagePartBody struct {
	// AttachmentId: When present, contains the ID of an external attachment
	// that can be retrieved in a separate messages.attachments.get request.
	// When not present, the entire content of the message part body is
	// contained in the data field.
	AttachmentId string `json:"attachmentId,omitempty"`

	// Data: The body data of a MIME message part as a base64url encoded
	// string. May be empty for MIME container types that have no message
	// body or when the body data is sent as a separate attachment. An
	// attachment ID is present if the body data is contained in a separate
	// attachment.
	Data string `json:"data,omitempty"`

	// Size: Number of bytes for the message part data (encoding
	// notwithstanding).
	Size int64 `json:"size,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AttachmentId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AttachmentId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MessagePartBody) MarshalJSON() ([]byte, error) {
	type noMethod MessagePartBody
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MessagePartHeader struct {
	// Name: The name of the header before the : separator. For example, To.
	Name string `json:"name,omitempty"`

	// Value: The value of the header after the : separator. For example,
	// someuser@example.com.
	Value string `json:"value,omitempty"`

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

func (s *MessagePartHeader) MarshalJSON() ([]byte, error) {
	type noMethod MessagePartHeader
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ModifyMessageRequest struct {
	// AddLabelIds: A list of IDs of labels to add to this message.
	AddLabelIds []string `json:"addLabelIds,omitempty"`

	// RemoveLabelIds: A list IDs of labels to remove from this message.
	RemoveLabelIds []string `json:"removeLabelIds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AddLabelIds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AddLabelIds") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ModifyMessageRequest) MarshalJSON() ([]byte, error) {
	type noMethod ModifyMessageRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ModifyThreadRequest struct {
	// AddLabelIds: A list of IDs of labels to add to this thread.
	AddLabelIds []string `json:"addLabelIds,omitempty"`

	// RemoveLabelIds: A list of IDs of labels to remove from this thread.
	RemoveLabelIds []string `json:"removeLabelIds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AddLabelIds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AddLabelIds") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ModifyThreadRequest) MarshalJSON() ([]byte, error) {
	type noMethod ModifyThreadRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PopSettings: POP settings for an account.
type PopSettings struct {
	// AccessWindow: The range of messages which are accessible via POP.
	//
	// Possible values:
	//   "accessWindowUnspecified"
	//   "allMail"
	//   "disabled"
	//   "fromNowOn"
	AccessWindow string `json:"accessWindow,omitempty"`

	// Disposition: The action that will be executed on a message after it
	// has been fetched via POP.
	//
	// Possible values:
	//   "archive"
	//   "dispositionUnspecified"
	//   "leaveInInbox"
	//   "markRead"
	//   "trash"
	Disposition string `json:"disposition,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AccessWindow") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccessWindow") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PopSettings) MarshalJSON() ([]byte, error) {
	type noMethod PopSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Profile: Profile for a Gmail user.
type Profile struct {
	// EmailAddress: The user's email address.
	EmailAddress string `json:"emailAddress,omitempty"`

	// HistoryId: The ID of the mailbox's current history record.
	HistoryId uint64 `json:"historyId,omitempty,string"`

	// MessagesTotal: The total number of messages in the mailbox.
	MessagesTotal int64 `json:"messagesTotal,omitempty"`

	// ThreadsTotal: The total number of threads in the mailbox.
	ThreadsTotal int64 `json:"threadsTotal,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "EmailAddress") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EmailAddress") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Profile) MarshalJSON() ([]byte, error) {
	type noMethod Profile
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SendAs: Settings associated with a send-as alias, which can be either
// the primary login address associated with the account or a custom
// "from" address. Send-as aliases correspond to the "Send Mail As"
// feature in the web interface.
type SendAs struct {
	// DisplayName: A name that appears in the "From:" header for mail sent
	// using this alias. For custom "from" addresses, when this is empty,
	// Gmail will populate the "From:" header with the name that is used for
	// the primary address associated with the account.
	DisplayName string `json:"displayName,omitempty"`

	// IsDefault: Whether this address is selected as the default "From:"
	// address in situations such as composing a new message or sending a
	// vacation auto-reply. Every Gmail account has exactly one default
	// send-as address, so the only legal value that clients may write to
	// this field is true. Changing this from false to true for an address
	// will result in this field becoming false for the other previous
	// default address.
	IsDefault bool `json:"isDefault,omitempty"`

	// IsPrimary: Whether this address is the primary address used to login
	// to the account. Every Gmail account has exactly one primary address,
	// and it cannot be deleted from the collection of send-as aliases. This
	// field is read-only.
	IsPrimary bool `json:"isPrimary,omitempty"`

	// ReplyToAddress: An optional email address that is included in a
	// "Reply-To:" header for mail sent using this alias. If this is empty,
	// Gmail will not generate a "Reply-To:" header.
	ReplyToAddress string `json:"replyToAddress,omitempty"`

	// SendAsEmail: The email address that appears in the "From:" header for
	// mail sent using this alias. This is read-only for all operations
	// except create.
	SendAsEmail string `json:"sendAsEmail,omitempty"`

	// Signature: An optional HTML signature that is included in messages
	// composed with this alias in the Gmail web UI.
	Signature string `json:"signature,omitempty"`

	// SmtpMsa: An optional SMTP service that will be used as an outbound
	// relay for mail sent using this alias. If this is empty, outbound mail
	// will be sent directly from Gmail's servers to the destination SMTP
	// service. This setting only applies to custom "from" aliases.
	SmtpMsa *SmtpMsa `json:"smtpMsa,omitempty"`

	// TreatAsAlias: Whether Gmail should  treat this address as an alias
	// for the user's primary email address. This setting only applies to
	// custom "from" aliases.
	TreatAsAlias bool `json:"treatAsAlias,omitempty"`

	// VerificationStatus: Indicates whether this address has been verified
	// for use as a send-as alias. Read-only. This setting only applies to
	// custom "from" aliases.
	//
	// Possible values:
	//   "accepted"
	//   "pending"
	//   "verificationStatusUnspecified"
	VerificationStatus string `json:"verificationStatus,omitempty"`

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

func (s *SendAs) MarshalJSON() ([]byte, error) {
	type noMethod SendAs
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SmtpMsa: Configuration for communication with an SMTP service.
type SmtpMsa struct {
	// Host: The hostname of the SMTP service. Required.
	Host string `json:"host,omitempty"`

	// Password: The password that will be used for authentication with the
	// SMTP service. This is a write-only field that can be specified in
	// requests to create or update SendAs settings; it is never populated
	// in responses.
	Password string `json:"password,omitempty"`

	// Port: The port of the SMTP service. Required.
	Port int64 `json:"port,omitempty"`

	// SecurityMode: The protocol that will be used to secure communication
	// with the SMTP service. Required.
	//
	// Possible values:
	//   "none"
	//   "securityModeUnspecified"
	//   "ssl"
	//   "starttls"
	SecurityMode string `json:"securityMode,omitempty"`

	// Username: The username that will be used for authentication with the
	// SMTP service. This is a write-only field that can be specified in
	// requests to create or update SendAs settings; it is never populated
	// in responses.
	Username string `json:"username,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Host") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Host") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SmtpMsa) MarshalJSON() ([]byte, error) {
	type noMethod SmtpMsa
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Thread: A collection of messages representing a conversation.
type Thread struct {
	// HistoryId: The ID of the last history record that modified this
	// thread.
	HistoryId uint64 `json:"historyId,omitempty,string"`

	// Id: The unique ID of the thread.
	Id string `json:"id,omitempty"`

	// Messages: The list of messages in the thread.
	Messages []*Message `json:"messages,omitempty"`

	// Snippet: A short part of the message text.
	Snippet string `json:"snippet,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "HistoryId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HistoryId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Thread) MarshalJSON() ([]byte, error) {
	type noMethod Thread
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VacationSettings: Vacation auto-reply settings for an account. These
// settings correspond to the "Vacation responder" feature in the web
// interface.
type VacationSettings struct {
	// EnableAutoReply: Flag that controls whether Gmail automatically
	// replies to messages.
	EnableAutoReply bool `json:"enableAutoReply,omitempty"`

	// EndTime: An optional end time for sending auto-replies (epoch ms).
	// When this is specified, Gmail will automatically reply only to
	// messages that it receives before the end time. If both startTime and
	// endTime are specified, startTime must precede endTime.
	EndTime int64 `json:"endTime,omitempty,string"`

	// ResponseBodyHtml: Response body in HTML format. Gmail will sanitize
	// the HTML before storing it.
	ResponseBodyHtml string `json:"responseBodyHtml,omitempty"`

	// ResponseBodyPlainText: Response body in plain text format.
	ResponseBodyPlainText string `json:"responseBodyPlainText,omitempty"`

	// ResponseSubject: Optional text to prepend to the subject line in
	// vacation responses. In order to enable auto-replies, either the
	// response subject or the response body must be nonempty.
	ResponseSubject string `json:"responseSubject,omitempty"`

	// RestrictToContacts: Flag that determines whether responses are sent
	// to recipients who are not in the user's list of contacts.
	RestrictToContacts bool `json:"restrictToContacts,omitempty"`

	// RestrictToDomain: Flag that determines whether responses are sent to
	// recipients who are outside of the user's domain. This feature is only
	// available for Google Apps users.
	RestrictToDomain bool `json:"restrictToDomain,omitempty"`

	// StartTime: An optional start time for sending auto-replies (epoch
	// ms). When this is specified, Gmail will automatically reply only to
	// messages that it receives after the start time. If both startTime and
	// endTime are specified, startTime must precede endTime.
	StartTime int64 `json:"startTime,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "EnableAutoReply") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EnableAutoReply") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VacationSettings) MarshalJSON() ([]byte, error) {
	type noMethod VacationSettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WatchRequest: Set up or update a new push notification watch on this
// user's mailbox.
type WatchRequest struct {
	// LabelFilterAction: Filtering behavior of labelIds list specified.
	//
	// Possible values:
	//   "exclude"
	//   "include"
	LabelFilterAction string `json:"labelFilterAction,omitempty"`

	// LabelIds: List of label_ids to restrict notifications about. By
	// default, if unspecified, all changes are pushed out. If specified
	// then dictates which labels are required for a push notification to be
	// generated.
	LabelIds []string `json:"labelIds,omitempty"`

	// TopicName: A fully qualified Google Cloud Pub/Sub API topic name to
	// publish the events to. This topic name **must** already exist in
	// Cloud Pub/Sub and you **must** have already granted gmail "publish"
	// permission on it. For example,
	// "projects/my-project-identifier/topics/my-topic-name" (using the
	// Cloud Pub/Sub "v1" topic naming format).
	//
	// Note that the "my-project-identifier" portion must exactly match your
	// Google developer project id (the one executing this watch request).
	TopicName string `json:"topicName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LabelFilterAction")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LabelFilterAction") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *WatchRequest) MarshalJSON() ([]byte, error) {
	type noMethod WatchRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WatchResponse: Push notification watch response.
type WatchResponse struct {
	// Expiration: When Gmail will stop sending notifications for mailbox
	// updates (epoch millis). Call watch again before this time to renew
	// the watch.
	Expiration int64 `json:"expiration,omitempty,string"`

	// HistoryId: The ID of the mailbox's current history record.
	HistoryId uint64 `json:"historyId,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Expiration") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Expiration") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WatchResponse) MarshalJSON() ([]byte, error) {
	type noMethod WatchResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "gmail.users.getProfile":

type UsersGetProfileCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetProfile: Gets the current user's Gmail profile.
func (r *UsersService) GetProfile(userId string) *UsersGetProfileCall {
	c := &UsersGetProfileCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersGetProfileCall) Fields(s ...googleapi.Field) *UsersGetProfileCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersGetProfileCall) IfNoneMatch(entityTag string) *UsersGetProfileCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersGetProfileCall) Context(ctx context.Context) *UsersGetProfileCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersGetProfileCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersGetProfileCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/profile")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.getProfile" call.
// Exactly one of *Profile or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Profile.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersGetProfileCall) Do(opts ...googleapi.CallOption) (*Profile, error) {
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
	ret := &Profile{
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
	//   "description": "Gets the current user's Gmail profile.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.getProfile",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/profile",
	//   "response": {
	//     "$ref": "Profile"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.stop":

type UsersStopCall struct {
	s          *Service
	userId     string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Stop: Stop receiving push notifications for the given user mailbox.
func (r *UsersService) Stop(userId string) *UsersStopCall {
	c := &UsersStopCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersStopCall) Fields(s ...googleapi.Field) *UsersStopCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersStopCall) Context(ctx context.Context) *UsersStopCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersStopCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersStopCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/stop")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.stop" call.
func (c *UsersStopCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Stop receiving push notifications for the given user mailbox.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.stop",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/stop",
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.watch":

type UsersWatchCall struct {
	s            *Service
	userId       string
	watchrequest *WatchRequest
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Watch: Set up or update a push notification watch on the given user
// mailbox.
func (r *UsersService) Watch(userId string, watchrequest *WatchRequest) *UsersWatchCall {
	c := &UsersWatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.watchrequest = watchrequest
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
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.watchrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/watch")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.watch" call.
// Exactly one of *WatchResponse or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *WatchResponse.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersWatchCall) Do(opts ...googleapi.CallOption) (*WatchResponse, error) {
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
	ret := &WatchResponse{
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
	//   "description": "Set up or update a push notification watch on the given user mailbox.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.watch",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/watch",
	//   "request": {
	//     "$ref": "WatchRequest"
	//   },
	//   "response": {
	//     "$ref": "WatchResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.drafts.create":

type UsersDraftsCreateCall struct {
	s                *Service
	userId           string
	draft            *Draft
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Create: Creates a new draft with the DRAFT label.
func (r *UsersDraftsService) Create(userId string, draft *Draft) *UsersDraftsCreateCall {
	c := &UsersDraftsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.draft = draft
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
func (c *UsersDraftsCreateCall) Media(r io.Reader, options ...googleapi.MediaOption) *UsersDraftsCreateCall {
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
func (c *UsersDraftsCreateCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *UsersDraftsCreateCall {
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
func (c *UsersDraftsCreateCall) ProgressUpdater(pu googleapi.ProgressUpdater) *UsersDraftsCreateCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDraftsCreateCall) Fields(s ...googleapi.Field) *UsersDraftsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *UsersDraftsCreateCall) Context(ctx context.Context) *UsersDraftsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDraftsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDraftsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.draft)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/drafts")
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
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.drafts.create" call.
// Exactly one of *Draft or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Draft.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersDraftsCreateCall) Do(opts ...googleapi.CallOption) (*Draft, error) {
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
	ret := &Draft{
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
	//   "description": "Creates a new draft with the DRAFT label.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.drafts.create",
	//   "mediaUpload": {
	//     "accept": [
	//       "message/rfc822"
	//     ],
	//     "maxSize": "35MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/gmail/v1/users/{userId}/drafts"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/gmail/v1/users/{userId}/drafts"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/drafts",
	//   "request": {
	//     "$ref": "Draft"
	//   },
	//   "response": {
	//     "$ref": "Draft"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "gmail.users.drafts.delete":

type UsersDraftsDeleteCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Immediately and permanently deletes the specified draft. Does
// not simply trash it.
func (r *UsersDraftsService) Delete(userId string, id string) *UsersDraftsDeleteCall {
	c := &UsersDraftsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDraftsDeleteCall) Fields(s ...googleapi.Field) *UsersDraftsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersDraftsDeleteCall) Context(ctx context.Context) *UsersDraftsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDraftsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDraftsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/drafts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.drafts.delete" call.
func (c *UsersDraftsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Immediately and permanently deletes the specified draft. Does not simply trash it.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.drafts.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the draft to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/drafts/{id}",
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.drafts.get":

type UsersDraftsGetCall struct {
	s            *Service
	userId       string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified draft.
func (r *UsersDraftsService) Get(userId string, id string) *UsersDraftsGetCall {
	c := &UsersDraftsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Format sets the optional parameter "format": The format to return the
// draft in.
//
// Possible values:
//   "full" (default)
//   "metadata"
//   "minimal"
//   "raw"
func (c *UsersDraftsGetCall) Format(format string) *UsersDraftsGetCall {
	c.urlParams_.Set("format", format)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDraftsGetCall) Fields(s ...googleapi.Field) *UsersDraftsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersDraftsGetCall) IfNoneMatch(entityTag string) *UsersDraftsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersDraftsGetCall) Context(ctx context.Context) *UsersDraftsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDraftsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDraftsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/drafts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.drafts.get" call.
// Exactly one of *Draft or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Draft.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersDraftsGetCall) Do(opts ...googleapi.CallOption) (*Draft, error) {
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
	ret := &Draft{
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
	//   "description": "Gets the specified draft.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.drafts.get",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "format": {
	//       "default": "full",
	//       "description": "The format to return the draft in.",
	//       "enum": [
	//         "full",
	//         "metadata",
	//         "minimal",
	//         "raw"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "The ID of the draft to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/drafts/{id}",
	//   "response": {
	//     "$ref": "Draft"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.drafts.list":

type UsersDraftsListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the drafts in the user's mailbox.
func (r *UsersDraftsService) List(userId string) *UsersDraftsListCall {
	c := &UsersDraftsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// IncludeSpamTrash sets the optional parameter "includeSpamTrash":
// Include drafts from SPAM and TRASH in the results.
func (c *UsersDraftsListCall) IncludeSpamTrash(includeSpamTrash bool) *UsersDraftsListCall {
	c.urlParams_.Set("includeSpamTrash", fmt.Sprint(includeSpamTrash))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of drafts to return.
func (c *UsersDraftsListCall) MaxResults(maxResults int64) *UsersDraftsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Page token to
// retrieve a specific page of results in the list.
func (c *UsersDraftsListCall) PageToken(pageToken string) *UsersDraftsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Q sets the optional parameter "q": Only return draft messages
// matching the specified query. Supports the same query format as the
// Gmail search box. For example, "from:someuser@example.com
// rfc822msgid: is:unread".
func (c *UsersDraftsListCall) Q(q string) *UsersDraftsListCall {
	c.urlParams_.Set("q", q)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDraftsListCall) Fields(s ...googleapi.Field) *UsersDraftsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersDraftsListCall) IfNoneMatch(entityTag string) *UsersDraftsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersDraftsListCall) Context(ctx context.Context) *UsersDraftsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDraftsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDraftsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/drafts")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.drafts.list" call.
// Exactly one of *ListDraftsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListDraftsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersDraftsListCall) Do(opts ...googleapi.CallOption) (*ListDraftsResponse, error) {
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
	ret := &ListDraftsResponse{
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
	//   "description": "Lists the drafts in the user's mailbox.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.drafts.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "includeSpamTrash": {
	//       "default": "false",
	//       "description": "Include drafts from SPAM and TRASH in the results.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "maxResults": {
	//       "default": "100",
	//       "description": "Maximum number of drafts to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Page token to retrieve a specific page of results in the list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Only return draft messages matching the specified query. Supports the same query format as the Gmail search box. For example, \"from:someuser@example.com rfc822msgid: is:unread\".",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/drafts",
	//   "response": {
	//     "$ref": "ListDraftsResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UsersDraftsListCall) Pages(ctx context.Context, f func(*ListDraftsResponse) error) error {
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

// method id "gmail.users.drafts.send":

type UsersDraftsSendCall struct {
	s                *Service
	userId           string
	draft            *Draft
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Send: Sends the specified, existing draft to the recipients in the
// To, Cc, and Bcc headers.
func (r *UsersDraftsService) Send(userId string, draft *Draft) *UsersDraftsSendCall {
	c := &UsersDraftsSendCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.draft = draft
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
func (c *UsersDraftsSendCall) Media(r io.Reader, options ...googleapi.MediaOption) *UsersDraftsSendCall {
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
func (c *UsersDraftsSendCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *UsersDraftsSendCall {
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
func (c *UsersDraftsSendCall) ProgressUpdater(pu googleapi.ProgressUpdater) *UsersDraftsSendCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDraftsSendCall) Fields(s ...googleapi.Field) *UsersDraftsSendCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *UsersDraftsSendCall) Context(ctx context.Context) *UsersDraftsSendCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDraftsSendCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDraftsSendCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.draft)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/drafts/send")
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
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.drafts.send" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersDraftsSendCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Sends the specified, existing draft to the recipients in the To, Cc, and Bcc headers.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.drafts.send",
	//   "mediaUpload": {
	//     "accept": [
	//       "message/rfc822"
	//     ],
	//     "maxSize": "35MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/gmail/v1/users/{userId}/drafts/send"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/gmail/v1/users/{userId}/drafts/send"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/drafts/send",
	//   "request": {
	//     "$ref": "Draft"
	//   },
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "gmail.users.drafts.update":

type UsersDraftsUpdateCall struct {
	s                *Service
	userId           string
	id               string
	draft            *Draft
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Update: Replaces a draft's content.
func (r *UsersDraftsService) Update(userId string, id string, draft *Draft) *UsersDraftsUpdateCall {
	c := &UsersDraftsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	c.draft = draft
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
func (c *UsersDraftsUpdateCall) Media(r io.Reader, options ...googleapi.MediaOption) *UsersDraftsUpdateCall {
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
func (c *UsersDraftsUpdateCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *UsersDraftsUpdateCall {
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
func (c *UsersDraftsUpdateCall) ProgressUpdater(pu googleapi.ProgressUpdater) *UsersDraftsUpdateCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersDraftsUpdateCall) Fields(s ...googleapi.Field) *UsersDraftsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *UsersDraftsUpdateCall) Context(ctx context.Context) *UsersDraftsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersDraftsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersDraftsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.draft)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/drafts/{id}")
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
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.drafts.update" call.
// Exactly one of *Draft or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Draft.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersDraftsUpdateCall) Do(opts ...googleapi.CallOption) (*Draft, error) {
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
	ret := &Draft{
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
	//   "description": "Replaces a draft's content.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.drafts.update",
	//   "mediaUpload": {
	//     "accept": [
	//       "message/rfc822"
	//     ],
	//     "maxSize": "35MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/gmail/v1/users/{userId}/drafts/{id}"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/gmail/v1/users/{userId}/drafts/{id}"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the draft to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/drafts/{id}",
	//   "request": {
	//     "$ref": "Draft"
	//   },
	//   "response": {
	//     "$ref": "Draft"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "gmail.users.history.list":

type UsersHistoryListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the history of all changes to the given mailbox. History
// results are returned in chronological order (increasing historyId).
func (r *UsersHistoryService) List(userId string) *UsersHistoryListCall {
	c := &UsersHistoryListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// LabelId sets the optional parameter "labelId": Only return messages
// with a label matching the ID.
func (c *UsersHistoryListCall) LabelId(labelId string) *UsersHistoryListCall {
	c.urlParams_.Set("labelId", labelId)
	return c
}

// MaxResults sets the optional parameter "maxResults": The maximum
// number of history records to return.
func (c *UsersHistoryListCall) MaxResults(maxResults int64) *UsersHistoryListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Page token to
// retrieve a specific page of results in the list.
func (c *UsersHistoryListCall) PageToken(pageToken string) *UsersHistoryListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// StartHistoryId sets the optional parameter "startHistoryId":
// Required. Returns history records after the specified startHistoryId.
// The supplied startHistoryId should be obtained from the historyId of
// a message, thread, or previous list response. History IDs increase
// chronologically but are not contiguous with random gaps in between
// valid IDs. Supplying an invalid or out of date startHistoryId
// typically returns an HTTP 404 error code. A historyId is typically
// valid for at least a week, but in some rare circumstances may be
// valid for only a few hours. If you receive an HTTP 404 error
// response, your application should perform a full sync. If you receive
// no nextPageToken in the response, there are no updates to retrieve
// and you can store the returned historyId for a future request.
func (c *UsersHistoryListCall) StartHistoryId(startHistoryId uint64) *UsersHistoryListCall {
	c.urlParams_.Set("startHistoryId", fmt.Sprint(startHistoryId))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersHistoryListCall) Fields(s ...googleapi.Field) *UsersHistoryListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersHistoryListCall) IfNoneMatch(entityTag string) *UsersHistoryListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersHistoryListCall) Context(ctx context.Context) *UsersHistoryListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersHistoryListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersHistoryListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/history")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.history.list" call.
// Exactly one of *ListHistoryResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListHistoryResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersHistoryListCall) Do(opts ...googleapi.CallOption) (*ListHistoryResponse, error) {
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
	ret := &ListHistoryResponse{
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
	//   "description": "Lists the history of all changes to the given mailbox. History results are returned in chronological order (increasing historyId).",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.history.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "labelId": {
	//       "description": "Only return messages with a label matching the ID.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "default": "100",
	//       "description": "The maximum number of history records to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Page token to retrieve a specific page of results in the list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startHistoryId": {
	//       "description": "Required. Returns history records after the specified startHistoryId. The supplied startHistoryId should be obtained from the historyId of a message, thread, or previous list response. History IDs increase chronologically but are not contiguous with random gaps in between valid IDs. Supplying an invalid or out of date startHistoryId typically returns an HTTP 404 error code. A historyId is typically valid for at least a week, but in some rare circumstances may be valid for only a few hours. If you receive an HTTP 404 error response, your application should perform a full sync. If you receive no nextPageToken in the response, there are no updates to retrieve and you can store the returned historyId for a future request.",
	//       "format": "uint64",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/history",
	//   "response": {
	//     "$ref": "ListHistoryResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UsersHistoryListCall) Pages(ctx context.Context, f func(*ListHistoryResponse) error) error {
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

// method id "gmail.users.labels.create":

type UsersLabelsCreateCall struct {
	s          *Service
	userId     string
	label      *Label
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a new label.
func (r *UsersLabelsService) Create(userId string, label *Label) *UsersLabelsCreateCall {
	c := &UsersLabelsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.label = label
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersLabelsCreateCall) Fields(s ...googleapi.Field) *UsersLabelsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersLabelsCreateCall) Context(ctx context.Context) *UsersLabelsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersLabelsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersLabelsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.label)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/labels")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.labels.create" call.
// Exactly one of *Label or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Label.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersLabelsCreateCall) Do(opts ...googleapi.CallOption) (*Label, error) {
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
	ret := &Label{
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
	//   "description": "Creates a new label.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.labels.create",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/labels",
	//   "request": {
	//     "$ref": "Label"
	//   },
	//   "response": {
	//     "$ref": "Label"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.labels",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.labels.delete":

type UsersLabelsDeleteCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Immediately and permanently deletes the specified label and
// removes it from any messages and threads that it is applied to.
func (r *UsersLabelsService) Delete(userId string, id string) *UsersLabelsDeleteCall {
	c := &UsersLabelsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersLabelsDeleteCall) Fields(s ...googleapi.Field) *UsersLabelsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersLabelsDeleteCall) Context(ctx context.Context) *UsersLabelsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersLabelsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersLabelsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/labels/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.labels.delete" call.
func (c *UsersLabelsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Immediately and permanently deletes the specified label and removes it from any messages and threads that it is applied to.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.labels.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the label to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/labels/{id}",
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.labels",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.labels.get":

type UsersLabelsGetCall struct {
	s            *Service
	userId       string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified label.
func (r *UsersLabelsService) Get(userId string, id string) *UsersLabelsGetCall {
	c := &UsersLabelsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersLabelsGetCall) Fields(s ...googleapi.Field) *UsersLabelsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersLabelsGetCall) IfNoneMatch(entityTag string) *UsersLabelsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersLabelsGetCall) Context(ctx context.Context) *UsersLabelsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersLabelsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersLabelsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/labels/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.labels.get" call.
// Exactly one of *Label or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Label.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersLabelsGetCall) Do(opts ...googleapi.CallOption) (*Label, error) {
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
	ret := &Label{
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
	//   "description": "Gets the specified label.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.labels.get",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the label to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/labels/{id}",
	//   "response": {
	//     "$ref": "Label"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.labels",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.labels.list":

type UsersLabelsListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists all labels in the user's mailbox.
func (r *UsersLabelsService) List(userId string) *UsersLabelsListCall {
	c := &UsersLabelsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersLabelsListCall) Fields(s ...googleapi.Field) *UsersLabelsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersLabelsListCall) IfNoneMatch(entityTag string) *UsersLabelsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersLabelsListCall) Context(ctx context.Context) *UsersLabelsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersLabelsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersLabelsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/labels")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.labels.list" call.
// Exactly one of *ListLabelsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListLabelsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersLabelsListCall) Do(opts ...googleapi.CallOption) (*ListLabelsResponse, error) {
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
	ret := &ListLabelsResponse{
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
	//   "description": "Lists all labels in the user's mailbox.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.labels.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/labels",
	//   "response": {
	//     "$ref": "ListLabelsResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.labels",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.labels.patch":

type UsersLabelsPatchCall struct {
	s          *Service
	userId     string
	id         string
	label      *Label
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates the specified label. This method supports patch
// semantics.
func (r *UsersLabelsService) Patch(userId string, id string, label *Label) *UsersLabelsPatchCall {
	c := &UsersLabelsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	c.label = label
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersLabelsPatchCall) Fields(s ...googleapi.Field) *UsersLabelsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersLabelsPatchCall) Context(ctx context.Context) *UsersLabelsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersLabelsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersLabelsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.label)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/labels/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.labels.patch" call.
// Exactly one of *Label or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Label.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersLabelsPatchCall) Do(opts ...googleapi.CallOption) (*Label, error) {
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
	ret := &Label{
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
	//   "description": "Updates the specified label. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "gmail.users.labels.patch",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the label to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/labels/{id}",
	//   "request": {
	//     "$ref": "Label"
	//   },
	//   "response": {
	//     "$ref": "Label"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.labels",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.labels.update":

type UsersLabelsUpdateCall struct {
	s          *Service
	userId     string
	id         string
	label      *Label
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates the specified label.
func (r *UsersLabelsService) Update(userId string, id string, label *Label) *UsersLabelsUpdateCall {
	c := &UsersLabelsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	c.label = label
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersLabelsUpdateCall) Fields(s ...googleapi.Field) *UsersLabelsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersLabelsUpdateCall) Context(ctx context.Context) *UsersLabelsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersLabelsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersLabelsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.label)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/labels/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.labels.update" call.
// Exactly one of *Label or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Label.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersLabelsUpdateCall) Do(opts ...googleapi.CallOption) (*Label, error) {
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
	ret := &Label{
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
	//   "description": "Updates the specified label.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.labels.update",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the label to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/labels/{id}",
	//   "request": {
	//     "$ref": "Label"
	//   },
	//   "response": {
	//     "$ref": "Label"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.labels",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.messages.batchDelete":

type UsersMessagesBatchDeleteCall struct {
	s                          *Service
	userId                     string
	batchdeletemessagesrequest *BatchDeleteMessagesRequest
	urlParams_                 gensupport.URLParams
	ctx_                       context.Context
	header_                    http.Header
}

// BatchDelete: Deletes many messages by message ID. Provides no
// guarantees that messages were not already deleted or even existed at
// all.
func (r *UsersMessagesService) BatchDelete(userId string, batchdeletemessagesrequest *BatchDeleteMessagesRequest) *UsersMessagesBatchDeleteCall {
	c := &UsersMessagesBatchDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.batchdeletemessagesrequest = batchdeletemessagesrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesBatchDeleteCall) Fields(s ...googleapi.Field) *UsersMessagesBatchDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesBatchDeleteCall) Context(ctx context.Context) *UsersMessagesBatchDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesBatchDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesBatchDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.batchdeletemessagesrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/batchDelete")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.batchDelete" call.
func (c *UsersMessagesBatchDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes many messages by message ID. Provides no guarantees that messages were not already deleted or even existed at all.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.batchDelete",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/batchDelete",
	//   "request": {
	//     "$ref": "BatchDeleteMessagesRequest"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/"
	//   ]
	// }

}

// method id "gmail.users.messages.delete":

type UsersMessagesDeleteCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Immediately and permanently deletes the specified message.
// This operation cannot be undone. Prefer messages.trash instead.
func (r *UsersMessagesService) Delete(userId string, id string) *UsersMessagesDeleteCall {
	c := &UsersMessagesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesDeleteCall) Fields(s ...googleapi.Field) *UsersMessagesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesDeleteCall) Context(ctx context.Context) *UsersMessagesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.delete" call.
func (c *UsersMessagesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Immediately and permanently deletes the specified message. This operation cannot be undone. Prefer messages.trash instead.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.messages.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the message to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/{id}",
	//   "scopes": [
	//     "https://mail.google.com/"
	//   ]
	// }

}

// method id "gmail.users.messages.get":

type UsersMessagesGetCall struct {
	s            *Service
	userId       string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified message.
func (r *UsersMessagesService) Get(userId string, id string) *UsersMessagesGetCall {
	c := &UsersMessagesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Format sets the optional parameter "format": The format to return the
// message in.
//
// Possible values:
//   "full" (default)
//   "metadata"
//   "minimal"
//   "raw"
func (c *UsersMessagesGetCall) Format(format string) *UsersMessagesGetCall {
	c.urlParams_.Set("format", format)
	return c
}

// MetadataHeaders sets the optional parameter "metadataHeaders": When
// given and format is METADATA, only include headers specified.
func (c *UsersMessagesGetCall) MetadataHeaders(metadataHeaders ...string) *UsersMessagesGetCall {
	c.urlParams_.SetMulti("metadataHeaders", append([]string{}, metadataHeaders...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesGetCall) Fields(s ...googleapi.Field) *UsersMessagesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersMessagesGetCall) IfNoneMatch(entityTag string) *UsersMessagesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesGetCall) Context(ctx context.Context) *UsersMessagesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.get" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesGetCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Gets the specified message.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.messages.get",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "format": {
	//       "default": "full",
	//       "description": "The format to return the message in.",
	//       "enum": [
	//         "full",
	//         "metadata",
	//         "minimal",
	//         "raw"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "The ID of the message to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "metadataHeaders": {
	//       "description": "When given and format is METADATA, only include headers specified.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/{id}",
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.messages.import":

type UsersMessagesImportCall struct {
	s                *Service
	userId           string
	message          *Message
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Import: Imports a message into only this user's mailbox, with
// standard email delivery scanning and classification similar to
// receiving via SMTP. Does not send a message.
func (r *UsersMessagesService) Import(userId string, message *Message) *UsersMessagesImportCall {
	c := &UsersMessagesImportCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.message = message
	return c
}

// Deleted sets the optional parameter "deleted": Mark the email as
// permanently deleted (not TRASH) and only visible in Google Apps Vault
// to a Vault administrator. Only used for Google Apps for Work
// accounts.
func (c *UsersMessagesImportCall) Deleted(deleted bool) *UsersMessagesImportCall {
	c.urlParams_.Set("deleted", fmt.Sprint(deleted))
	return c
}

// InternalDateSource sets the optional parameter "internalDateSource":
// Source for Gmail's internal date of the message.
//
// Possible values:
//   "dateHeader" (default)
//   "receivedTime"
func (c *UsersMessagesImportCall) InternalDateSource(internalDateSource string) *UsersMessagesImportCall {
	c.urlParams_.Set("internalDateSource", internalDateSource)
	return c
}

// NeverMarkSpam sets the optional parameter "neverMarkSpam": Ignore the
// Gmail spam classifier decision and never mark this email as SPAM in
// the mailbox.
func (c *UsersMessagesImportCall) NeverMarkSpam(neverMarkSpam bool) *UsersMessagesImportCall {
	c.urlParams_.Set("neverMarkSpam", fmt.Sprint(neverMarkSpam))
	return c
}

// ProcessForCalendar sets the optional parameter "processForCalendar":
// Process calendar invites in the email and add any extracted meetings
// to the Google Calendar for this user.
func (c *UsersMessagesImportCall) ProcessForCalendar(processForCalendar bool) *UsersMessagesImportCall {
	c.urlParams_.Set("processForCalendar", fmt.Sprint(processForCalendar))
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
func (c *UsersMessagesImportCall) Media(r io.Reader, options ...googleapi.MediaOption) *UsersMessagesImportCall {
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
func (c *UsersMessagesImportCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *UsersMessagesImportCall {
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
func (c *UsersMessagesImportCall) ProgressUpdater(pu googleapi.ProgressUpdater) *UsersMessagesImportCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesImportCall) Fields(s ...googleapi.Field) *UsersMessagesImportCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *UsersMessagesImportCall) Context(ctx context.Context) *UsersMessagesImportCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesImportCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesImportCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.message)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/import")
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
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.import" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesImportCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Imports a message into only this user's mailbox, with standard email delivery scanning and classification similar to receiving via SMTP. Does not send a message.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.import",
	//   "mediaUpload": {
	//     "accept": [
	//       "message/rfc822"
	//     ],
	//     "maxSize": "35MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/gmail/v1/users/{userId}/messages/import"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/gmail/v1/users/{userId}/messages/import"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "deleted": {
	//       "default": "false",
	//       "description": "Mark the email as permanently deleted (not TRASH) and only visible in Google Apps Vault to a Vault administrator. Only used for Google Apps for Work accounts.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "internalDateSource": {
	//       "default": "dateHeader",
	//       "description": "Source for Gmail's internal date of the message.",
	//       "enum": [
	//         "dateHeader",
	//         "receivedTime"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "neverMarkSpam": {
	//       "default": "false",
	//       "description": "Ignore the Gmail spam classifier decision and never mark this email as SPAM in the mailbox.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "processForCalendar": {
	//       "default": "false",
	//       "description": "Process calendar invites in the email and add any extracted meetings to the Google Calendar for this user.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/import",
	//   "request": {
	//     "$ref": "Message"
	//   },
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.insert",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "gmail.users.messages.insert":

type UsersMessagesInsertCall struct {
	s                *Service
	userId           string
	message          *Message
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Insert: Directly inserts a message into only this user's mailbox
// similar to IMAP APPEND, bypassing most scanning and classification.
// Does not send a message.
func (r *UsersMessagesService) Insert(userId string, message *Message) *UsersMessagesInsertCall {
	c := &UsersMessagesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.message = message
	return c
}

// Deleted sets the optional parameter "deleted": Mark the email as
// permanently deleted (not TRASH) and only visible in Google Apps Vault
// to a Vault administrator. Only used for Google Apps for Work
// accounts.
func (c *UsersMessagesInsertCall) Deleted(deleted bool) *UsersMessagesInsertCall {
	c.urlParams_.Set("deleted", fmt.Sprint(deleted))
	return c
}

// InternalDateSource sets the optional parameter "internalDateSource":
// Source for Gmail's internal date of the message.
//
// Possible values:
//   "dateHeader"
//   "receivedTime" (default)
func (c *UsersMessagesInsertCall) InternalDateSource(internalDateSource string) *UsersMessagesInsertCall {
	c.urlParams_.Set("internalDateSource", internalDateSource)
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
func (c *UsersMessagesInsertCall) Media(r io.Reader, options ...googleapi.MediaOption) *UsersMessagesInsertCall {
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
func (c *UsersMessagesInsertCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *UsersMessagesInsertCall {
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
func (c *UsersMessagesInsertCall) ProgressUpdater(pu googleapi.ProgressUpdater) *UsersMessagesInsertCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesInsertCall) Fields(s ...googleapi.Field) *UsersMessagesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *UsersMessagesInsertCall) Context(ctx context.Context) *UsersMessagesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.message)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages")
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
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.insert" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesInsertCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Directly inserts a message into only this user's mailbox similar to IMAP APPEND, bypassing most scanning and classification. Does not send a message.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.insert",
	//   "mediaUpload": {
	//     "accept": [
	//       "message/rfc822"
	//     ],
	//     "maxSize": "35MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/gmail/v1/users/{userId}/messages"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/gmail/v1/users/{userId}/messages"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "deleted": {
	//       "default": "false",
	//       "description": "Mark the email as permanently deleted (not TRASH) and only visible in Google Apps Vault to a Vault administrator. Only used for Google Apps for Work accounts.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "internalDateSource": {
	//       "default": "receivedTime",
	//       "description": "Source for Gmail's internal date of the message.",
	//       "enum": [
	//         "dateHeader",
	//         "receivedTime"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages",
	//   "request": {
	//     "$ref": "Message"
	//   },
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.insert",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "gmail.users.messages.list":

type UsersMessagesListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the messages in the user's mailbox.
func (r *UsersMessagesService) List(userId string) *UsersMessagesListCall {
	c := &UsersMessagesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// IncludeSpamTrash sets the optional parameter "includeSpamTrash":
// Include messages from SPAM and TRASH in the results.
func (c *UsersMessagesListCall) IncludeSpamTrash(includeSpamTrash bool) *UsersMessagesListCall {
	c.urlParams_.Set("includeSpamTrash", fmt.Sprint(includeSpamTrash))
	return c
}

// LabelIds sets the optional parameter "labelIds": Only return messages
// with labels that match all of the specified label IDs.
func (c *UsersMessagesListCall) LabelIds(labelIds ...string) *UsersMessagesListCall {
	c.urlParams_.SetMulti("labelIds", append([]string{}, labelIds...))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of messages to return.
func (c *UsersMessagesListCall) MaxResults(maxResults int64) *UsersMessagesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Page token to
// retrieve a specific page of results in the list.
func (c *UsersMessagesListCall) PageToken(pageToken string) *UsersMessagesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Q sets the optional parameter "q": Only return messages matching the
// specified query. Supports the same query format as the Gmail search
// box. For example, "from:someuser@example.com rfc822msgid: is:unread".
// Parameter cannot be used when accessing the api using the
// gmail.metadata scope.
func (c *UsersMessagesListCall) Q(q string) *UsersMessagesListCall {
	c.urlParams_.Set("q", q)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesListCall) Fields(s ...googleapi.Field) *UsersMessagesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersMessagesListCall) IfNoneMatch(entityTag string) *UsersMessagesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesListCall) Context(ctx context.Context) *UsersMessagesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.list" call.
// Exactly one of *ListMessagesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListMessagesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersMessagesListCall) Do(opts ...googleapi.CallOption) (*ListMessagesResponse, error) {
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
	ret := &ListMessagesResponse{
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
	//   "description": "Lists the messages in the user's mailbox.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.messages.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "includeSpamTrash": {
	//       "default": "false",
	//       "description": "Include messages from SPAM and TRASH in the results.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "labelIds": {
	//       "description": "Only return messages with labels that match all of the specified label IDs.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "default": "100",
	//       "description": "Maximum number of messages to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Page token to retrieve a specific page of results in the list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Only return messages matching the specified query. Supports the same query format as the Gmail search box. For example, \"from:someuser@example.com rfc822msgid: is:unread\". Parameter cannot be used when accessing the api using the gmail.metadata scope.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages",
	//   "response": {
	//     "$ref": "ListMessagesResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UsersMessagesListCall) Pages(ctx context.Context, f func(*ListMessagesResponse) error) error {
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

// method id "gmail.users.messages.modify":

type UsersMessagesModifyCall struct {
	s                    *Service
	userId               string
	id                   string
	modifymessagerequest *ModifyMessageRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Modify: Modifies the labels on the specified message.
func (r *UsersMessagesService) Modify(userId string, id string, modifymessagerequest *ModifyMessageRequest) *UsersMessagesModifyCall {
	c := &UsersMessagesModifyCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	c.modifymessagerequest = modifymessagerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesModifyCall) Fields(s ...googleapi.Field) *UsersMessagesModifyCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesModifyCall) Context(ctx context.Context) *UsersMessagesModifyCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesModifyCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesModifyCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.modifymessagerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/{id}/modify")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.modify" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesModifyCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Modifies the labels on the specified message.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.modify",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the message to modify.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/{id}/modify",
	//   "request": {
	//     "$ref": "ModifyMessageRequest"
	//   },
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.messages.send":

type UsersMessagesSendCall struct {
	s                *Service
	userId           string
	message          *Message
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// Send: Sends the specified message to the recipients in the To, Cc,
// and Bcc headers.
func (r *UsersMessagesService) Send(userId string, message *Message) *UsersMessagesSendCall {
	c := &UsersMessagesSendCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.message = message
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
func (c *UsersMessagesSendCall) Media(r io.Reader, options ...googleapi.MediaOption) *UsersMessagesSendCall {
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
func (c *UsersMessagesSendCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *UsersMessagesSendCall {
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
func (c *UsersMessagesSendCall) ProgressUpdater(pu googleapi.ProgressUpdater) *UsersMessagesSendCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesSendCall) Fields(s ...googleapi.Field) *UsersMessagesSendCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *UsersMessagesSendCall) Context(ctx context.Context) *UsersMessagesSendCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesSendCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesSendCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.message)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/send")
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
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.send" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesSendCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Sends the specified message to the recipients in the To, Cc, and Bcc headers.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.send",
	//   "mediaUpload": {
	//     "accept": [
	//       "message/rfc822"
	//     ],
	//     "maxSize": "35MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/gmail/v1/users/{userId}/messages/send"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/gmail/v1/users/{userId}/messages/send"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/send",
	//   "request": {
	//     "$ref": "Message"
	//   },
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.compose",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.send"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "gmail.users.messages.trash":

type UsersMessagesTrashCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Trash: Moves the specified message to the trash.
func (r *UsersMessagesService) Trash(userId string, id string) *UsersMessagesTrashCall {
	c := &UsersMessagesTrashCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesTrashCall) Fields(s ...googleapi.Field) *UsersMessagesTrashCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesTrashCall) Context(ctx context.Context) *UsersMessagesTrashCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesTrashCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesTrashCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/{id}/trash")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.trash" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesTrashCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Moves the specified message to the trash.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.trash",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the message to Trash.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/{id}/trash",
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.messages.untrash":

type UsersMessagesUntrashCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Untrash: Removes the specified message from the trash.
func (r *UsersMessagesService) Untrash(userId string, id string) *UsersMessagesUntrashCall {
	c := &UsersMessagesUntrashCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesUntrashCall) Fields(s ...googleapi.Field) *UsersMessagesUntrashCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesUntrashCall) Context(ctx context.Context) *UsersMessagesUntrashCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesUntrashCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesUntrashCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/{id}/untrash")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.untrash" call.
// Exactly one of *Message or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Message.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersMessagesUntrashCall) Do(opts ...googleapi.CallOption) (*Message, error) {
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
	ret := &Message{
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
	//   "description": "Removes the specified message from the trash.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.messages.untrash",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the message to remove from Trash.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/{id}/untrash",
	//   "response": {
	//     "$ref": "Message"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.messages.attachments.get":

type UsersMessagesAttachmentsGetCall struct {
	s            *Service
	userId       string
	messageId    string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified message attachment.
func (r *UsersMessagesAttachmentsService) Get(userId string, messageId string, id string) *UsersMessagesAttachmentsGetCall {
	c := &UsersMessagesAttachmentsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.messageId = messageId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersMessagesAttachmentsGetCall) Fields(s ...googleapi.Field) *UsersMessagesAttachmentsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersMessagesAttachmentsGetCall) IfNoneMatch(entityTag string) *UsersMessagesAttachmentsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersMessagesAttachmentsGetCall) Context(ctx context.Context) *UsersMessagesAttachmentsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersMessagesAttachmentsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersMessagesAttachmentsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/messages/{messageId}/attachments/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":    c.userId,
		"messageId": c.messageId,
		"id":        c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.messages.attachments.get" call.
// Exactly one of *MessagePartBody or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *MessagePartBody.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersMessagesAttachmentsGetCall) Do(opts ...googleapi.CallOption) (*MessagePartBody, error) {
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
	ret := &MessagePartBody{
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
	//   "description": "Gets the specified message attachment.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.messages.attachments.get",
	//   "parameterOrder": [
	//     "userId",
	//     "messageId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the attachment.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "messageId": {
	//       "description": "The ID of the message containing the attachment.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/messages/{messageId}/attachments/{id}",
	//   "response": {
	//     "$ref": "MessagePartBody"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.settings.getAutoForwarding":

type UsersSettingsGetAutoForwardingCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetAutoForwarding: Gets the auto-forwarding setting for the specified
// account.
func (r *UsersSettingsService) GetAutoForwarding(userId string) *UsersSettingsGetAutoForwardingCall {
	c := &UsersSettingsGetAutoForwardingCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsGetAutoForwardingCall) Fields(s ...googleapi.Field) *UsersSettingsGetAutoForwardingCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsGetAutoForwardingCall) IfNoneMatch(entityTag string) *UsersSettingsGetAutoForwardingCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsGetAutoForwardingCall) Context(ctx context.Context) *UsersSettingsGetAutoForwardingCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsGetAutoForwardingCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsGetAutoForwardingCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/autoForwarding")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.getAutoForwarding" call.
// Exactly one of *AutoForwarding or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *AutoForwarding.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsGetAutoForwardingCall) Do(opts ...googleapi.CallOption) (*AutoForwarding, error) {
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
	ret := &AutoForwarding{
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
	//   "description": "Gets the auto-forwarding setting for the specified account.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.getAutoForwarding",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/autoForwarding",
	//   "response": {
	//     "$ref": "AutoForwarding"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.getImap":

type UsersSettingsGetImapCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetImap: Gets IMAP settings.
func (r *UsersSettingsService) GetImap(userId string) *UsersSettingsGetImapCall {
	c := &UsersSettingsGetImapCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsGetImapCall) Fields(s ...googleapi.Field) *UsersSettingsGetImapCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsGetImapCall) IfNoneMatch(entityTag string) *UsersSettingsGetImapCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsGetImapCall) Context(ctx context.Context) *UsersSettingsGetImapCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsGetImapCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsGetImapCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/imap")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.getImap" call.
// Exactly one of *ImapSettings or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ImapSettings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersSettingsGetImapCall) Do(opts ...googleapi.CallOption) (*ImapSettings, error) {
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
	ret := &ImapSettings{
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
	//   "description": "Gets IMAP settings.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.getImap",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/imap",
	//   "response": {
	//     "$ref": "ImapSettings"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.getPop":

type UsersSettingsGetPopCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetPop: Gets POP settings.
func (r *UsersSettingsService) GetPop(userId string) *UsersSettingsGetPopCall {
	c := &UsersSettingsGetPopCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsGetPopCall) Fields(s ...googleapi.Field) *UsersSettingsGetPopCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsGetPopCall) IfNoneMatch(entityTag string) *UsersSettingsGetPopCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsGetPopCall) Context(ctx context.Context) *UsersSettingsGetPopCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsGetPopCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsGetPopCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/pop")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.getPop" call.
// Exactly one of *PopSettings or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *PopSettings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersSettingsGetPopCall) Do(opts ...googleapi.CallOption) (*PopSettings, error) {
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
	ret := &PopSettings{
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
	//   "description": "Gets POP settings.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.getPop",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/pop",
	//   "response": {
	//     "$ref": "PopSettings"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.getVacation":

type UsersSettingsGetVacationCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetVacation: Gets vacation responder settings.
func (r *UsersSettingsService) GetVacation(userId string) *UsersSettingsGetVacationCall {
	c := &UsersSettingsGetVacationCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsGetVacationCall) Fields(s ...googleapi.Field) *UsersSettingsGetVacationCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsGetVacationCall) IfNoneMatch(entityTag string) *UsersSettingsGetVacationCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsGetVacationCall) Context(ctx context.Context) *UsersSettingsGetVacationCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsGetVacationCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsGetVacationCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/vacation")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.getVacation" call.
// Exactly one of *VacationSettings or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *VacationSettings.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsGetVacationCall) Do(opts ...googleapi.CallOption) (*VacationSettings, error) {
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
	ret := &VacationSettings{
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
	//   "description": "Gets vacation responder settings.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.getVacation",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/vacation",
	//   "response": {
	//     "$ref": "VacationSettings"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.updateAutoForwarding":

type UsersSettingsUpdateAutoForwardingCall struct {
	s              *Service
	userId         string
	autoforwarding *AutoForwarding
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// UpdateAutoForwarding: Updates the auto-forwarding setting for the
// specified account. A verified forwarding address must be specified
// when auto-forwarding is enabled.
func (r *UsersSettingsService) UpdateAutoForwarding(userId string, autoforwarding *AutoForwarding) *UsersSettingsUpdateAutoForwardingCall {
	c := &UsersSettingsUpdateAutoForwardingCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.autoforwarding = autoforwarding
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsUpdateAutoForwardingCall) Fields(s ...googleapi.Field) *UsersSettingsUpdateAutoForwardingCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsUpdateAutoForwardingCall) Context(ctx context.Context) *UsersSettingsUpdateAutoForwardingCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsUpdateAutoForwardingCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsUpdateAutoForwardingCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.autoforwarding)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/autoForwarding")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.updateAutoForwarding" call.
// Exactly one of *AutoForwarding or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *AutoForwarding.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsUpdateAutoForwardingCall) Do(opts ...googleapi.CallOption) (*AutoForwarding, error) {
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
	ret := &AutoForwarding{
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
	//   "description": "Updates the auto-forwarding setting for the specified account. A verified forwarding address must be specified when auto-forwarding is enabled.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.settings.updateAutoForwarding",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/autoForwarding",
	//   "request": {
	//     "$ref": "AutoForwarding"
	//   },
	//   "response": {
	//     "$ref": "AutoForwarding"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.updateImap":

type UsersSettingsUpdateImapCall struct {
	s            *Service
	userId       string
	imapsettings *ImapSettings
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// UpdateImap: Updates IMAP settings.
func (r *UsersSettingsService) UpdateImap(userId string, imapsettings *ImapSettings) *UsersSettingsUpdateImapCall {
	c := &UsersSettingsUpdateImapCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.imapsettings = imapsettings
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsUpdateImapCall) Fields(s ...googleapi.Field) *UsersSettingsUpdateImapCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsUpdateImapCall) Context(ctx context.Context) *UsersSettingsUpdateImapCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsUpdateImapCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsUpdateImapCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.imapsettings)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/imap")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.updateImap" call.
// Exactly one of *ImapSettings or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ImapSettings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersSettingsUpdateImapCall) Do(opts ...googleapi.CallOption) (*ImapSettings, error) {
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
	ret := &ImapSettings{
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
	//   "description": "Updates IMAP settings.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.settings.updateImap",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/imap",
	//   "request": {
	//     "$ref": "ImapSettings"
	//   },
	//   "response": {
	//     "$ref": "ImapSettings"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.updatePop":

type UsersSettingsUpdatePopCall struct {
	s           *Service
	userId      string
	popsettings *PopSettings
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// UpdatePop: Updates POP settings.
func (r *UsersSettingsService) UpdatePop(userId string, popsettings *PopSettings) *UsersSettingsUpdatePopCall {
	c := &UsersSettingsUpdatePopCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.popsettings = popsettings
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsUpdatePopCall) Fields(s ...googleapi.Field) *UsersSettingsUpdatePopCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsUpdatePopCall) Context(ctx context.Context) *UsersSettingsUpdatePopCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsUpdatePopCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsUpdatePopCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.popsettings)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/pop")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.updatePop" call.
// Exactly one of *PopSettings or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *PopSettings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UsersSettingsUpdatePopCall) Do(opts ...googleapi.CallOption) (*PopSettings, error) {
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
	ret := &PopSettings{
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
	//   "description": "Updates POP settings.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.settings.updatePop",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/pop",
	//   "request": {
	//     "$ref": "PopSettings"
	//   },
	//   "response": {
	//     "$ref": "PopSettings"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.updateVacation":

type UsersSettingsUpdateVacationCall struct {
	s                *Service
	userId           string
	vacationsettings *VacationSettings
	urlParams_       gensupport.URLParams
	ctx_             context.Context
	header_          http.Header
}

// UpdateVacation: Updates vacation responder settings.
func (r *UsersSettingsService) UpdateVacation(userId string, vacationsettings *VacationSettings) *UsersSettingsUpdateVacationCall {
	c := &UsersSettingsUpdateVacationCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.vacationsettings = vacationsettings
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsUpdateVacationCall) Fields(s ...googleapi.Field) *UsersSettingsUpdateVacationCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsUpdateVacationCall) Context(ctx context.Context) *UsersSettingsUpdateVacationCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsUpdateVacationCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsUpdateVacationCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.vacationsettings)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/vacation")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.updateVacation" call.
// Exactly one of *VacationSettings or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *VacationSettings.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsUpdateVacationCall) Do(opts ...googleapi.CallOption) (*VacationSettings, error) {
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
	ret := &VacationSettings{
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
	//   "description": "Updates vacation responder settings.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.settings.updateVacation",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/vacation",
	//   "request": {
	//     "$ref": "VacationSettings"
	//   },
	//   "response": {
	//     "$ref": "VacationSettings"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.filters.create":

type UsersSettingsFiltersCreateCall struct {
	s          *Service
	userId     string
	filter     *Filter
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a filter.
func (r *UsersSettingsFiltersService) Create(userId string, filter *Filter) *UsersSettingsFiltersCreateCall {
	c := &UsersSettingsFiltersCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.filter = filter
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsFiltersCreateCall) Fields(s ...googleapi.Field) *UsersSettingsFiltersCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsFiltersCreateCall) Context(ctx context.Context) *UsersSettingsFiltersCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsFiltersCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsFiltersCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.filter)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/filters")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.filters.create" call.
// Exactly one of *Filter or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Filter.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersSettingsFiltersCreateCall) Do(opts ...googleapi.CallOption) (*Filter, error) {
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
	ret := &Filter{
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
	//   "description": "Creates a filter.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.settings.filters.create",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/filters",
	//   "request": {
	//     "$ref": "Filter"
	//   },
	//   "response": {
	//     "$ref": "Filter"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.filters.delete":

type UsersSettingsFiltersDeleteCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a filter.
func (r *UsersSettingsFiltersService) Delete(userId string, id string) *UsersSettingsFiltersDeleteCall {
	c := &UsersSettingsFiltersDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsFiltersDeleteCall) Fields(s ...googleapi.Field) *UsersSettingsFiltersDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsFiltersDeleteCall) Context(ctx context.Context) *UsersSettingsFiltersDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsFiltersDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsFiltersDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/filters/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.filters.delete" call.
func (c *UsersSettingsFiltersDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes a filter.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.settings.filters.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the filter to be deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/filters/{id}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.filters.get":

type UsersSettingsFiltersGetCall struct {
	s            *Service
	userId       string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a filter.
func (r *UsersSettingsFiltersService) Get(userId string, id string) *UsersSettingsFiltersGetCall {
	c := &UsersSettingsFiltersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsFiltersGetCall) Fields(s ...googleapi.Field) *UsersSettingsFiltersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsFiltersGetCall) IfNoneMatch(entityTag string) *UsersSettingsFiltersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsFiltersGetCall) Context(ctx context.Context) *UsersSettingsFiltersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsFiltersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsFiltersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/filters/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.filters.get" call.
// Exactly one of *Filter or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Filter.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersSettingsFiltersGetCall) Do(opts ...googleapi.CallOption) (*Filter, error) {
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
	ret := &Filter{
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
	//   "description": "Gets a filter.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.filters.get",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the filter to be fetched.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/filters/{id}",
	//   "response": {
	//     "$ref": "Filter"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.filters.list":

type UsersSettingsFiltersListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the message filters of a Gmail user.
func (r *UsersSettingsFiltersService) List(userId string) *UsersSettingsFiltersListCall {
	c := &UsersSettingsFiltersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsFiltersListCall) Fields(s ...googleapi.Field) *UsersSettingsFiltersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsFiltersListCall) IfNoneMatch(entityTag string) *UsersSettingsFiltersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsFiltersListCall) Context(ctx context.Context) *UsersSettingsFiltersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsFiltersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsFiltersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/filters")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.filters.list" call.
// Exactly one of *ListFiltersResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListFiltersResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsFiltersListCall) Do(opts ...googleapi.CallOption) (*ListFiltersResponse, error) {
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
	ret := &ListFiltersResponse{
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
	//   "description": "Lists the message filters of a Gmail user.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.filters.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/filters",
	//   "response": {
	//     "$ref": "ListFiltersResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.forwardingAddresses.create":

type UsersSettingsForwardingAddressesCreateCall struct {
	s                 *Service
	userId            string
	forwardingaddress *ForwardingAddress
	urlParams_        gensupport.URLParams
	ctx_              context.Context
	header_           http.Header
}

// Create: Creates a forwarding address. If ownership verification is
// required, a message will be sent to the recipient and the resource's
// verification status will be set to pending; otherwise, the resource
// will be created with verification status set to accepted.
func (r *UsersSettingsForwardingAddressesService) Create(userId string, forwardingaddress *ForwardingAddress) *UsersSettingsForwardingAddressesCreateCall {
	c := &UsersSettingsForwardingAddressesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.forwardingaddress = forwardingaddress
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsForwardingAddressesCreateCall) Fields(s ...googleapi.Field) *UsersSettingsForwardingAddressesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsForwardingAddressesCreateCall) Context(ctx context.Context) *UsersSettingsForwardingAddressesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsForwardingAddressesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsForwardingAddressesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.forwardingaddress)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/forwardingAddresses")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.forwardingAddresses.create" call.
// Exactly one of *ForwardingAddress or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ForwardingAddress.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsForwardingAddressesCreateCall) Do(opts ...googleapi.CallOption) (*ForwardingAddress, error) {
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
	ret := &ForwardingAddress{
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
	//   "description": "Creates a forwarding address. If ownership verification is required, a message will be sent to the recipient and the resource's verification status will be set to pending; otherwise, the resource will be created with verification status set to accepted.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.settings.forwardingAddresses.create",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/forwardingAddresses",
	//   "request": {
	//     "$ref": "ForwardingAddress"
	//   },
	//   "response": {
	//     "$ref": "ForwardingAddress"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.forwardingAddresses.delete":

type UsersSettingsForwardingAddressesDeleteCall struct {
	s               *Service
	userId          string
	forwardingEmail string
	urlParams_      gensupport.URLParams
	ctx_            context.Context
	header_         http.Header
}

// Delete: Deletes the specified forwarding address and revokes any
// verification that may have been required.
func (r *UsersSettingsForwardingAddressesService) Delete(userId string, forwardingEmail string) *UsersSettingsForwardingAddressesDeleteCall {
	c := &UsersSettingsForwardingAddressesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.forwardingEmail = forwardingEmail
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsForwardingAddressesDeleteCall) Fields(s ...googleapi.Field) *UsersSettingsForwardingAddressesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsForwardingAddressesDeleteCall) Context(ctx context.Context) *UsersSettingsForwardingAddressesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsForwardingAddressesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsForwardingAddressesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/forwardingAddresses/{forwardingEmail}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":          c.userId,
		"forwardingEmail": c.forwardingEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.forwardingAddresses.delete" call.
func (c *UsersSettingsForwardingAddressesDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes the specified forwarding address and revokes any verification that may have been required.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.settings.forwardingAddresses.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "forwardingEmail"
	//   ],
	//   "parameters": {
	//     "forwardingEmail": {
	//       "description": "The forwarding address to be deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/forwardingAddresses/{forwardingEmail}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.forwardingAddresses.get":

type UsersSettingsForwardingAddressesGetCall struct {
	s               *Service
	userId          string
	forwardingEmail string
	urlParams_      gensupport.URLParams
	ifNoneMatch_    string
	ctx_            context.Context
	header_         http.Header
}

// Get: Gets the specified forwarding address.
func (r *UsersSettingsForwardingAddressesService) Get(userId string, forwardingEmail string) *UsersSettingsForwardingAddressesGetCall {
	c := &UsersSettingsForwardingAddressesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.forwardingEmail = forwardingEmail
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsForwardingAddressesGetCall) Fields(s ...googleapi.Field) *UsersSettingsForwardingAddressesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsForwardingAddressesGetCall) IfNoneMatch(entityTag string) *UsersSettingsForwardingAddressesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsForwardingAddressesGetCall) Context(ctx context.Context) *UsersSettingsForwardingAddressesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsForwardingAddressesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsForwardingAddressesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/forwardingAddresses/{forwardingEmail}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":          c.userId,
		"forwardingEmail": c.forwardingEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.forwardingAddresses.get" call.
// Exactly one of *ForwardingAddress or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ForwardingAddress.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsForwardingAddressesGetCall) Do(opts ...googleapi.CallOption) (*ForwardingAddress, error) {
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
	ret := &ForwardingAddress{
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
	//   "description": "Gets the specified forwarding address.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.forwardingAddresses.get",
	//   "parameterOrder": [
	//     "userId",
	//     "forwardingEmail"
	//   ],
	//   "parameters": {
	//     "forwardingEmail": {
	//       "description": "The forwarding address to be retrieved.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/forwardingAddresses/{forwardingEmail}",
	//   "response": {
	//     "$ref": "ForwardingAddress"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.forwardingAddresses.list":

type UsersSettingsForwardingAddressesListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the forwarding addresses for the specified account.
func (r *UsersSettingsForwardingAddressesService) List(userId string) *UsersSettingsForwardingAddressesListCall {
	c := &UsersSettingsForwardingAddressesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsForwardingAddressesListCall) Fields(s ...googleapi.Field) *UsersSettingsForwardingAddressesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsForwardingAddressesListCall) IfNoneMatch(entityTag string) *UsersSettingsForwardingAddressesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsForwardingAddressesListCall) Context(ctx context.Context) *UsersSettingsForwardingAddressesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsForwardingAddressesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsForwardingAddressesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/forwardingAddresses")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.forwardingAddresses.list" call.
// Exactly one of *ListForwardingAddressesResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ListForwardingAddressesResponse.ServerResponse.Header or (if
// a response was returned at all) in error.(*googleapi.Error).Header.
// Use googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsForwardingAddressesListCall) Do(opts ...googleapi.CallOption) (*ListForwardingAddressesResponse, error) {
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
	ret := &ListForwardingAddressesResponse{
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
	//   "description": "Lists the forwarding addresses for the specified account.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.forwardingAddresses.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/forwardingAddresses",
	//   "response": {
	//     "$ref": "ListForwardingAddressesResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.create":

type UsersSettingsSendAsCreateCall struct {
	s          *Service
	userId     string
	sendas     *SendAs
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a custom "from" send-as alias. If an SMTP MSA is
// specified, Gmail will attempt to connect to the SMTP service to
// validate the configuration before creating the alias. If ownership
// verification is required for the alias, a message will be sent to the
// email address and the resource's verification status will be set to
// pending; otherwise, the resource will be created with verification
// status set to accepted. If a signature is provided, Gmail will
// sanitize the HTML before saving it with the alias.
func (r *UsersSettingsSendAsService) Create(userId string, sendas *SendAs) *UsersSettingsSendAsCreateCall {
	c := &UsersSettingsSendAsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.sendas = sendas
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsCreateCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsCreateCall) Context(ctx context.Context) *UsersSettingsSendAsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.sendas)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.create" call.
// Exactly one of *SendAs or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *SendAs.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersSettingsSendAsCreateCall) Do(opts ...googleapi.CallOption) (*SendAs, error) {
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
	ret := &SendAs{
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
	//   "description": "Creates a custom \"from\" send-as alias. If an SMTP MSA is specified, Gmail will attempt to connect to the SMTP service to validate the configuration before creating the alias. If ownership verification is required for the alias, a message will be sent to the email address and the resource's verification status will be set to pending; otherwise, the resource will be created with verification status set to accepted. If a signature is provided, Gmail will sanitize the HTML before saving it with the alias.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.settings.sendAs.create",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs",
	//   "request": {
	//     "$ref": "SendAs"
	//   },
	//   "response": {
	//     "$ref": "SendAs"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.delete":

type UsersSettingsSendAsDeleteCall struct {
	s           *Service
	userId      string
	sendAsEmail string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Delete: Deletes the specified send-as alias. Revokes any verification
// that may have been required for using it.
func (r *UsersSettingsSendAsService) Delete(userId string, sendAsEmail string) *UsersSettingsSendAsDeleteCall {
	c := &UsersSettingsSendAsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.sendAsEmail = sendAsEmail
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsDeleteCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsDeleteCall) Context(ctx context.Context) *UsersSettingsSendAsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs/{sendAsEmail}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":      c.userId,
		"sendAsEmail": c.sendAsEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.delete" call.
func (c *UsersSettingsSendAsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes the specified send-as alias. Revokes any verification that may have been required for using it.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.settings.sendAs.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "sendAsEmail"
	//   ],
	//   "parameters": {
	//     "sendAsEmail": {
	//       "description": "The send-as alias to be deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs/{sendAsEmail}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.get":

type UsersSettingsSendAsGetCall struct {
	s            *Service
	userId       string
	sendAsEmail  string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified send-as alias. Fails with an HTTP 404 error
// if the specified address is not a member of the collection.
func (r *UsersSettingsSendAsService) Get(userId string, sendAsEmail string) *UsersSettingsSendAsGetCall {
	c := &UsersSettingsSendAsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.sendAsEmail = sendAsEmail
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsGetCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsSendAsGetCall) IfNoneMatch(entityTag string) *UsersSettingsSendAsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsGetCall) Context(ctx context.Context) *UsersSettingsSendAsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs/{sendAsEmail}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":      c.userId,
		"sendAsEmail": c.sendAsEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.get" call.
// Exactly one of *SendAs or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *SendAs.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersSettingsSendAsGetCall) Do(opts ...googleapi.CallOption) (*SendAs, error) {
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
	ret := &SendAs{
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
	//   "description": "Gets the specified send-as alias. Fails with an HTTP 404 error if the specified address is not a member of the collection.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.sendAs.get",
	//   "parameterOrder": [
	//     "userId",
	//     "sendAsEmail"
	//   ],
	//   "parameters": {
	//     "sendAsEmail": {
	//       "description": "The send-as alias to be retrieved.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs/{sendAsEmail}",
	//   "response": {
	//     "$ref": "SendAs"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.list":

type UsersSettingsSendAsListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the send-as aliases for the specified account. The result
// includes the primary send-as address associated with the account as
// well as any custom "from" aliases.
func (r *UsersSettingsSendAsService) List(userId string) *UsersSettingsSendAsListCall {
	c := &UsersSettingsSendAsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsListCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersSettingsSendAsListCall) IfNoneMatch(entityTag string) *UsersSettingsSendAsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsListCall) Context(ctx context.Context) *UsersSettingsSendAsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.list" call.
// Exactly one of *ListSendAsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListSendAsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersSettingsSendAsListCall) Do(opts ...googleapi.CallOption) (*ListSendAsResponse, error) {
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
	ret := &ListSendAsResponse{
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
	//   "description": "Lists the send-as aliases for the specified account. The result includes the primary send-as address associated with the account as well as any custom \"from\" aliases.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.settings.sendAs.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs",
	//   "response": {
	//     "$ref": "ListSendAsResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly",
	//     "https://www.googleapis.com/auth/gmail.settings.basic"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.patch":

type UsersSettingsSendAsPatchCall struct {
	s           *Service
	userId      string
	sendAsEmail string
	sendas      *SendAs
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Patch: Updates a send-as alias. If a signature is provided, Gmail
// will sanitize the HTML before saving it with the alias. This method
// supports patch semantics.
func (r *UsersSettingsSendAsService) Patch(userId string, sendAsEmail string, sendas *SendAs) *UsersSettingsSendAsPatchCall {
	c := &UsersSettingsSendAsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.sendAsEmail = sendAsEmail
	c.sendas = sendas
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsPatchCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsPatchCall) Context(ctx context.Context) *UsersSettingsSendAsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.sendas)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs/{sendAsEmail}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":      c.userId,
		"sendAsEmail": c.sendAsEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.patch" call.
// Exactly one of *SendAs or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *SendAs.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersSettingsSendAsPatchCall) Do(opts ...googleapi.CallOption) (*SendAs, error) {
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
	ret := &SendAs{
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
	//   "description": "Updates a send-as alias. If a signature is provided, Gmail will sanitize the HTML before saving it with the alias. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "gmail.users.settings.sendAs.patch",
	//   "parameterOrder": [
	//     "userId",
	//     "sendAsEmail"
	//   ],
	//   "parameters": {
	//     "sendAsEmail": {
	//       "description": "The send-as alias to be updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs/{sendAsEmail}",
	//   "request": {
	//     "$ref": "SendAs"
	//   },
	//   "response": {
	//     "$ref": "SendAs"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic",
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.update":

type UsersSettingsSendAsUpdateCall struct {
	s           *Service
	userId      string
	sendAsEmail string
	sendas      *SendAs
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Update: Updates a send-as alias. If a signature is provided, Gmail
// will sanitize the HTML before saving it with the alias.
func (r *UsersSettingsSendAsService) Update(userId string, sendAsEmail string, sendas *SendAs) *UsersSettingsSendAsUpdateCall {
	c := &UsersSettingsSendAsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.sendAsEmail = sendAsEmail
	c.sendas = sendas
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsUpdateCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsUpdateCall) Context(ctx context.Context) *UsersSettingsSendAsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.sendas)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs/{sendAsEmail}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":      c.userId,
		"sendAsEmail": c.sendAsEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.update" call.
// Exactly one of *SendAs or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *SendAs.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersSettingsSendAsUpdateCall) Do(opts ...googleapi.CallOption) (*SendAs, error) {
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
	ret := &SendAs{
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
	//   "description": "Updates a send-as alias. If a signature is provided, Gmail will sanitize the HTML before saving it with the alias.",
	//   "httpMethod": "PUT",
	//   "id": "gmail.users.settings.sendAs.update",
	//   "parameterOrder": [
	//     "userId",
	//     "sendAsEmail"
	//   ],
	//   "parameters": {
	//     "sendAsEmail": {
	//       "description": "The send-as alias to be updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs/{sendAsEmail}",
	//   "request": {
	//     "$ref": "SendAs"
	//   },
	//   "response": {
	//     "$ref": "SendAs"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.basic",
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.settings.sendAs.verify":

type UsersSettingsSendAsVerifyCall struct {
	s           *Service
	userId      string
	sendAsEmail string
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Verify: Sends a verification email to the specified send-as alias
// address. The verification status must be pending.
func (r *UsersSettingsSendAsService) Verify(userId string, sendAsEmail string) *UsersSettingsSendAsVerifyCall {
	c := &UsersSettingsSendAsVerifyCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.sendAsEmail = sendAsEmail
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersSettingsSendAsVerifyCall) Fields(s ...googleapi.Field) *UsersSettingsSendAsVerifyCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersSettingsSendAsVerifyCall) Context(ctx context.Context) *UsersSettingsSendAsVerifyCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersSettingsSendAsVerifyCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersSettingsSendAsVerifyCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/settings/sendAs/{sendAsEmail}/verify")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId":      c.userId,
		"sendAsEmail": c.sendAsEmail,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.settings.sendAs.verify" call.
func (c *UsersSettingsSendAsVerifyCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Sends a verification email to the specified send-as alias address. The verification status must be pending.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.settings.sendAs.verify",
	//   "parameterOrder": [
	//     "userId",
	//     "sendAsEmail"
	//   ],
	//   "parameters": {
	//     "sendAsEmail": {
	//       "description": "The send-as alias to be verified.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "User's email address. The special value \"me\" can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/settings/sendAs/{sendAsEmail}/verify",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/gmail.settings.sharing"
	//   ]
	// }

}

// method id "gmail.users.threads.delete":

type UsersThreadsDeleteCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Immediately and permanently deletes the specified thread.
// This operation cannot be undone. Prefer threads.trash instead.
func (r *UsersThreadsService) Delete(userId string, id string) *UsersThreadsDeleteCall {
	c := &UsersThreadsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersThreadsDeleteCall) Fields(s ...googleapi.Field) *UsersThreadsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersThreadsDeleteCall) Context(ctx context.Context) *UsersThreadsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersThreadsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersThreadsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/threads/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.threads.delete" call.
func (c *UsersThreadsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Immediately and permanently deletes the specified thread. This operation cannot be undone. Prefer threads.trash instead.",
	//   "httpMethod": "DELETE",
	//   "id": "gmail.users.threads.delete",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "ID of the Thread to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/threads/{id}",
	//   "scopes": [
	//     "https://mail.google.com/"
	//   ]
	// }

}

// method id "gmail.users.threads.get":

type UsersThreadsGetCall struct {
	s            *Service
	userId       string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the specified thread.
func (r *UsersThreadsService) Get(userId string, id string) *UsersThreadsGetCall {
	c := &UsersThreadsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Format sets the optional parameter "format": The format to return the
// messages in.
//
// Possible values:
//   "full" (default)
//   "metadata"
//   "minimal"
func (c *UsersThreadsGetCall) Format(format string) *UsersThreadsGetCall {
	c.urlParams_.Set("format", format)
	return c
}

// MetadataHeaders sets the optional parameter "metadataHeaders": When
// given and format is METADATA, only include headers specified.
func (c *UsersThreadsGetCall) MetadataHeaders(metadataHeaders ...string) *UsersThreadsGetCall {
	c.urlParams_.SetMulti("metadataHeaders", append([]string{}, metadataHeaders...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersThreadsGetCall) Fields(s ...googleapi.Field) *UsersThreadsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersThreadsGetCall) IfNoneMatch(entityTag string) *UsersThreadsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersThreadsGetCall) Context(ctx context.Context) *UsersThreadsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersThreadsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersThreadsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/threads/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.threads.get" call.
// Exactly one of *Thread or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Thread.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersThreadsGetCall) Do(opts ...googleapi.CallOption) (*Thread, error) {
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
	ret := &Thread{
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
	//   "description": "Gets the specified thread.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.threads.get",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "format": {
	//       "default": "full",
	//       "description": "The format to return the messages in.",
	//       "enum": [
	//         "full",
	//         "metadata",
	//         "minimal"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "The ID of the thread to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "metadataHeaders": {
	//       "description": "When given and format is METADATA, only include headers specified.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/threads/{id}",
	//   "response": {
	//     "$ref": "Thread"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// method id "gmail.users.threads.list":

type UsersThreadsListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Lists the threads in the user's mailbox.
func (r *UsersThreadsService) List(userId string) *UsersThreadsListCall {
	c := &UsersThreadsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// IncludeSpamTrash sets the optional parameter "includeSpamTrash":
// Include threads from SPAM and TRASH in the results.
func (c *UsersThreadsListCall) IncludeSpamTrash(includeSpamTrash bool) *UsersThreadsListCall {
	c.urlParams_.Set("includeSpamTrash", fmt.Sprint(includeSpamTrash))
	return c
}

// LabelIds sets the optional parameter "labelIds": Only return threads
// with labels that match all of the specified label IDs.
func (c *UsersThreadsListCall) LabelIds(labelIds ...string) *UsersThreadsListCall {
	c.urlParams_.SetMulti("labelIds", append([]string{}, labelIds...))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of threads to return.
func (c *UsersThreadsListCall) MaxResults(maxResults int64) *UsersThreadsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Page token to
// retrieve a specific page of results in the list.
func (c *UsersThreadsListCall) PageToken(pageToken string) *UsersThreadsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Q sets the optional parameter "q": Only return threads matching the
// specified query. Supports the same query format as the Gmail search
// box. For example, "from:someuser@example.com rfc822msgid: is:unread".
// Parameter cannot be used when accessing the api using the
// gmail.metadata scope.
func (c *UsersThreadsListCall) Q(q string) *UsersThreadsListCall {
	c.urlParams_.Set("q", q)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersThreadsListCall) Fields(s ...googleapi.Field) *UsersThreadsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UsersThreadsListCall) IfNoneMatch(entityTag string) *UsersThreadsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersThreadsListCall) Context(ctx context.Context) *UsersThreadsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersThreadsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersThreadsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/threads")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.threads.list" call.
// Exactly one of *ListThreadsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListThreadsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UsersThreadsListCall) Do(opts ...googleapi.CallOption) (*ListThreadsResponse, error) {
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
	ret := &ListThreadsResponse{
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
	//   "description": "Lists the threads in the user's mailbox.",
	//   "httpMethod": "GET",
	//   "id": "gmail.users.threads.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "includeSpamTrash": {
	//       "default": "false",
	//       "description": "Include threads from SPAM and TRASH in the results.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "labelIds": {
	//       "description": "Only return threads with labels that match all of the specified label IDs.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "default": "100",
	//       "description": "Maximum number of threads to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Page token to retrieve a specific page of results in the list.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Only return threads matching the specified query. Supports the same query format as the Gmail search box. For example, \"from:someuser@example.com rfc822msgid: is:unread\". Parameter cannot be used when accessing the api using the gmail.metadata scope.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/threads",
	//   "response": {
	//     "$ref": "ListThreadsResponse"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.metadata",
	//     "https://www.googleapis.com/auth/gmail.modify",
	//     "https://www.googleapis.com/auth/gmail.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UsersThreadsListCall) Pages(ctx context.Context, f func(*ListThreadsResponse) error) error {
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

// method id "gmail.users.threads.modify":

type UsersThreadsModifyCall struct {
	s                   *Service
	userId              string
	id                  string
	modifythreadrequest *ModifyThreadRequest
	urlParams_          gensupport.URLParams
	ctx_                context.Context
	header_             http.Header
}

// Modify: Modifies the labels applied to the thread. This applies to
// all messages in the thread.
func (r *UsersThreadsService) Modify(userId string, id string, modifythreadrequest *ModifyThreadRequest) *UsersThreadsModifyCall {
	c := &UsersThreadsModifyCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	c.modifythreadrequest = modifythreadrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersThreadsModifyCall) Fields(s ...googleapi.Field) *UsersThreadsModifyCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersThreadsModifyCall) Context(ctx context.Context) *UsersThreadsModifyCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersThreadsModifyCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersThreadsModifyCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.modifythreadrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/threads/{id}/modify")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.threads.modify" call.
// Exactly one of *Thread or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Thread.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersThreadsModifyCall) Do(opts ...googleapi.CallOption) (*Thread, error) {
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
	ret := &Thread{
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
	//   "description": "Modifies the labels applied to the thread. This applies to all messages in the thread.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.threads.modify",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the thread to modify.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/threads/{id}/modify",
	//   "request": {
	//     "$ref": "ModifyThreadRequest"
	//   },
	//   "response": {
	//     "$ref": "Thread"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.threads.trash":

type UsersThreadsTrashCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Trash: Moves the specified thread to the trash.
func (r *UsersThreadsService) Trash(userId string, id string) *UsersThreadsTrashCall {
	c := &UsersThreadsTrashCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersThreadsTrashCall) Fields(s ...googleapi.Field) *UsersThreadsTrashCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersThreadsTrashCall) Context(ctx context.Context) *UsersThreadsTrashCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersThreadsTrashCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersThreadsTrashCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/threads/{id}/trash")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.threads.trash" call.
// Exactly one of *Thread or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Thread.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersThreadsTrashCall) Do(opts ...googleapi.CallOption) (*Thread, error) {
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
	ret := &Thread{
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
	//   "description": "Moves the specified thread to the trash.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.threads.trash",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the thread to Trash.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/threads/{id}/trash",
	//   "response": {
	//     "$ref": "Thread"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

// method id "gmail.users.threads.untrash":

type UsersThreadsUntrashCall struct {
	s          *Service
	userId     string
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Untrash: Removes the specified thread from the trash.
func (r *UsersThreadsService) Untrash(userId string, id string) *UsersThreadsUntrashCall {
	c := &UsersThreadsUntrashCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UsersThreadsUntrashCall) Fields(s ...googleapi.Field) *UsersThreadsUntrashCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UsersThreadsUntrashCall) Context(ctx context.Context) *UsersThreadsUntrashCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UsersThreadsUntrashCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UsersThreadsUntrashCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "{userId}/threads/{id}/untrash")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"id":     c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "gmail.users.threads.untrash" call.
// Exactly one of *Thread or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Thread.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UsersThreadsUntrashCall) Do(opts ...googleapi.CallOption) (*Thread, error) {
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
	ret := &Thread{
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
	//   "description": "Removes the specified thread from the trash.",
	//   "httpMethod": "POST",
	//   "id": "gmail.users.threads.untrash",
	//   "parameterOrder": [
	//     "userId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The ID of the thread to remove from Trash.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "default": "me",
	//       "description": "The user's email address. The special value me can be used to indicate the authenticated user.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "{userId}/threads/{id}/untrash",
	//   "response": {
	//     "$ref": "Thread"
	//   },
	//   "scopes": [
	//     "https://mail.google.com/",
	//     "https://www.googleapis.com/auth/gmail.modify"
	//   ]
	// }

}

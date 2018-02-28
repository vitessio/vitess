// Package adexchangebuyer provides access to the Ad Exchange Buyer API.
//
// See https://developers.google.com/ad-exchange/buyer-rest
//
// Usage example:
//
//   import "google.golang.org/api/adexchangebuyer/v1.4"
//   ...
//   adexchangebuyerService, err := adexchangebuyer.New(oauthHttpClient)
package adexchangebuyer // import "google.golang.org/api/adexchangebuyer/v1.4"

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

const apiId = "adexchangebuyer:v1.4"
const apiName = "adexchangebuyer"
const apiVersion = "v1.4"
const basePath = "https://www.googleapis.com/adexchangebuyer/v1.4/"

// OAuth2 scopes used by this API.
const (
	// Manage your Ad Exchange buyer account configuration
	AdexchangeBuyerScope = "https://www.googleapis.com/auth/adexchange.buyer"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Accounts = NewAccountsService(s)
	s.BillingInfo = NewBillingInfoService(s)
	s.Budget = NewBudgetService(s)
	s.Creatives = NewCreativesService(s)
	s.Marketplacedeals = NewMarketplacedealsService(s)
	s.Marketplacenotes = NewMarketplacenotesService(s)
	s.Marketplaceprivateauction = NewMarketplaceprivateauctionService(s)
	s.PerformanceReport = NewPerformanceReportService(s)
	s.PretargetingConfig = NewPretargetingConfigService(s)
	s.Products = NewProductsService(s)
	s.Proposals = NewProposalsService(s)
	s.Pubprofiles = NewPubprofilesService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Accounts *AccountsService

	BillingInfo *BillingInfoService

	Budget *BudgetService

	Creatives *CreativesService

	Marketplacedeals *MarketplacedealsService

	Marketplacenotes *MarketplacenotesService

	Marketplaceprivateauction *MarketplaceprivateauctionService

	PerformanceReport *PerformanceReportService

	PretargetingConfig *PretargetingConfigService

	Products *ProductsService

	Proposals *ProposalsService

	Pubprofiles *PubprofilesService
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

func NewBillingInfoService(s *Service) *BillingInfoService {
	rs := &BillingInfoService{s: s}
	return rs
}

type BillingInfoService struct {
	s *Service
}

func NewBudgetService(s *Service) *BudgetService {
	rs := &BudgetService{s: s}
	return rs
}

type BudgetService struct {
	s *Service
}

func NewCreativesService(s *Service) *CreativesService {
	rs := &CreativesService{s: s}
	return rs
}

type CreativesService struct {
	s *Service
}

func NewMarketplacedealsService(s *Service) *MarketplacedealsService {
	rs := &MarketplacedealsService{s: s}
	return rs
}

type MarketplacedealsService struct {
	s *Service
}

func NewMarketplacenotesService(s *Service) *MarketplacenotesService {
	rs := &MarketplacenotesService{s: s}
	return rs
}

type MarketplacenotesService struct {
	s *Service
}

func NewMarketplaceprivateauctionService(s *Service) *MarketplaceprivateauctionService {
	rs := &MarketplaceprivateauctionService{s: s}
	return rs
}

type MarketplaceprivateauctionService struct {
	s *Service
}

func NewPerformanceReportService(s *Service) *PerformanceReportService {
	rs := &PerformanceReportService{s: s}
	return rs
}

type PerformanceReportService struct {
	s *Service
}

func NewPretargetingConfigService(s *Service) *PretargetingConfigService {
	rs := &PretargetingConfigService{s: s}
	return rs
}

type PretargetingConfigService struct {
	s *Service
}

func NewProductsService(s *Service) *ProductsService {
	rs := &ProductsService{s: s}
	return rs
}

type ProductsService struct {
	s *Service
}

func NewProposalsService(s *Service) *ProposalsService {
	rs := &ProposalsService{s: s}
	return rs
}

type ProposalsService struct {
	s *Service
}

func NewPubprofilesService(s *Service) *PubprofilesService {
	rs := &PubprofilesService{s: s}
	return rs
}

type PubprofilesService struct {
	s *Service
}

// Account: Configuration data for an Ad Exchange buyer account.
type Account struct {
	// BidderLocation: Your bidder locations that have distinct URLs.
	BidderLocation []*AccountBidderLocation `json:"bidderLocation,omitempty"`

	// CookieMatchingNid: The nid parameter value used in cookie match
	// requests. Please contact your technical account manager if you need
	// to change this.
	CookieMatchingNid string `json:"cookieMatchingNid,omitempty"`

	// CookieMatchingUrl: The base URL used in cookie match requests.
	CookieMatchingUrl string `json:"cookieMatchingUrl,omitempty"`

	// Id: Account id.
	Id int64 `json:"id,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// MaximumActiveCreatives: The maximum number of active creatives that
	// an account can have, where a creative is active if it was inserted or
	// bid with in the last 30 days. Please contact your technical account
	// manager if you need to change this.
	MaximumActiveCreatives int64 `json:"maximumActiveCreatives,omitempty"`

	// MaximumTotalQps: The sum of all bidderLocation.maximumQps values
	// cannot exceed this. Please contact your technical account manager if
	// you need to change this.
	MaximumTotalQps int64 `json:"maximumTotalQps,omitempty"`

	// NumberActiveCreatives: The number of creatives that this account
	// inserted or bid with in the last 30 days.
	NumberActiveCreatives int64 `json:"numberActiveCreatives,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BidderLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BidderLocation") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Account) MarshalJSON() ([]byte, error) {
	type noMethod Account
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AccountBidderLocation struct {
	// BidProtocol: The protocol that the bidder endpoint is using. By
	// default, OpenRTB protocols use JSON, except
	// PROTOCOL_OPENRTB_PROTOBUF. PROTOCOL_OPENRTB_PROTOBUF uses protobuf
	// encoding over the latest OpenRTB protocol version, which is 2.4 right
	// now. Allowed values:
	// - PROTOCOL_ADX
	// - PROTOCOL_OPENRTB_2_2
	// - PROTOCOL_OPENRTB_2_3
	// - PROTOCOL_OPENRTB_2_4
	// - PROTOCOL_OPENRTB_PROTOBUF
	BidProtocol string `json:"bidProtocol,omitempty"`

	// MaximumQps: The maximum queries per second the Ad Exchange will send.
	MaximumQps int64 `json:"maximumQps,omitempty"`

	// Region: The geographical region the Ad Exchange should send requests
	// from. Only used by some quota systems, but always setting the value
	// is recommended. Allowed values:
	// - ASIA
	// - EUROPE
	// - US_EAST
	// - US_WEST
	Region string `json:"region,omitempty"`

	// Url: The URL to which the Ad Exchange will send bid requests.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BidProtocol") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BidProtocol") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AccountBidderLocation) MarshalJSON() ([]byte, error) {
	type noMethod AccountBidderLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AccountsList: An account feed lists Ad Exchange buyer accounts that
// the user has access to. Each entry in the feed corresponds to a
// single buyer account.
type AccountsList struct {
	// Items: A list of accounts.
	Items []*Account `json:"items,omitempty"`

	// Kind: Resource type.
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

func (s *AccountsList) MarshalJSON() ([]byte, error) {
	type noMethod AccountsList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AddOrderDealsRequest struct {
	// Deals: The list of deals to add
	Deals []*MarketplaceDeal `json:"deals,omitempty"`

	// ProposalRevisionNumber: The last known proposal revision number.
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// UpdateAction: Indicates an optional action to take on the proposal
	UpdateAction string `json:"updateAction,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Deals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Deals") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AddOrderDealsRequest) MarshalJSON() ([]byte, error) {
	type noMethod AddOrderDealsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AddOrderDealsResponse struct {
	// Deals: List of deals added (in the same proposal as passed in the
	// request)
	Deals []*MarketplaceDeal `json:"deals,omitempty"`

	// ProposalRevisionNumber: The updated revision number for the proposal.
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Deals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Deals") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AddOrderDealsResponse) MarshalJSON() ([]byte, error) {
	type noMethod AddOrderDealsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AddOrderNotesRequest struct {
	// Notes: The list of notes to add.
	Notes []*MarketplaceNote `json:"notes,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Notes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Notes") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AddOrderNotesRequest) MarshalJSON() ([]byte, error) {
	type noMethod AddOrderNotesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AddOrderNotesResponse struct {
	Notes []*MarketplaceNote `json:"notes,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Notes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Notes") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AddOrderNotesResponse) MarshalJSON() ([]byte, error) {
	type noMethod AddOrderNotesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BillingInfo: The configuration data for an Ad Exchange billing info.
type BillingInfo struct {
	// AccountId: Account id.
	AccountId int64 `json:"accountId,omitempty"`

	// AccountName: Account name.
	AccountName string `json:"accountName,omitempty"`

	// BillingId: A list of adgroup IDs associated with this particular
	// account. These IDs may show up as part of a realtime bidding
	// BidRequest, which indicates a bid request for this account.
	BillingId []string `json:"billingId,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AccountId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccountId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BillingInfo) MarshalJSON() ([]byte, error) {
	type noMethod BillingInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BillingInfoList: A billing info feed lists Billing Info the Ad
// Exchange buyer account has access to. Each entry in the feed
// corresponds to a single billing info.
type BillingInfoList struct {
	// Items: A list of billing info relevant for your account.
	Items []*BillingInfo `json:"items,omitempty"`

	// Kind: Resource type.
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

func (s *BillingInfoList) MarshalJSON() ([]byte, error) {
	type noMethod BillingInfoList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Budget: The configuration data for Ad Exchange RTB - Budget API.
type Budget struct {
	// AccountId: The id of the account. This is required for get and update
	// requests.
	AccountId int64 `json:"accountId,omitempty,string"`

	// BillingId: The billing id to determine which adgroup to provide
	// budget information for. This is required for get and update requests.
	BillingId int64 `json:"billingId,omitempty,string"`

	// BudgetAmount: The daily budget amount in unit amount of the account
	// currency to apply for the billingId provided. This is required for
	// update requests.
	BudgetAmount int64 `json:"budgetAmount,omitempty,string"`

	// CurrencyCode: The currency code for the buyer. This cannot be altered
	// here.
	CurrencyCode string `json:"currencyCode,omitempty"`

	// Id: The unique id that describes this item.
	Id string `json:"id,omitempty"`

	// Kind: The kind of the resource, i.e. "adexchangebuyer#budget".
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AccountId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccountId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Budget) MarshalJSON() ([]byte, error) {
	type noMethod Budget
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Buyer struct {
	// AccountId: Adx account id of the buyer.
	AccountId string `json:"accountId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AccountId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccountId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Buyer) MarshalJSON() ([]byte, error) {
	type noMethod Buyer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ContactInformation struct {
	// Email: Email address of the contact.
	Email string `json:"email,omitempty"`

	// Name: The name of the contact.
	Name string `json:"name,omitempty"`

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

func (s *ContactInformation) MarshalJSON() ([]byte, error) {
	type noMethod ContactInformation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreateOrdersRequest struct {
	// Proposals: The list of proposals to create.
	Proposals []*Proposal `json:"proposals,omitempty"`

	// WebPropertyCode: Web property id of the seller creating these orders
	WebPropertyCode string `json:"webPropertyCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Proposals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Proposals") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateOrdersRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateOrdersRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreateOrdersResponse struct {
	// Proposals: The list of proposals successfully created.
	Proposals []*Proposal `json:"proposals,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Proposals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Proposals") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateOrdersResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateOrdersResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Creative: A creative and its classification data.
type Creative struct {
	// HTMLSnippet: The HTML snippet that displays the ad when inserted in
	// the web page. If set, videoURL should not be set.
	HTMLSnippet string `json:"HTMLSnippet,omitempty"`

	// AccountId: Account id.
	AccountId int64 `json:"accountId,omitempty"`

	// AdChoicesDestinationUrl: The link to the Ad Preferences page. This is
	// only supported for native ads.
	AdChoicesDestinationUrl string `json:"adChoicesDestinationUrl,omitempty"`

	// AdvertiserId: Detected advertiser id, if any. Read-only. This field
	// should not be set in requests.
	AdvertiserId googleapi.Int64s `json:"advertiserId,omitempty"`

	// AdvertiserName: The name of the company being advertised in the
	// creative.
	AdvertiserName string `json:"advertiserName,omitempty"`

	// AgencyId: The agency id for this creative.
	AgencyId int64 `json:"agencyId,omitempty,string"`

	// ApiUploadTimestamp: The last upload timestamp of this creative if it
	// was uploaded via API. Read-only. The value of this field is
	// generated, and will be ignored for uploads. (formatted RFC 3339
	// timestamp).
	ApiUploadTimestamp string `json:"apiUploadTimestamp,omitempty"`

	// Attribute: All attributes for the ads that may be shown from this
	// snippet.
	Attribute []int64 `json:"attribute,omitempty"`

	// BuyerCreativeId: A buyer-specific id identifying the creative in this
	// ad.
	BuyerCreativeId string `json:"buyerCreativeId,omitempty"`

	// ClickThroughUrl: The set of destination urls for the snippet.
	ClickThroughUrl []string `json:"clickThroughUrl,omitempty"`

	// Corrections: Shows any corrections that were applied to this
	// creative. Read-only. This field should not be set in requests.
	Corrections []*CreativeCorrections `json:"corrections,omitempty"`

	// DealsStatus: Top-level deals status. Read-only. This field should not
	// be set in requests. If disapproved, an entry for
	// auctionType=DIRECT_DEALS (or ALL) in servingRestrictions will also
	// exist. Note that this may be nuanced with other contextual
	// restrictions, in which case it may be preferable to read from
	// servingRestrictions directly.
	DealsStatus string `json:"dealsStatus,omitempty"`

	// DetectedDomains: Detected domains for this creative. Read-only. This
	// field should not be set in requests.
	DetectedDomains []string `json:"detectedDomains,omitempty"`

	// FilteringReasons: The filtering reasons for the creative. Read-only.
	// This field should not be set in requests.
	FilteringReasons *CreativeFilteringReasons `json:"filteringReasons,omitempty"`

	// Height: Ad height.
	Height int64 `json:"height,omitempty"`

	// ImpressionTrackingUrl: The set of urls to be called to record an
	// impression.
	ImpressionTrackingUrl []string `json:"impressionTrackingUrl,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// Languages: Detected languages for this creative. Read-only. This
	// field should not be set in requests.
	Languages []string `json:"languages,omitempty"`

	// NativeAd: If nativeAd is set, HTMLSnippet and the videoURL outside of
	// nativeAd should not be set. (The videoURL inside nativeAd can be
	// set.)
	NativeAd *CreativeNativeAd `json:"nativeAd,omitempty"`

	// OpenAuctionStatus: Top-level open auction status. Read-only. This
	// field should not be set in requests. If disapproved, an entry for
	// auctionType=OPEN_AUCTION (or ALL) in servingRestrictions will also
	// exist. Note that this may be nuanced with other contextual
	// restrictions, in which case it may be preferable to read from
	// ServingRestrictions directly.
	OpenAuctionStatus string `json:"openAuctionStatus,omitempty"`

	// ProductCategories: Detected product categories, if any. Read-only.
	// This field should not be set in requests.
	ProductCategories []int64 `json:"productCategories,omitempty"`

	// RestrictedCategories: All restricted categories for the ads that may
	// be shown from this snippet.
	RestrictedCategories []int64 `json:"restrictedCategories,omitempty"`

	// SensitiveCategories: Detected sensitive categories, if any.
	// Read-only. This field should not be set in requests.
	SensitiveCategories []int64 `json:"sensitiveCategories,omitempty"`

	// ServingRestrictions: The granular status of this ad in specific
	// contexts. A context here relates to where something ultimately serves
	// (for example, a physical location, a platform, an HTTPS vs HTTP
	// request, or the type of auction). Read-only. This field should not be
	// set in requests.
	ServingRestrictions []*CreativeServingRestrictions `json:"servingRestrictions,omitempty"`

	// VendorType: All vendor types for the ads that may be shown from this
	// snippet.
	VendorType []int64 `json:"vendorType,omitempty"`

	// Version: The version for this creative. Read-only. This field should
	// not be set in requests.
	Version int64 `json:"version,omitempty"`

	// VideoURL: The URL to fetch a video ad. If set, HTMLSnippet and the
	// nativeAd should not be set. Note, this is different from
	// resource.native_ad.video_url above.
	VideoURL string `json:"videoURL,omitempty"`

	// Width: Ad width.
	Width int64 `json:"width,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "HTMLSnippet") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "HTMLSnippet") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Creative) MarshalJSON() ([]byte, error) {
	type noMethod Creative
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeCorrections struct {
	// Contexts: All known serving contexts containing serving status
	// information.
	Contexts []*CreativeCorrectionsContexts `json:"contexts,omitempty"`

	// Details: Additional details about the correction.
	Details []string `json:"details,omitempty"`

	// Reason: The type of correction that was applied to the creative.
	Reason string `json:"reason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Contexts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Contexts") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeCorrections) MarshalJSON() ([]byte, error) {
	type noMethod CreativeCorrections
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeCorrectionsContexts struct {
	// AuctionType: Only set when contextType=AUCTION_TYPE. Represents the
	// auction types this correction applies to.
	AuctionType []string `json:"auctionType,omitempty"`

	// ContextType: The type of context (e.g., location, platform, auction
	// type, SSL-ness).
	ContextType string `json:"contextType,omitempty"`

	// GeoCriteriaId: Only set when contextType=LOCATION. Represents the geo
	// criterias this correction applies to.
	GeoCriteriaId []int64 `json:"geoCriteriaId,omitempty"`

	// Platform: Only set when contextType=PLATFORM. Represents the
	// platforms this correction applies to.
	Platform []string `json:"platform,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AuctionType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuctionType") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeCorrectionsContexts) MarshalJSON() ([]byte, error) {
	type noMethod CreativeCorrectionsContexts
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativeFilteringReasons: The filtering reasons for the creative.
// Read-only. This field should not be set in requests.
type CreativeFilteringReasons struct {
	// Date: The date in ISO 8601 format for the data. The data is collected
	// from 00:00:00 to 23:59:59 in PST.
	Date string `json:"date,omitempty"`

	// Reasons: The filtering reasons.
	Reasons []*CreativeFilteringReasonsReasons `json:"reasons,omitempty"`

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

func (s *CreativeFilteringReasons) MarshalJSON() ([]byte, error) {
	type noMethod CreativeFilteringReasons
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeFilteringReasonsReasons struct {
	// FilteringCount: The number of times the creative was filtered for the
	// status. The count is aggregated across all publishers on the
	// exchange.
	FilteringCount int64 `json:"filteringCount,omitempty,string"`

	// FilteringStatus: The filtering status code. Please refer to the
	// creative-status-codes.txt file for different statuses.
	FilteringStatus int64 `json:"filteringStatus,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FilteringCount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FilteringCount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreativeFilteringReasonsReasons) MarshalJSON() ([]byte, error) {
	type noMethod CreativeFilteringReasonsReasons
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativeNativeAd: If nativeAd is set, HTMLSnippet and the videoURL
// outside of nativeAd should not be set. (The videoURL inside nativeAd
// can be set.)
type CreativeNativeAd struct {
	Advertiser string `json:"advertiser,omitempty"`

	// AppIcon: The app icon, for app download ads.
	AppIcon *CreativeNativeAdAppIcon `json:"appIcon,omitempty"`

	// Body: A long description of the ad.
	Body string `json:"body,omitempty"`

	// CallToAction: A label for the button that the user is supposed to
	// click.
	CallToAction string `json:"callToAction,omitempty"`

	// ClickLinkUrl: The URL that the browser/SDK will load when the user
	// clicks the ad.
	ClickLinkUrl string `json:"clickLinkUrl,omitempty"`

	// ClickTrackingUrl: The URL to use for click tracking.
	ClickTrackingUrl string `json:"clickTrackingUrl,omitempty"`

	// Headline: A short title for the ad.
	Headline string `json:"headline,omitempty"`

	// Image: A large image.
	Image *CreativeNativeAdImage `json:"image,omitempty"`

	// ImpressionTrackingUrl: The URLs are called when the impression is
	// rendered.
	ImpressionTrackingUrl []string `json:"impressionTrackingUrl,omitempty"`

	// Logo: A smaller image, for the advertiser logo.
	Logo *CreativeNativeAdLogo `json:"logo,omitempty"`

	// Price: The price of the promoted app including the currency info.
	Price string `json:"price,omitempty"`

	// StarRating: The app rating in the app store. Must be in the range
	// [0-5].
	StarRating float64 `json:"starRating,omitempty"`

	// Store: The URL to the app store to purchase/download the promoted
	// app.
	Store string `json:"store,omitempty"`

	// VideoURL: The URL of the XML VAST for a native ad. Note this is a
	// separate field from resource.video_url.
	VideoURL string `json:"videoURL,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Advertiser") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Advertiser") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeNativeAd) MarshalJSON() ([]byte, error) {
	type noMethod CreativeNativeAd
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativeNativeAdAppIcon: The app icon, for app download ads.
type CreativeNativeAdAppIcon struct {
	Height int64 `json:"height,omitempty"`

	Url string `json:"url,omitempty"`

	Width int64 `json:"width,omitempty"`

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

func (s *CreativeNativeAdAppIcon) MarshalJSON() ([]byte, error) {
	type noMethod CreativeNativeAdAppIcon
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativeNativeAdImage: A large image.
type CreativeNativeAdImage struct {
	Height int64 `json:"height,omitempty"`

	Url string `json:"url,omitempty"`

	Width int64 `json:"width,omitempty"`

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

func (s *CreativeNativeAdImage) MarshalJSON() ([]byte, error) {
	type noMethod CreativeNativeAdImage
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativeNativeAdLogo: A smaller image, for the advertiser logo.
type CreativeNativeAdLogo struct {
	Height int64 `json:"height,omitempty"`

	Url string `json:"url,omitempty"`

	Width int64 `json:"width,omitempty"`

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

func (s *CreativeNativeAdLogo) MarshalJSON() ([]byte, error) {
	type noMethod CreativeNativeAdLogo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeServingRestrictions struct {
	// Contexts: All known contexts/restrictions.
	Contexts []*CreativeServingRestrictionsContexts `json:"contexts,omitempty"`

	// DisapprovalReasons: The reasons for disapproval within this
	// restriction, if any. Note that not all disapproval reasons may be
	// categorized, so it is possible for the creative to have a status of
	// DISAPPROVED or CONDITIONALLY_APPROVED with an empty list for
	// disapproval_reasons. In this case, please reach out to your TAM to
	// help debug the issue.
	DisapprovalReasons []*CreativeServingRestrictionsDisapprovalReasons `json:"disapprovalReasons,omitempty"`

	// Reason: Why the creative is ineligible to serve in this context
	// (e.g., it has been explicitly disapproved or is pending review).
	Reason string `json:"reason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Contexts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Contexts") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeServingRestrictions) MarshalJSON() ([]byte, error) {
	type noMethod CreativeServingRestrictions
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeServingRestrictionsContexts struct {
	// AuctionType: Only set when contextType=AUCTION_TYPE. Represents the
	// auction types this restriction applies to.
	AuctionType []string `json:"auctionType,omitempty"`

	// ContextType: The type of context (e.g., location, platform, auction
	// type, SSL-ness).
	ContextType string `json:"contextType,omitempty"`

	// GeoCriteriaId: Only set when contextType=LOCATION. Represents the geo
	// criterias this restriction applies to.
	GeoCriteriaId []int64 `json:"geoCriteriaId,omitempty"`

	// Platform: Only set when contextType=PLATFORM. Represents the
	// platforms this restriction applies to.
	Platform []string `json:"platform,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AuctionType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuctionType") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeServingRestrictionsContexts) MarshalJSON() ([]byte, error) {
	type noMethod CreativeServingRestrictionsContexts
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeServingRestrictionsDisapprovalReasons struct {
	// Details: Additional details about the reason for disapproval.
	Details []string `json:"details,omitempty"`

	// Reason: The categorized reason for disapproval.
	Reason string `json:"reason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Details") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Details") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeServingRestrictionsDisapprovalReasons) MarshalJSON() ([]byte, error) {
	type noMethod CreativeServingRestrictionsDisapprovalReasons
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativeDealIds: The external deal ids associated with a creative.
type CreativeDealIds struct {
	// DealStatuses: A list of external deal ids and ARC approval status.
	DealStatuses []*CreativeDealIdsDealStatuses `json:"dealStatuses,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DealStatuses") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DealStatuses") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeDealIds) MarshalJSON() ([]byte, error) {
	type noMethod CreativeDealIds
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CreativeDealIdsDealStatuses struct {
	// ArcStatus: ARC approval status.
	ArcStatus string `json:"arcStatus,omitempty"`

	// DealId: External deal ID.
	DealId int64 `json:"dealId,omitempty,string"`

	// WebPropertyId: Publisher ID.
	WebPropertyId int64 `json:"webPropertyId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ArcStatus") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ArcStatus") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreativeDealIdsDealStatuses) MarshalJSON() ([]byte, error) {
	type noMethod CreativeDealIdsDealStatuses
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreativesList: The creatives feed lists the active creatives for the
// Ad Exchange buyer accounts that the user has access to. Each entry in
// the feed corresponds to a single creative.
type CreativesList struct {
	// Items: A list of creatives.
	Items []*Creative `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Continuation token used to page through creatives. To
	// retrieve the next page of results, set the next request's "pageToken"
	// value to this.
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

func (s *CreativesList) MarshalJSON() ([]byte, error) {
	type noMethod CreativesList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealServingMetadata struct {
	// DealPauseStatus: Tracks which parties (if any) have paused a deal.
	// (readonly, except via PauseResumeOrderDeals action)
	DealPauseStatus *DealServingMetadataDealPauseStatus `json:"dealPauseStatus,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DealPauseStatus") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DealPauseStatus") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DealServingMetadata) MarshalJSON() ([]byte, error) {
	type noMethod DealServingMetadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DealServingMetadataDealPauseStatus: Tracks which parties (if any)
// have paused a deal. The deal is considered paused if has_buyer_paused
// || has_seller_paused. Each of the has_buyer_paused or the
// has_seller_paused bits can be set independently.
type DealServingMetadataDealPauseStatus struct {
	BuyerPauseReason string `json:"buyerPauseReason,omitempty"`

	// FirstPausedBy: If the deal is paused, records which party paused the
	// deal first.
	FirstPausedBy string `json:"firstPausedBy,omitempty"`

	HasBuyerPaused bool `json:"hasBuyerPaused,omitempty"`

	HasSellerPaused bool `json:"hasSellerPaused,omitempty"`

	SellerPauseReason string `json:"sellerPauseReason,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BuyerPauseReason") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BuyerPauseReason") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DealServingMetadataDealPauseStatus) MarshalJSON() ([]byte, error) {
	type noMethod DealServingMetadataDealPauseStatus
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealTerms struct {
	// BrandingType: Visibilty of the URL in bid requests.
	BrandingType string `json:"brandingType,omitempty"`

	// CrossListedExternalDealIdType: Indicates that this ExternalDealId
	// exists under at least two different AdxInventoryDeals. Currently, the
	// only case that the same ExternalDealId will exist is programmatic
	// cross sell case.
	CrossListedExternalDealIdType string `json:"crossListedExternalDealIdType,omitempty"`

	// Description: Description for the proposed terms of the deal.
	Description string `json:"description,omitempty"`

	// EstimatedGrossSpend: Non-binding estimate of the estimated gross
	// spend for this deal Can be set by buyer or seller.
	EstimatedGrossSpend *Price `json:"estimatedGrossSpend,omitempty"`

	// EstimatedImpressionsPerDay: Non-binding estimate of the impressions
	// served per day Can be set by buyer or seller.
	EstimatedImpressionsPerDay int64 `json:"estimatedImpressionsPerDay,omitempty,string"`

	// GuaranteedFixedPriceTerms: The terms for guaranteed fixed price
	// deals.
	GuaranteedFixedPriceTerms *DealTermsGuaranteedFixedPriceTerms `json:"guaranteedFixedPriceTerms,omitempty"`

	// NonGuaranteedAuctionTerms: The terms for non-guaranteed auction
	// deals.
	NonGuaranteedAuctionTerms *DealTermsNonGuaranteedAuctionTerms `json:"nonGuaranteedAuctionTerms,omitempty"`

	// NonGuaranteedFixedPriceTerms: The terms for non-guaranteed fixed
	// price deals.
	NonGuaranteedFixedPriceTerms *DealTermsNonGuaranteedFixedPriceTerms `json:"nonGuaranteedFixedPriceTerms,omitempty"`

	// RubiconNonGuaranteedTerms: The terms for rubicon non-guaranteed
	// deals.
	RubiconNonGuaranteedTerms *DealTermsRubiconNonGuaranteedTerms `json:"rubiconNonGuaranteedTerms,omitempty"`

	// SellerTimeZone: For deals with Cost Per Day billing, defines the
	// timezone used to mark the boundaries of a day (buyer-readonly)
	SellerTimeZone string `json:"sellerTimeZone,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BrandingType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BrandingType") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DealTerms) MarshalJSON() ([]byte, error) {
	type noMethod DealTerms
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealTermsGuaranteedFixedPriceTerms struct {
	// BillingInfo: External billing info for this Deal. This field is
	// relevant when external billing info such as price has a different
	// currency code than DFP/AdX.
	BillingInfo *DealTermsGuaranteedFixedPriceTermsBillingInfo `json:"billingInfo,omitempty"`

	// FixedPrices: Fixed price for the specified buyer.
	FixedPrices []*PricePerBuyer `json:"fixedPrices,omitempty"`

	// GuaranteedImpressions: Guaranteed impressions as a percentage. This
	// is the percentage of guaranteed looks that the buyer is guaranteeing
	// to buy.
	GuaranteedImpressions int64 `json:"guaranteedImpressions,omitempty,string"`

	// GuaranteedLooks: Count of guaranteed looks. Required for deal,
	// optional for product. For CPD deals, buyer changes to
	// guaranteed_looks will be ignored.
	GuaranteedLooks int64 `json:"guaranteedLooks,omitempty,string"`

	// MinimumDailyLooks: Count of minimum daily looks for a CPD deal. For
	// CPD deals, buyer should negotiate on this field instead of
	// guaranteed_looks.
	MinimumDailyLooks int64 `json:"minimumDailyLooks,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "BillingInfo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BillingInfo") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DealTermsGuaranteedFixedPriceTerms) MarshalJSON() ([]byte, error) {
	type noMethod DealTermsGuaranteedFixedPriceTerms
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealTermsGuaranteedFixedPriceTermsBillingInfo struct {
	// CurrencyConversionTimeMs: The timestamp (in ms since epoch) when the
	// original reservation price for the deal was first converted to DFP
	// currency. This is used to convert the contracted price into
	// advertiser's currency without discrepancy.
	CurrencyConversionTimeMs int64 `json:"currencyConversionTimeMs,omitempty,string"`

	// DfpLineItemId: The DFP line item id associated with this deal. For
	// features like CPD, buyers can retrieve the DFP line item for billing
	// reconciliation.
	DfpLineItemId int64 `json:"dfpLineItemId,omitempty,string"`

	// OriginalContractedQuantity: The original contracted quantity (#
	// impressions) for this deal. To ensure delivery, sometimes the
	// publisher will book the deal with a impression buffer, such that
	// guaranteed_looks is greater than the contracted quantity. However
	// clients are billed using the original contracted quantity.
	OriginalContractedQuantity int64 `json:"originalContractedQuantity,omitempty,string"`

	// Price: The original reservation price for the deal, if the currency
	// code is different from the one used in negotiation.
	Price *Price `json:"price,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "CurrencyConversionTimeMs") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CurrencyConversionTimeMs")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DealTermsGuaranteedFixedPriceTermsBillingInfo) MarshalJSON() ([]byte, error) {
	type noMethod DealTermsGuaranteedFixedPriceTermsBillingInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealTermsNonGuaranteedAuctionTerms struct {
	// AutoOptimizePrivateAuction: True if open auction buyers are allowed
	// to compete with invited buyers in this private auction
	// (buyer-readonly).
	AutoOptimizePrivateAuction bool `json:"autoOptimizePrivateAuction,omitempty"`

	// ReservePricePerBuyers: Reserve price for the specified buyer.
	ReservePricePerBuyers []*PricePerBuyer `json:"reservePricePerBuyers,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AutoOptimizePrivateAuction") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "AutoOptimizePrivateAuction") to include in API requests with the
	// JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DealTermsNonGuaranteedAuctionTerms) MarshalJSON() ([]byte, error) {
	type noMethod DealTermsNonGuaranteedAuctionTerms
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealTermsNonGuaranteedFixedPriceTerms struct {
	// FixedPrices: Fixed price for the specified buyer.
	FixedPrices []*PricePerBuyer `json:"fixedPrices,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FixedPrices") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FixedPrices") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DealTermsNonGuaranteedFixedPriceTerms) MarshalJSON() ([]byte, error) {
	type noMethod DealTermsNonGuaranteedFixedPriceTerms
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DealTermsRubiconNonGuaranteedTerms struct {
	// PriorityPrice: Optional price for Rubicon priority access in the
	// auction.
	PriorityPrice *Price `json:"priorityPrice,omitempty"`

	// StandardPrice: Optional price for Rubicon standard access in the
	// auction.
	StandardPrice *Price `json:"standardPrice,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PriorityPrice") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PriorityPrice") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DealTermsRubiconNonGuaranteedTerms) MarshalJSON() ([]byte, error) {
	type noMethod DealTermsRubiconNonGuaranteedTerms
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DeleteOrderDealsRequest struct {
	// DealIds: List of deals to delete for a given proposal
	DealIds []string `json:"dealIds,omitempty"`

	// ProposalRevisionNumber: The last known proposal revision number.
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// UpdateAction: Indicates an optional action to take on the proposal
	UpdateAction string `json:"updateAction,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DealIds") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DealIds") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteOrderDealsRequest) MarshalJSON() ([]byte, error) {
	type noMethod DeleteOrderDealsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DeleteOrderDealsResponse struct {
	// Deals: List of deals deleted (in the same proposal as passed in the
	// request)
	Deals []*MarketplaceDeal `json:"deals,omitempty"`

	// ProposalRevisionNumber: The updated revision number for the proposal.
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Deals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Deals") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteOrderDealsResponse) MarshalJSON() ([]byte, error) {
	type noMethod DeleteOrderDealsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DeliveryControl struct {
	CreativeBlockingLevel string `json:"creativeBlockingLevel,omitempty"`

	DeliveryRateType string `json:"deliveryRateType,omitempty"`

	FrequencyCaps []*DeliveryControlFrequencyCap `json:"frequencyCaps,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "CreativeBlockingLevel") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreativeBlockingLevel") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DeliveryControl) MarshalJSON() ([]byte, error) {
	type noMethod DeliveryControl
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DeliveryControlFrequencyCap struct {
	MaxImpressions int64 `json:"maxImpressions,omitempty"`

	NumTimeUnits int64 `json:"numTimeUnits,omitempty"`

	TimeUnitType string `json:"timeUnitType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "MaxImpressions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MaxImpressions") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DeliveryControlFrequencyCap) MarshalJSON() ([]byte, error) {
	type noMethod DeliveryControlFrequencyCap
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Dimension: This message carries publisher provided breakdown. E.g.
// {dimension_type: 'COUNTRY', [{dimension_value: {id: 1, name: 'US'}},
// {dimension_value: {id: 2, name: 'UK'}}]}
type Dimension struct {
	DimensionType string `json:"dimensionType,omitempty"`

	DimensionValues []*DimensionDimensionValue `json:"dimensionValues,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DimensionType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DimensionType") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Dimension) MarshalJSON() ([]byte, error) {
	type noMethod Dimension
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DimensionDimensionValue: Value of the dimension.
type DimensionDimensionValue struct {
	// Id: Id of the dimension.
	Id int64 `json:"id,omitempty"`

	// Name: Name of the dimension mainly for debugging purposes, except for
	// the case of CREATIVE_SIZE. For CREATIVE_SIZE, strings are used
	// instead of ids.
	Name string `json:"name,omitempty"`

	// Percentage: Percent of total impressions for a dimension type. e.g.
	// {dimension_type: 'GENDER', [{dimension_value: {id: 1, name: 'MALE',
	// percentage: 60}}]} Gender MALE is 60% of all impressions which have
	// gender.
	Percentage int64 `json:"percentage,omitempty"`

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

func (s *DimensionDimensionValue) MarshalJSON() ([]byte, error) {
	type noMethod DimensionDimensionValue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type EditAllOrderDealsRequest struct {
	// Deals: List of deals to edit. Service may perform 3 different
	// operations based on comparison of deals in this list vs deals already
	// persisted in database: 1. Add new deal to proposal If a deal in this
	// list does not exist in the proposal, the service will create a new
	// deal and add it to the proposal. Validation will follow
	// AddOrderDealsRequest. 2. Update existing deal in the proposal If a
	// deal in this list already exist in the proposal, the service will
	// update that existing deal to this new deal in the request. Validation
	// will follow UpdateOrderDealsRequest. 3. Delete deals from the
	// proposal (just need the id) If a existing deal in the proposal is not
	// present in this list, the service will delete that deal from the
	// proposal. Validation will follow DeleteOrderDealsRequest.
	Deals []*MarketplaceDeal `json:"deals,omitempty"`

	// Proposal: If specified, also updates the proposal in the batch
	// transaction. This is useful when the proposal and the deals need to
	// be updated in one transaction.
	Proposal *Proposal `json:"proposal,omitempty"`

	// ProposalRevisionNumber: The last known revision number for the
	// proposal.
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// UpdateAction: Indicates an optional action to take on the proposal
	UpdateAction string `json:"updateAction,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Deals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Deals") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EditAllOrderDealsRequest) MarshalJSON() ([]byte, error) {
	type noMethod EditAllOrderDealsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type EditAllOrderDealsResponse struct {
	// Deals: List of all deals in the proposal after edit.
	Deals []*MarketplaceDeal `json:"deals,omitempty"`

	// OrderRevisionNumber: The latest revision number after the update has
	// been applied.
	OrderRevisionNumber int64 `json:"orderRevisionNumber,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Deals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Deals") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *EditAllOrderDealsResponse) MarshalJSON() ([]byte, error) {
	type noMethod EditAllOrderDealsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GetOffersResponse struct {
	// Products: The returned list of products.
	Products []*Product `json:"products,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Products") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Products") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetOffersResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetOffersResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GetOrderDealsResponse struct {
	// Deals: List of deals for the proposal
	Deals []*MarketplaceDeal `json:"deals,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Deals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Deals") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetOrderDealsResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetOrderDealsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GetOrderNotesResponse struct {
	// Notes: The list of matching notes. The notes for a proposal are
	// ordered from oldest to newest. If the notes span multiple proposals,
	// they will be grouped by proposal, with the notes for the most
	// recently modified proposal appearing first.
	Notes []*MarketplaceNote `json:"notes,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Notes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Notes") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetOrderNotesResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetOrderNotesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GetOrdersResponse struct {
	// Proposals: The list of matching proposals.
	Proposals []*Proposal `json:"proposals,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Proposals") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Proposals") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetOrdersResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetOrdersResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GetPublisherProfilesByAccountIdResponse struct {
	// Profiles: Profiles for the requested publisher
	Profiles []*PublisherProfileApiProto `json:"profiles,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Profiles") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Profiles") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetPublisherProfilesByAccountIdResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetPublisherProfilesByAccountIdResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MarketplaceDeal: A proposal can contain multiple deals. A deal
// contains the terms and targeting information that is used for
// serving.
type MarketplaceDeal struct {
	// BuyerPrivateData: Buyer private data (hidden from seller).
	BuyerPrivateData *PrivateData `json:"buyerPrivateData,omitempty"`

	// CreationTimeMs: The time (ms since epoch) of the deal creation.
	// (readonly)
	CreationTimeMs int64 `json:"creationTimeMs,omitempty,string"`

	// CreativePreApprovalPolicy: Specifies the creative pre-approval policy
	// (buyer-readonly)
	CreativePreApprovalPolicy string `json:"creativePreApprovalPolicy,omitempty"`

	// CreativeSafeFrameCompatibility: Specifies whether the creative is
	// safeFrame compatible (buyer-readonly)
	CreativeSafeFrameCompatibility string `json:"creativeSafeFrameCompatibility,omitempty"`

	// DealId: A unique deal-id for the deal (readonly).
	DealId string `json:"dealId,omitempty"`

	// DealServingMetadata: Metadata about the serving status of this deal
	// (readonly, writes via custom actions)
	DealServingMetadata *DealServingMetadata `json:"dealServingMetadata,omitempty"`

	// DeliveryControl: The set of fields around delivery control that are
	// interesting for a buyer to see but are non-negotiable. These are set
	// by the publisher. This message is assigned an id of 100 since some
	// day we would want to model this as a protobuf extension.
	DeliveryControl *DeliveryControl `json:"deliveryControl,omitempty"`

	// ExternalDealId: The external deal id assigned to this deal once the
	// deal is finalized. This is the deal-id that shows up in
	// serving/reporting etc. (readonly)
	ExternalDealId string `json:"externalDealId,omitempty"`

	// FlightEndTimeMs: Proposed flight end time of the deal (ms since
	// epoch) This will generally be stored in a granularity of a second.
	// (updatable)
	FlightEndTimeMs int64 `json:"flightEndTimeMs,omitempty,string"`

	// FlightStartTimeMs: Proposed flight start time of the deal (ms since
	// epoch) This will generally be stored in a granularity of a second.
	// (updatable)
	FlightStartTimeMs int64 `json:"flightStartTimeMs,omitempty,string"`

	// InventoryDescription: Description for the deal terms. (updatable)
	InventoryDescription string `json:"inventoryDescription,omitempty"`

	// IsRfpTemplate: Indicates whether the current deal is a RFP template.
	// RFP template is created by buyer and not based on seller created
	// products.
	IsRfpTemplate bool `json:"isRfpTemplate,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "adexchangebuyer#marketplaceDeal".
	Kind string `json:"kind,omitempty"`

	// LastUpdateTimeMs: The time (ms since epoch) when the deal was last
	// updated. (readonly)
	LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty,string"`

	// Name: The name of the deal. (updatable)
	Name string `json:"name,omitempty"`

	// ProductId: The product-id from which this deal was created.
	// (readonly, except on create)
	ProductId string `json:"productId,omitempty"`

	// ProductRevisionNumber: The revision number of the product that the
	// deal was created from (readonly, except on create)
	ProductRevisionNumber int64 `json:"productRevisionNumber,omitempty,string"`

	// ProgrammaticCreativeSource: Specifies the creative source for
	// programmatic deals, PUBLISHER means creative is provided by seller
	// and ADVERTISR means creative is provided by buyer. (buyer-readonly)
	ProgrammaticCreativeSource string `json:"programmaticCreativeSource,omitempty"`

	ProposalId string `json:"proposalId,omitempty"`

	// SellerContacts: Optional Seller contact information for the deal
	// (buyer-readonly)
	SellerContacts []*ContactInformation `json:"sellerContacts,omitempty"`

	// SharedTargetings: The shared targeting visible to buyers and sellers.
	// Each shared targeting entity is AND'd together. (updatable)
	SharedTargetings []*SharedTargeting `json:"sharedTargetings,omitempty"`

	// SyndicationProduct: The syndication product associated with the deal.
	// (readonly, except on create)
	SyndicationProduct string `json:"syndicationProduct,omitempty"`

	// Terms: The negotiable terms of the deal. (updatable)
	Terms *DealTerms `json:"terms,omitempty"`

	WebPropertyCode string `json:"webPropertyCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BuyerPrivateData") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BuyerPrivateData") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *MarketplaceDeal) MarshalJSON() ([]byte, error) {
	type noMethod MarketplaceDeal
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MarketplaceDealParty struct {
	// Buyer: The buyer/seller associated with the deal. One of buyer/seller
	// is specified for a deal-party.
	Buyer *Buyer `json:"buyer,omitempty"`

	// Seller: The buyer/seller associated with the deal. One of
	// buyer/seller is specified for a deal party.
	Seller *Seller `json:"seller,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Buyer") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Buyer") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MarketplaceDealParty) MarshalJSON() ([]byte, error) {
	type noMethod MarketplaceDealParty
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MarketplaceLabel struct {
	// AccountId: The accountId of the party that created the label.
	AccountId string `json:"accountId,omitempty"`

	// CreateTimeMs: The creation time (in ms since epoch) for the label.
	CreateTimeMs int64 `json:"createTimeMs,omitempty,string"`

	// DeprecatedMarketplaceDealParty: Information about the party that
	// created the label.
	DeprecatedMarketplaceDealParty *MarketplaceDealParty `json:"deprecatedMarketplaceDealParty,omitempty"`

	// Label: The label to use.
	Label string `json:"label,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AccountId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccountId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MarketplaceLabel) MarshalJSON() ([]byte, error) {
	type noMethod MarketplaceLabel
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MarketplaceNote: A proposal is associated with a bunch of notes which
// may optionally be associated with a deal and/or revision number.
type MarketplaceNote struct {
	// CreatorRole: The role of the person (buyer/seller) creating the note.
	// (readonly)
	CreatorRole string `json:"creatorRole,omitempty"`

	// DealId: Notes can optionally be associated with a deal. (readonly,
	// except on create)
	DealId string `json:"dealId,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "adexchangebuyer#marketplaceNote".
	Kind string `json:"kind,omitempty"`

	// Note: The actual note to attach. (readonly, except on create)
	Note string `json:"note,omitempty"`

	// NoteId: The unique id for the note. (readonly)
	NoteId string `json:"noteId,omitempty"`

	// ProposalId: The proposalId that a note is attached to. (readonly)
	ProposalId string `json:"proposalId,omitempty"`

	// ProposalRevisionNumber: If the note is associated with a proposal
	// revision number, then store that here. (readonly, except on create)
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// TimestampMs: The timestamp (ms since epoch) that this note was
	// created. (readonly)
	TimestampMs int64 `json:"timestampMs,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "CreatorRole") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreatorRole") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MarketplaceNote) MarshalJSON() ([]byte, error) {
	type noMethod MarketplaceNote
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PerformanceReport: The configuration data for an Ad Exchange
// performance report list.
type PerformanceReport struct {
	// BidRate: The number of bid responses with an ad.
	BidRate float64 `json:"bidRate,omitempty"`

	// BidRequestRate: The number of bid requests sent to your bidder.
	BidRequestRate float64 `json:"bidRequestRate,omitempty"`

	// CalloutStatusRate: Rate of various prefiltering statuses per match.
	// Please refer to the callout-status-codes.txt file for different
	// statuses.
	CalloutStatusRate []interface{} `json:"calloutStatusRate,omitempty"`

	// CookieMatcherStatusRate: Average QPS for cookie matcher operations.
	CookieMatcherStatusRate []interface{} `json:"cookieMatcherStatusRate,omitempty"`

	// CreativeStatusRate: Rate of ads with a given status. Please refer to
	// the creative-status-codes.txt file for different statuses.
	CreativeStatusRate []interface{} `json:"creativeStatusRate,omitempty"`

	// FilteredBidRate: The number of bid responses that were filtered due
	// to a policy violation or other errors.
	FilteredBidRate float64 `json:"filteredBidRate,omitempty"`

	// HostedMatchStatusRate: Average QPS for hosted match operations.
	HostedMatchStatusRate []interface{} `json:"hostedMatchStatusRate,omitempty"`

	// InventoryMatchRate: The number of potential queries based on your
	// pretargeting settings.
	InventoryMatchRate float64 `json:"inventoryMatchRate,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// Latency50thPercentile: The 50th percentile round trip latency(ms) as
	// perceived from Google servers for the duration period covered by the
	// report.
	Latency50thPercentile float64 `json:"latency50thPercentile,omitempty"`

	// Latency85thPercentile: The 85th percentile round trip latency(ms) as
	// perceived from Google servers for the duration period covered by the
	// report.
	Latency85thPercentile float64 `json:"latency85thPercentile,omitempty"`

	// Latency95thPercentile: The 95th percentile round trip latency(ms) as
	// perceived from Google servers for the duration period covered by the
	// report.
	Latency95thPercentile float64 `json:"latency95thPercentile,omitempty"`

	// NoQuotaInRegion: Rate of various quota account statuses per quota
	// check.
	NoQuotaInRegion float64 `json:"noQuotaInRegion,omitempty"`

	// OutOfQuota: Rate of various quota account statuses per quota check.
	OutOfQuota float64 `json:"outOfQuota,omitempty"`

	// PixelMatchRequests: Average QPS for pixel match requests from
	// clients.
	PixelMatchRequests float64 `json:"pixelMatchRequests,omitempty"`

	// PixelMatchResponses: Average QPS for pixel match responses from
	// clients.
	PixelMatchResponses float64 `json:"pixelMatchResponses,omitempty"`

	// QuotaConfiguredLimit: The configured quota limits for this account.
	QuotaConfiguredLimit float64 `json:"quotaConfiguredLimit,omitempty"`

	// QuotaThrottledLimit: The throttled quota limits for this account.
	QuotaThrottledLimit float64 `json:"quotaThrottledLimit,omitempty"`

	// Region: The trading location of this data.
	Region string `json:"region,omitempty"`

	// SuccessfulRequestRate: The number of properly formed bid responses
	// received by our servers within the deadline.
	SuccessfulRequestRate float64 `json:"successfulRequestRate,omitempty"`

	// Timestamp: The unix timestamp of the starting time of this
	// performance data.
	Timestamp int64 `json:"timestamp,omitempty,string"`

	// UnsuccessfulRequestRate: The number of bid responses that were
	// unsuccessful due to timeouts, incorrect formatting, etc.
	UnsuccessfulRequestRate float64 `json:"unsuccessfulRequestRate,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BidRate") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BidRate") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PerformanceReport) MarshalJSON() ([]byte, error) {
	type noMethod PerformanceReport
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PerformanceReportList: The configuration data for an Ad Exchange
// performance report list.
type PerformanceReportList struct {
	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// PerformanceReport: A list of performance reports relevant for the
	// account.
	PerformanceReport []*PerformanceReport `json:"performanceReport,omitempty"`

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

func (s *PerformanceReportList) MarshalJSON() ([]byte, error) {
	type noMethod PerformanceReportList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PretargetingConfig struct {
	// BillingId: The id for billing purposes, provided for reference. Leave
	// this field blank for insert requests; the id will be generated
	// automatically.
	BillingId int64 `json:"billingId,omitempty,string"`

	// ConfigId: The config id; generated automatically. Leave this field
	// blank for insert requests.
	ConfigId int64 `json:"configId,omitempty,string"`

	// ConfigName: The name of the config. Must be unique. Required for all
	// requests.
	ConfigName string `json:"configName,omitempty"`

	// CreativeType: List must contain exactly one of
	// PRETARGETING_CREATIVE_TYPE_HTML or PRETARGETING_CREATIVE_TYPE_VIDEO.
	CreativeType []string `json:"creativeType,omitempty"`

	// Dimensions: Requests which allow one of these (width, height) pairs
	// will match. All pairs must be supported ad dimensions.
	Dimensions []*PretargetingConfigDimensions `json:"dimensions,omitempty"`

	// ExcludedContentLabels: Requests with any of these content labels will
	// not match. Values are from content-labels.txt in the downloadable
	// files section.
	ExcludedContentLabels googleapi.Int64s `json:"excludedContentLabels,omitempty"`

	// ExcludedGeoCriteriaIds: Requests containing any of these geo criteria
	// ids will not match.
	ExcludedGeoCriteriaIds googleapi.Int64s `json:"excludedGeoCriteriaIds,omitempty"`

	// ExcludedPlacements: Requests containing any of these placements will
	// not match.
	ExcludedPlacements []*PretargetingConfigExcludedPlacements `json:"excludedPlacements,omitempty"`

	// ExcludedUserLists: Requests containing any of these users list ids
	// will not match.
	ExcludedUserLists googleapi.Int64s `json:"excludedUserLists,omitempty"`

	// ExcludedVerticals: Requests containing any of these vertical ids will
	// not match. Values are from the publisher-verticals.txt file in the
	// downloadable files section.
	ExcludedVerticals googleapi.Int64s `json:"excludedVerticals,omitempty"`

	// GeoCriteriaIds: Requests containing any of these geo criteria ids
	// will match.
	GeoCriteriaIds googleapi.Int64s `json:"geoCriteriaIds,omitempty"`

	// IsActive: Whether this config is active. Required for all requests.
	IsActive bool `json:"isActive,omitempty"`

	// Kind: The kind of the resource, i.e.
	// "adexchangebuyer#pretargetingConfig".
	Kind string `json:"kind,omitempty"`

	// Languages: Request containing any of these language codes will match.
	Languages []string `json:"languages,omitempty"`

	// MobileCarriers: Requests containing any of these mobile carrier ids
	// will match. Values are from mobile-carriers.csv in the downloadable
	// files section.
	MobileCarriers googleapi.Int64s `json:"mobileCarriers,omitempty"`

	// MobileDevices: Requests containing any of these mobile device ids
	// will match. Values are from mobile-devices.csv in the downloadable
	// files section.
	MobileDevices googleapi.Int64s `json:"mobileDevices,omitempty"`

	// MobileOperatingSystemVersions: Requests containing any of these
	// mobile operating system version ids will match. Values are from
	// mobile-os.csv in the downloadable files section.
	MobileOperatingSystemVersions googleapi.Int64s `json:"mobileOperatingSystemVersions,omitempty"`

	// Placements: Requests containing any of these placements will match.
	Placements []*PretargetingConfigPlacements `json:"placements,omitempty"`

	// Platforms: Requests matching any of these platforms will match.
	// Possible values are PRETARGETING_PLATFORM_MOBILE,
	// PRETARGETING_PLATFORM_DESKTOP, and PRETARGETING_PLATFORM_TABLET.
	Platforms []string `json:"platforms,omitempty"`

	// SupportedCreativeAttributes: Creative attributes should be declared
	// here if all creatives corresponding to this pretargeting
	// configuration have that creative attribute. Values are from
	// pretargetable-creative-attributes.txt in the downloadable files
	// section.
	SupportedCreativeAttributes googleapi.Int64s `json:"supportedCreativeAttributes,omitempty"`

	// UserIdentifierDataRequired: Requests containing the specified type of
	// user data will match. Possible values are HOSTED_MATCH_DATA, which
	// means the request is cookie-targetable and has a match in the buyer's
	// hosted match table, and COOKIE_OR_IDFA, which means the request has
	// either a targetable cookie or an iOS IDFA.
	UserIdentifierDataRequired []string `json:"userIdentifierDataRequired,omitempty"`

	// UserLists: Requests containing any of these user list ids will match.
	UserLists googleapi.Int64s `json:"userLists,omitempty"`

	// VendorTypes: Requests that allow any of these vendor ids will match.
	// Values are from vendors.txt in the downloadable files section.
	VendorTypes googleapi.Int64s `json:"vendorTypes,omitempty"`

	// Verticals: Requests containing any of these vertical ids will match.
	Verticals googleapi.Int64s `json:"verticals,omitempty"`

	// VideoPlayerSizes: Video requests satisfying any of these player size
	// constraints will match.
	VideoPlayerSizes []*PretargetingConfigVideoPlayerSizes `json:"videoPlayerSizes,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BillingId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BillingId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PretargetingConfig) MarshalJSON() ([]byte, error) {
	type noMethod PretargetingConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PretargetingConfigDimensions struct {
	// Height: Height in pixels.
	Height int64 `json:"height,omitempty,string"`

	// Width: Width in pixels.
	Width int64 `json:"width,omitempty,string"`

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

func (s *PretargetingConfigDimensions) MarshalJSON() ([]byte, error) {
	type noMethod PretargetingConfigDimensions
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PretargetingConfigExcludedPlacements struct {
	// Token: The value of the placement. Interpretation depends on the
	// placement type, e.g. URL for a site placement, channel name for a
	// channel placement, app id for a mobile app placement.
	Token string `json:"token,omitempty"`

	// Type: The type of the placement.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Token") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Token") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PretargetingConfigExcludedPlacements) MarshalJSON() ([]byte, error) {
	type noMethod PretargetingConfigExcludedPlacements
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PretargetingConfigPlacements struct {
	// Token: The value of the placement. Interpretation depends on the
	// placement type, e.g. URL for a site placement, channel name for a
	// channel placement, app id for a mobile app placement.
	Token string `json:"token,omitempty"`

	// Type: The type of the placement.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Token") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Token") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PretargetingConfigPlacements) MarshalJSON() ([]byte, error) {
	type noMethod PretargetingConfigPlacements
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PretargetingConfigVideoPlayerSizes struct {
	// AspectRatio: The type of aspect ratio. Leave this field blank to
	// match all aspect ratios.
	AspectRatio string `json:"aspectRatio,omitempty"`

	// MinHeight: The minimum player height in pixels. Leave this field
	// blank to match any player height.
	MinHeight int64 `json:"minHeight,omitempty,string"`

	// MinWidth: The minimum player width in pixels. Leave this field blank
	// to match any player width.
	MinWidth int64 `json:"minWidth,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "AspectRatio") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AspectRatio") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PretargetingConfigVideoPlayerSizes) MarshalJSON() ([]byte, error) {
	type noMethod PretargetingConfigVideoPlayerSizes
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PretargetingConfigList struct {
	// Items: A list of pretargeting configs
	Items []*PretargetingConfig `json:"items,omitempty"`

	// Kind: Resource type.
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

func (s *PretargetingConfigList) MarshalJSON() ([]byte, error) {
	type noMethod PretargetingConfigList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Price struct {
	// AmountMicros: The price value in micros.
	AmountMicros float64 `json:"amountMicros,omitempty"`

	// CurrencyCode: The currency code for the price.
	CurrencyCode string `json:"currencyCode,omitempty"`

	// ExpectedCpmMicros: In case of CPD deals, the expected CPM in micros.
	ExpectedCpmMicros float64 `json:"expectedCpmMicros,omitempty"`

	// PricingType: The pricing type for the deal/product.
	PricingType string `json:"pricingType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AmountMicros") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AmountMicros") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Price) MarshalJSON() ([]byte, error) {
	type noMethod Price
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PricePerBuyer: Used to specify pricing rules for buyers/advertisers.
// Each PricePerBuyer in an product can become [0,1] deals. To check if
// there is a PricePerBuyer for a particular buyer or buyer/advertiser
// pair, we look for the most specific matching rule - we first look for
// a rule matching the buyer and advertiser, next a rule with the buyer
// but an empty advertiser list, and otherwise look for a matching rule
// where no buyer is set.
type PricePerBuyer struct {
	// AuctionTier: Optional access type for this buyer.
	AuctionTier string `json:"auctionTier,omitempty"`

	// Buyer: The buyer who will pay this price. If unset, all buyers can
	// pay this price (if the advertisers match, and there's no more
	// specific rule matching the buyer).
	Buyer *Buyer `json:"buyer,omitempty"`

	// Price: The specified price
	Price *Price `json:"price,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AuctionTier") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AuctionTier") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PricePerBuyer) MarshalJSON() ([]byte, error) {
	type noMethod PricePerBuyer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PrivateData struct {
	ReferenceId string `json:"referenceId,omitempty"`

	ReferencePayload string `json:"referencePayload,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ReferenceId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ReferenceId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PrivateData) MarshalJSON() ([]byte, error) {
	type noMethod PrivateData
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Product: A product is segment of inventory that a seller wishes to
// sell. It is associated with certain terms and targeting information
// which helps buyer know more about the inventory. Each field in a
// product can have one of the following setting:
//
// (readonly) - It is an error to try and set this field.
// (buyer-readonly) - Only the seller can set this field.
// (seller-readonly) - Only the buyer can set this field. (updatable) -
// The field is updatable at all times by either buyer or the seller.
type Product struct {
	// CreationTimeMs: Creation time in ms. since epoch (readonly)
	CreationTimeMs int64 `json:"creationTimeMs,omitempty,string"`

	// CreatorContacts: Optional contact information for the creator of this
	// product. (buyer-readonly)
	CreatorContacts []*ContactInformation `json:"creatorContacts,omitempty"`

	// DeliveryControl: The set of fields around delivery control that are
	// interesting for a buyer to see but are non-negotiable. These are set
	// by the publisher. This message is assigned an id of 100 since some
	// day we would want to model this as a protobuf extension.
	DeliveryControl *DeliveryControl `json:"deliveryControl,omitempty"`

	// FlightEndTimeMs: The proposed end time for the deal (ms since epoch)
	// (buyer-readonly)
	FlightEndTimeMs int64 `json:"flightEndTimeMs,omitempty,string"`

	// FlightStartTimeMs: Inventory availability dates. (times are in ms
	// since epoch) The granularity is generally in the order of seconds.
	// (buyer-readonly)
	FlightStartTimeMs int64 `json:"flightStartTimeMs,omitempty,string"`

	// HasCreatorSignedOff: If the creator has already signed off on the
	// product, then the buyer can finalize the deal by accepting the
	// product as is. When copying to a proposal, if any of the terms are
	// changed, then auto_finalize is automatically set to false.
	HasCreatorSignedOff bool `json:"hasCreatorSignedOff,omitempty"`

	// InventorySource: What exchange will provide this inventory (readonly,
	// except on create).
	InventorySource string `json:"inventorySource,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "adexchangebuyer#product".
	Kind string `json:"kind,omitempty"`

	// Labels: Optional List of labels for the product (optional,
	// buyer-readonly).
	Labels []*MarketplaceLabel `json:"labels,omitempty"`

	// LastUpdateTimeMs: Time of last update in ms. since epoch (readonly)
	LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty,string"`

	// LegacyOfferId: Optional legacy offer id if this offer is a preferred
	// deal offer.
	LegacyOfferId string `json:"legacyOfferId,omitempty"`

	// Name: The name for this product as set by the seller.
	// (buyer-readonly)
	Name string `json:"name,omitempty"`

	// PrivateAuctionId: Optional private auction id if this offer is a
	// private auction offer.
	PrivateAuctionId string `json:"privateAuctionId,omitempty"`

	// ProductId: The unique id for the product (readonly)
	ProductId string `json:"productId,omitempty"`

	// PublisherProfileId: Id of the publisher profile for a given seller. A
	// (seller.account_id, publisher_profile_id) pair uniquely identifies a
	// publisher profile. Buyers can call the PublisherProfiles::List
	// endpoint to get a list of publisher profiles for a given seller.
	PublisherProfileId string `json:"publisherProfileId,omitempty"`

	// PublisherProvidedForecast: Publisher self-provided forecast
	// information.
	PublisherProvidedForecast *PublisherProvidedForecast `json:"publisherProvidedForecast,omitempty"`

	// RevisionNumber: The revision number of the product. (readonly)
	RevisionNumber int64 `json:"revisionNumber,omitempty,string"`

	// Seller: Information about the seller that created this product
	// (readonly, except on create)
	Seller *Seller `json:"seller,omitempty"`

	// SharedTargetings: Targeting that is shared between the buyer and the
	// seller. Each targeting criteria has a specified key and for each key
	// there is a list of inclusion value or exclusion values.
	// (buyer-readonly)
	SharedTargetings []*SharedTargeting `json:"sharedTargetings,omitempty"`

	// State: The state of the product. (buyer-readonly)
	State string `json:"state,omitempty"`

	// SyndicationProduct: The syndication product associated with the deal.
	// (readonly, except on create)
	SyndicationProduct string `json:"syndicationProduct,omitempty"`

	// Terms: The negotiable terms of the deal (buyer-readonly)
	Terms *DealTerms `json:"terms,omitempty"`

	// WebPropertyCode: The web property code for the seller. This field is
	// meant to be copied over as is when creating deals.
	WebPropertyCode string `json:"webPropertyCode,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CreationTimeMs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreationTimeMs") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Product) MarshalJSON() ([]byte, error) {
	type noMethod Product
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Proposal: Represents a proposal in the marketplace. A proposal is the
// unit of negotiation between a seller and a buyer and contains deals
// which are served. Each field in a proposal can have one of the
// following setting:
//
// (readonly) - It is an error to try and set this field.
// (buyer-readonly) - Only the seller can set this field.
// (seller-readonly) - Only the buyer can set this field. (updatable) -
// The field is updatable at all times by either buyer or the seller.
type Proposal struct {
	// BilledBuyer: Reference to the buyer that will get billed for this
	// proposal. (readonly)
	BilledBuyer *Buyer `json:"billedBuyer,omitempty"`

	// Buyer: Reference to the buyer on the proposal. (readonly, except on
	// create)
	Buyer *Buyer `json:"buyer,omitempty"`

	// BuyerContacts: Optional contact information of the buyer.
	// (seller-readonly)
	BuyerContacts []*ContactInformation `json:"buyerContacts,omitempty"`

	// BuyerPrivateData: Private data for buyer. (hidden from seller).
	BuyerPrivateData *PrivateData `json:"buyerPrivateData,omitempty"`

	// DbmAdvertiserIds: IDs of DBM advertisers permission to this proposal.
	DbmAdvertiserIds []string `json:"dbmAdvertiserIds,omitempty"`

	// HasBuyerSignedOff: When an proposal is in an accepted state,
	// indicates whether the buyer has signed off. Once both sides have
	// signed off on a deal, the proposal can be finalized by the seller.
	// (seller-readonly)
	HasBuyerSignedOff bool `json:"hasBuyerSignedOff,omitempty"`

	// HasSellerSignedOff: When an proposal is in an accepted state,
	// indicates whether the buyer has signed off Once both sides have
	// signed off on a deal, the proposal can be finalized by the seller.
	// (buyer-readonly)
	HasSellerSignedOff bool `json:"hasSellerSignedOff,omitempty"`

	// InventorySource: What exchange will provide this inventory (readonly,
	// except on create).
	InventorySource string `json:"inventorySource,omitempty"`

	// IsRenegotiating: True if the proposal is being renegotiated
	// (readonly).
	IsRenegotiating bool `json:"isRenegotiating,omitempty"`

	// IsSetupComplete: True, if the buyside inventory setup is complete for
	// this proposal. (readonly, except via OrderSetupCompleted action)
	IsSetupComplete bool `json:"isSetupComplete,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "adexchangebuyer#proposal".
	Kind string `json:"kind,omitempty"`

	// Labels: List of labels associated with the proposal. (readonly)
	Labels []*MarketplaceLabel `json:"labels,omitempty"`

	// LastUpdaterOrCommentorRole: The role of the last user that either
	// updated the proposal or left a comment. (readonly)
	LastUpdaterOrCommentorRole string `json:"lastUpdaterOrCommentorRole,omitempty"`

	// Name: The name for the proposal (updatable)
	Name string `json:"name,omitempty"`

	// NegotiationId: Optional negotiation id if this proposal is a
	// preferred deal proposal.
	NegotiationId string `json:"negotiationId,omitempty"`

	// OriginatorRole: Indicates whether the buyer/seller created the
	// proposal.(readonly)
	OriginatorRole string `json:"originatorRole,omitempty"`

	// PrivateAuctionId: Optional private auction id if this proposal is a
	// private auction proposal.
	PrivateAuctionId string `json:"privateAuctionId,omitempty"`

	// ProposalId: The unique id of the proposal. (readonly).
	ProposalId string `json:"proposalId,omitempty"`

	// ProposalState: The current state of the proposal. (readonly)
	ProposalState string `json:"proposalState,omitempty"`

	// RevisionNumber: The revision number for the proposal (readonly).
	RevisionNumber int64 `json:"revisionNumber,omitempty,string"`

	// RevisionTimeMs: The time (ms since epoch) when the proposal was last
	// revised (readonly).
	RevisionTimeMs int64 `json:"revisionTimeMs,omitempty,string"`

	// Seller: Reference to the seller on the proposal. (readonly, except on
	// create)
	Seller *Seller `json:"seller,omitempty"`

	// SellerContacts: Optional contact information of the seller
	// (buyer-readonly).
	SellerContacts []*ContactInformation `json:"sellerContacts,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BilledBuyer") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BilledBuyer") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Proposal) MarshalJSON() ([]byte, error) {
	type noMethod Proposal
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PublisherProfileApiProto struct {
	// AccountId: The account id of the seller.
	AccountId string `json:"accountId,omitempty"`

	// Audience: Publisher provided info on its audience.
	Audience string `json:"audience,omitempty"`

	// BuyerPitchStatement: A pitch statement for the buyer
	BuyerPitchStatement string `json:"buyerPitchStatement,omitempty"`

	// DirectContact: Direct contact for the publisher profile.
	DirectContact string `json:"directContact,omitempty"`

	// Exchange: Exchange where this publisher profile is from. E.g. AdX,
	// Rubicon etc...
	Exchange string `json:"exchange,omitempty"`

	// GooglePlusLink: Link to publisher's Google+ page.
	GooglePlusLink string `json:"googlePlusLink,omitempty"`

	// IsParent: True, if this is the parent profile, which represents all
	// domains owned by the publisher.
	IsParent bool `json:"isParent,omitempty"`

	// IsPublished: True, if this profile is published. Deprecated for
	// state.
	IsPublished bool `json:"isPublished,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "adexchangebuyer#publisherProfileApiProto".
	Kind string `json:"kind,omitempty"`

	// LogoUrl: The url to the logo for the publisher.
	LogoUrl string `json:"logoUrl,omitempty"`

	// MediaKitLink: The url for additional marketing and sales materials.
	MediaKitLink string `json:"mediaKitLink,omitempty"`

	Name string `json:"name,omitempty"`

	// Overview: Publisher provided overview.
	Overview string `json:"overview,omitempty"`

	// ProfileId: The pair of (seller.account_id, profile_id) uniquely
	// identifies a publisher profile for a given publisher.
	ProfileId int64 `json:"profileId,omitempty"`

	// ProgrammaticContact: Programmatic contact for the publisher profile.
	ProgrammaticContact string `json:"programmaticContact,omitempty"`

	// PublisherDomains: The list of domains represented in this publisher
	// profile. Empty if this is a parent profile.
	PublisherDomains []string `json:"publisherDomains,omitempty"`

	// PublisherProfileId: Unique Id for publisher profile.
	PublisherProfileId string `json:"publisherProfileId,omitempty"`

	// PublisherProvidedForecast: Publisher provided forecasting
	// information.
	PublisherProvidedForecast *PublisherProvidedForecast `json:"publisherProvidedForecast,omitempty"`

	// RateCardInfoLink: Link to publisher rate card
	RateCardInfoLink string `json:"rateCardInfoLink,omitempty"`

	// SamplePageLink: Link for a sample content page.
	SamplePageLink string `json:"samplePageLink,omitempty"`

	// Seller: Seller of the publisher profile.
	Seller *Seller `json:"seller,omitempty"`

	// State: State of the publisher profile.
	State string `json:"state,omitempty"`

	// TopHeadlines: Publisher provided key metrics and rankings.
	TopHeadlines []string `json:"topHeadlines,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AccountId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccountId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PublisherProfileApiProto) MarshalJSON() ([]byte, error) {
	type noMethod PublisherProfileApiProto
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PublisherProvidedForecast: This message carries publisher provided
// forecasting information.
type PublisherProvidedForecast struct {
	// Dimensions: Publisher provided dimensions. E.g. geo, sizes etc...
	Dimensions []*Dimension `json:"dimensions,omitempty"`

	// WeeklyImpressions: Publisher provided weekly impressions.
	WeeklyImpressions int64 `json:"weeklyImpressions,omitempty,string"`

	// WeeklyUniques: Publisher provided weekly uniques.
	WeeklyUniques int64 `json:"weeklyUniques,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "Dimensions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Dimensions") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PublisherProvidedForecast) MarshalJSON() ([]byte, error) {
	type noMethod PublisherProvidedForecast
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Seller struct {
	// AccountId: The unique id for the seller. The seller fills in this
	// field. The seller account id is then available to buyer in the
	// product.
	AccountId string `json:"accountId,omitempty"`

	// SubAccountId: Optional sub-account id for the seller.
	SubAccountId string `json:"subAccountId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AccountId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccountId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Seller) MarshalJSON() ([]byte, error) {
	type noMethod Seller
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SharedTargeting struct {
	// Exclusions: The list of values to exclude from targeting. Each value
	// is AND'd together.
	Exclusions []*TargetingValue `json:"exclusions,omitempty"`

	// Inclusions: The list of value to include as part of the targeting.
	// Each value is OR'd together.
	Inclusions []*TargetingValue `json:"inclusions,omitempty"`

	// Key: The key representing the shared targeting criterion.
	Key string `json:"key,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Exclusions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Exclusions") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SharedTargeting) MarshalJSON() ([]byte, error) {
	type noMethod SharedTargeting
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TargetingValue struct {
	// CreativeSizeValue: The creative size value to exclude/include.
	CreativeSizeValue *TargetingValueCreativeSize `json:"creativeSizeValue,omitempty"`

	// DayPartTargetingValue: The daypart targeting to include / exclude.
	// Filled in when the key is GOOG_DAYPART_TARGETING.
	DayPartTargetingValue *TargetingValueDayPartTargeting `json:"dayPartTargetingValue,omitempty"`

	// LongValue: The long value to exclude/include.
	LongValue int64 `json:"longValue,omitempty,string"`

	// StringValue: The string value to exclude/include.
	StringValue string `json:"stringValue,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CreativeSizeValue")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreativeSizeValue") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *TargetingValue) MarshalJSON() ([]byte, error) {
	type noMethod TargetingValue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TargetingValueCreativeSize struct {
	// CompanionSizes: For video size type, the list of companion sizes.
	CompanionSizes []*TargetingValueSize `json:"companionSizes,omitempty"`

	// CreativeSizeType: The Creative size type.
	CreativeSizeType string `json:"creativeSizeType,omitempty"`

	// Size: For regular or video creative size type, specifies the size of
	// the creative.
	Size *TargetingValueSize `json:"size,omitempty"`

	// SkippableAdType: The skippable ad type for video size.
	SkippableAdType string `json:"skippableAdType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CompanionSizes") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CompanionSizes") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *TargetingValueCreativeSize) MarshalJSON() ([]byte, error) {
	type noMethod TargetingValueCreativeSize
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TargetingValueDayPartTargeting struct {
	DayParts []*TargetingValueDayPartTargetingDayPart `json:"dayParts,omitempty"`

	TimeZoneType string `json:"timeZoneType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DayParts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DayParts") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TargetingValueDayPartTargeting) MarshalJSON() ([]byte, error) {
	type noMethod TargetingValueDayPartTargeting
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TargetingValueDayPartTargetingDayPart struct {
	DayOfWeek string `json:"dayOfWeek,omitempty"`

	EndHour int64 `json:"endHour,omitempty"`

	EndMinute int64 `json:"endMinute,omitempty"`

	StartHour int64 `json:"startHour,omitempty"`

	StartMinute int64 `json:"startMinute,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DayOfWeek") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DayOfWeek") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TargetingValueDayPartTargetingDayPart) MarshalJSON() ([]byte, error) {
	type noMethod TargetingValueDayPartTargetingDayPart
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type TargetingValueSize struct {
	// Height: The height of the creative.
	Height int64 `json:"height,omitempty"`

	// Width: The width of the creative.
	Width int64 `json:"width,omitempty"`

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

func (s *TargetingValueSize) MarshalJSON() ([]byte, error) {
	type noMethod TargetingValueSize
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UpdatePrivateAuctionProposalRequest struct {
	// ExternalDealId: The externalDealId of the deal to be updated.
	ExternalDealId string `json:"externalDealId,omitempty"`

	// Note: Optional note to be added.
	Note *MarketplaceNote `json:"note,omitempty"`

	// ProposalRevisionNumber: The current revision number of the proposal
	// to be updated.
	ProposalRevisionNumber int64 `json:"proposalRevisionNumber,omitempty,string"`

	// UpdateAction: The proposed action on the private auction proposal.
	UpdateAction string `json:"updateAction,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ExternalDealId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ExternalDealId") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *UpdatePrivateAuctionProposalRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdatePrivateAuctionProposalRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "adexchangebuyer.accounts.get":

type AccountsGetCall struct {
	s            *Service
	id           int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets one account by ID.
func (r *AccountsService) Get(id int64) *AccountsGetCall {
	c := &AccountsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsGetCall) Fields(s ...googleapi.Field) *AccountsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AccountsGetCall) IfNoneMatch(entityTag string) *AccountsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsGetCall) Context(ctx context.Context) *AccountsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "accounts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": strconv.FormatInt(c.id, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.accounts.get" call.
// Exactly one of *Account or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Account.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AccountsGetCall) Do(opts ...googleapi.CallOption) (*Account, error) {
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
	//   "description": "Gets one account by ID.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.accounts.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "The account id",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "accounts/{id}",
	//   "response": {
	//     "$ref": "Account"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.accounts.list":

type AccountsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves the authenticated user's list of accounts.
func (r *AccountsService) List() *AccountsListCall {
	c := &AccountsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsListCall) Fields(s ...googleapi.Field) *AccountsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *AccountsListCall) IfNoneMatch(entityTag string) *AccountsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsListCall) Context(ctx context.Context) *AccountsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "accounts")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.accounts.list" call.
// Exactly one of *AccountsList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *AccountsList.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *AccountsListCall) Do(opts ...googleapi.CallOption) (*AccountsList, error) {
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
	ret := &AccountsList{
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
	//   "description": "Retrieves the authenticated user's list of accounts.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.accounts.list",
	//   "path": "accounts",
	//   "response": {
	//     "$ref": "AccountsList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.accounts.patch":

type AccountsPatchCall struct {
	s          *Service
	id         int64
	account    *Account
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates an existing account. This method supports patch
// semantics.
func (r *AccountsService) Patch(id int64, account *Account) *AccountsPatchCall {
	c := &AccountsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.account = account
	return c
}

// ConfirmUnsafeAccountChange sets the optional parameter
// "confirmUnsafeAccountChange": Confirmation for erasing bidder and
// cookie matching urls.
func (c *AccountsPatchCall) ConfirmUnsafeAccountChange(confirmUnsafeAccountChange bool) *AccountsPatchCall {
	c.urlParams_.Set("confirmUnsafeAccountChange", fmt.Sprint(confirmUnsafeAccountChange))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsPatchCall) Fields(s ...googleapi.Field) *AccountsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsPatchCall) Context(ctx context.Context) *AccountsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsPatchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "accounts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": strconv.FormatInt(c.id, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.accounts.patch" call.
// Exactly one of *Account or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Account.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AccountsPatchCall) Do(opts ...googleapi.CallOption) (*Account, error) {
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
	//   "description": "Updates an existing account. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "adexchangebuyer.accounts.patch",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "confirmUnsafeAccountChange": {
	//       "description": "Confirmation for erasing bidder and cookie matching urls.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "id": {
	//       "description": "The account id",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "accounts/{id}",
	//   "request": {
	//     "$ref": "Account"
	//   },
	//   "response": {
	//     "$ref": "Account"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.accounts.update":

type AccountsUpdateCall struct {
	s          *Service
	id         int64
	account    *Account
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates an existing account.
func (r *AccountsService) Update(id int64, account *Account) *AccountsUpdateCall {
	c := &AccountsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.account = account
	return c
}

// ConfirmUnsafeAccountChange sets the optional parameter
// "confirmUnsafeAccountChange": Confirmation for erasing bidder and
// cookie matching urls.
func (c *AccountsUpdateCall) ConfirmUnsafeAccountChange(confirmUnsafeAccountChange bool) *AccountsUpdateCall {
	c.urlParams_.Set("confirmUnsafeAccountChange", fmt.Sprint(confirmUnsafeAccountChange))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *AccountsUpdateCall) Fields(s ...googleapi.Field) *AccountsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *AccountsUpdateCall) Context(ctx context.Context) *AccountsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *AccountsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *AccountsUpdateCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "accounts/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": strconv.FormatInt(c.id, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.accounts.update" call.
// Exactly one of *Account or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Account.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *AccountsUpdateCall) Do(opts ...googleapi.CallOption) (*Account, error) {
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
	//   "description": "Updates an existing account.",
	//   "httpMethod": "PUT",
	//   "id": "adexchangebuyer.accounts.update",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "confirmUnsafeAccountChange": {
	//       "description": "Confirmation for erasing bidder and cookie matching urls.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "id": {
	//       "description": "The account id",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "accounts/{id}",
	//   "request": {
	//     "$ref": "Account"
	//   },
	//   "response": {
	//     "$ref": "Account"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.billingInfo.get":

type BillingInfoGetCall struct {
	s            *Service
	accountId    int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns the billing information for one account specified by
// account ID.
func (r *BillingInfoService) Get(accountId int64) *BillingInfoGetCall {
	c := &BillingInfoGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BillingInfoGetCall) Fields(s ...googleapi.Field) *BillingInfoGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BillingInfoGetCall) IfNoneMatch(entityTag string) *BillingInfoGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BillingInfoGetCall) Context(ctx context.Context) *BillingInfoGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BillingInfoGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BillingInfoGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "billinginfo/{accountId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.billingInfo.get" call.
// Exactly one of *BillingInfo or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *BillingInfo.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *BillingInfoGetCall) Do(opts ...googleapi.CallOption) (*BillingInfo, error) {
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
	ret := &BillingInfo{
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
	//   "description": "Returns the billing information for one account specified by account ID.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.billingInfo.get",
	//   "parameterOrder": [
	//     "accountId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "billinginfo/{accountId}",
	//   "response": {
	//     "$ref": "BillingInfo"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.billingInfo.list":

type BillingInfoListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of billing information for all accounts of the
// authenticated user.
func (r *BillingInfoService) List() *BillingInfoListCall {
	c := &BillingInfoListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BillingInfoListCall) Fields(s ...googleapi.Field) *BillingInfoListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BillingInfoListCall) IfNoneMatch(entityTag string) *BillingInfoListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BillingInfoListCall) Context(ctx context.Context) *BillingInfoListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BillingInfoListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BillingInfoListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "billinginfo")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.billingInfo.list" call.
// Exactly one of *BillingInfoList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *BillingInfoList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *BillingInfoListCall) Do(opts ...googleapi.CallOption) (*BillingInfoList, error) {
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
	ret := &BillingInfoList{
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
	//   "description": "Retrieves a list of billing information for all accounts of the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.billingInfo.list",
	//   "path": "billinginfo",
	//   "response": {
	//     "$ref": "BillingInfoList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.budget.get":

type BudgetGetCall struct {
	s            *Service
	accountId    int64
	billingId    int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns the budget information for the adgroup specified by the
// accountId and billingId.
func (r *BudgetService) Get(accountId int64, billingId int64) *BudgetGetCall {
	c := &BudgetGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.billingId = billingId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BudgetGetCall) Fields(s ...googleapi.Field) *BudgetGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BudgetGetCall) IfNoneMatch(entityTag string) *BudgetGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BudgetGetCall) Context(ctx context.Context) *BudgetGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BudgetGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BudgetGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "billinginfo/{accountId}/{billingId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"billingId": strconv.FormatInt(c.billingId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.budget.get" call.
// Exactly one of *Budget or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Budget.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BudgetGetCall) Do(opts ...googleapi.CallOption) (*Budget, error) {
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
	ret := &Budget{
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
	//   "description": "Returns the budget information for the adgroup specified by the accountId and billingId.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.budget.get",
	//   "parameterOrder": [
	//     "accountId",
	//     "billingId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to get the budget information for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "billingId": {
	//       "description": "The billing id to get the budget information for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "billinginfo/{accountId}/{billingId}",
	//   "response": {
	//     "$ref": "Budget"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.budget.patch":

type BudgetPatchCall struct {
	s          *Service
	accountId  int64
	billingId  int64
	budget     *Budget
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates the budget amount for the budget of the adgroup
// specified by the accountId and billingId, with the budget amount in
// the request. This method supports patch semantics.
func (r *BudgetService) Patch(accountId int64, billingId int64, budget *Budget) *BudgetPatchCall {
	c := &BudgetPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.billingId = billingId
	c.budget = budget
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BudgetPatchCall) Fields(s ...googleapi.Field) *BudgetPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BudgetPatchCall) Context(ctx context.Context) *BudgetPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BudgetPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BudgetPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.budget)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "billinginfo/{accountId}/{billingId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"billingId": strconv.FormatInt(c.billingId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.budget.patch" call.
// Exactly one of *Budget or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Budget.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BudgetPatchCall) Do(opts ...googleapi.CallOption) (*Budget, error) {
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
	ret := &Budget{
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
	//   "description": "Updates the budget amount for the budget of the adgroup specified by the accountId and billingId, with the budget amount in the request. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "adexchangebuyer.budget.patch",
	//   "parameterOrder": [
	//     "accountId",
	//     "billingId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id associated with the budget being updated.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "billingId": {
	//       "description": "The billing id associated with the budget being updated.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "billinginfo/{accountId}/{billingId}",
	//   "request": {
	//     "$ref": "Budget"
	//   },
	//   "response": {
	//     "$ref": "Budget"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.budget.update":

type BudgetUpdateCall struct {
	s          *Service
	accountId  int64
	billingId  int64
	budget     *Budget
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates the budget amount for the budget of the adgroup
// specified by the accountId and billingId, with the budget amount in
// the request.
func (r *BudgetService) Update(accountId int64, billingId int64, budget *Budget) *BudgetUpdateCall {
	c := &BudgetUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.billingId = billingId
	c.budget = budget
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BudgetUpdateCall) Fields(s ...googleapi.Field) *BudgetUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BudgetUpdateCall) Context(ctx context.Context) *BudgetUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BudgetUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BudgetUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.budget)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "billinginfo/{accountId}/{billingId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"billingId": strconv.FormatInt(c.billingId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.budget.update" call.
// Exactly one of *Budget or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Budget.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BudgetUpdateCall) Do(opts ...googleapi.CallOption) (*Budget, error) {
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
	ret := &Budget{
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
	//   "description": "Updates the budget amount for the budget of the adgroup specified by the accountId and billingId, with the budget amount in the request.",
	//   "httpMethod": "PUT",
	//   "id": "adexchangebuyer.budget.update",
	//   "parameterOrder": [
	//     "accountId",
	//     "billingId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id associated with the budget being updated.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "billingId": {
	//       "description": "The billing id associated with the budget being updated.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "billinginfo/{accountId}/{billingId}",
	//   "request": {
	//     "$ref": "Budget"
	//   },
	//   "response": {
	//     "$ref": "Budget"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.creatives.addDeal":

type CreativesAddDealCall struct {
	s               *Service
	accountId       int64
	buyerCreativeId string
	dealId          int64
	urlParams_      gensupport.URLParams
	ctx_            context.Context
	header_         http.Header
}

// AddDeal: Add a deal id association for the creative.
func (r *CreativesService) AddDeal(accountId int64, buyerCreativeId string, dealId int64) *CreativesAddDealCall {
	c := &CreativesAddDealCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.buyerCreativeId = buyerCreativeId
	c.dealId = dealId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CreativesAddDealCall) Fields(s ...googleapi.Field) *CreativesAddDealCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CreativesAddDealCall) Context(ctx context.Context) *CreativesAddDealCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CreativesAddDealCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CreativesAddDealCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "creatives/{accountId}/{buyerCreativeId}/addDeal/{dealId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId":       strconv.FormatInt(c.accountId, 10),
		"buyerCreativeId": c.buyerCreativeId,
		"dealId":          strconv.FormatInt(c.dealId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.creatives.addDeal" call.
func (c *CreativesAddDealCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Add a deal id association for the creative.",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.creatives.addDeal",
	//   "parameterOrder": [
	//     "accountId",
	//     "buyerCreativeId",
	//     "dealId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The id for the account that will serve this creative.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "buyerCreativeId": {
	//       "description": "The buyer-specific id for this creative.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "dealId": {
	//       "description": "The id of the deal id to associate with this creative.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "creatives/{accountId}/{buyerCreativeId}/addDeal/{dealId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.creatives.get":

type CreativesGetCall struct {
	s               *Service
	accountId       int64
	buyerCreativeId string
	urlParams_      gensupport.URLParams
	ifNoneMatch_    string
	ctx_            context.Context
	header_         http.Header
}

// Get: Gets the status for a single creative. A creative will be
// available 30-40 minutes after submission.
func (r *CreativesService) Get(accountId int64, buyerCreativeId string) *CreativesGetCall {
	c := &CreativesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.buyerCreativeId = buyerCreativeId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CreativesGetCall) Fields(s ...googleapi.Field) *CreativesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CreativesGetCall) IfNoneMatch(entityTag string) *CreativesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CreativesGetCall) Context(ctx context.Context) *CreativesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CreativesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CreativesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "creatives/{accountId}/{buyerCreativeId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId":       strconv.FormatInt(c.accountId, 10),
		"buyerCreativeId": c.buyerCreativeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.creatives.get" call.
// Exactly one of *Creative or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Creative.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CreativesGetCall) Do(opts ...googleapi.CallOption) (*Creative, error) {
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
	ret := &Creative{
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
	//   "description": "Gets the status for a single creative. A creative will be available 30-40 minutes after submission.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.creatives.get",
	//   "parameterOrder": [
	//     "accountId",
	//     "buyerCreativeId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The id for the account that will serve this creative.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "buyerCreativeId": {
	//       "description": "The buyer-specific id for this creative.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "creatives/{accountId}/{buyerCreativeId}",
	//   "response": {
	//     "$ref": "Creative"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.creatives.insert":

type CreativesInsertCall struct {
	s          *Service
	creative   *Creative
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Submit a new creative.
func (r *CreativesService) Insert(creative *Creative) *CreativesInsertCall {
	c := &CreativesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.creative = creative
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CreativesInsertCall) Fields(s ...googleapi.Field) *CreativesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CreativesInsertCall) Context(ctx context.Context) *CreativesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CreativesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CreativesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.creative)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "creatives")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.creatives.insert" call.
// Exactly one of *Creative or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Creative.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CreativesInsertCall) Do(opts ...googleapi.CallOption) (*Creative, error) {
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
	ret := &Creative{
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
	//   "description": "Submit a new creative.",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.creatives.insert",
	//   "path": "creatives",
	//   "request": {
	//     "$ref": "Creative"
	//   },
	//   "response": {
	//     "$ref": "Creative"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.creatives.list":

type CreativesListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of the authenticated user's active creatives.
// A creative will be available 30-40 minutes after submission.
func (r *CreativesService) List() *CreativesListCall {
	c := &CreativesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// AccountId sets the optional parameter "accountId": When specified,
// only creatives for the given account ids are returned.
func (c *CreativesListCall) AccountId(accountId ...int64) *CreativesListCall {
	var accountId_ []string
	for _, v := range accountId {
		accountId_ = append(accountId_, fmt.Sprint(v))
	}
	c.urlParams_.SetMulti("accountId", accountId_)
	return c
}

// BuyerCreativeId sets the optional parameter "buyerCreativeId": When
// specified, only creatives for the given buyer creative ids are
// returned.
func (c *CreativesListCall) BuyerCreativeId(buyerCreativeId ...string) *CreativesListCall {
	c.urlParams_.SetMulti("buyerCreativeId", append([]string{}, buyerCreativeId...))
	return c
}

// DealsStatusFilter sets the optional parameter "dealsStatusFilter":
// When specified, only creatives having the given deals status are
// returned.
//
// Possible values:
//   "approved" - Creatives which have been approved for serving on
// deals.
//   "conditionally_approved" - Creatives which have been conditionally
// approved for serving on deals.
//   "disapproved" - Creatives which have been disapproved for serving
// on deals.
//   "not_checked" - Creatives whose deals status is not yet checked.
func (c *CreativesListCall) DealsStatusFilter(dealsStatusFilter string) *CreativesListCall {
	c.urlParams_.Set("dealsStatusFilter", dealsStatusFilter)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. If not set, the default is
// 100.
func (c *CreativesListCall) MaxResults(maxResults int64) *CreativesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OpenAuctionStatusFilter sets the optional parameter
// "openAuctionStatusFilter": When specified, only creatives having the
// given open auction status are returned.
//
// Possible values:
//   "approved" - Creatives which have been approved for serving on the
// open auction.
//   "conditionally_approved" - Creatives which have been conditionally
// approved for serving on the open auction.
//   "disapproved" - Creatives which have been disapproved for serving
// on the open auction.
//   "not_checked" - Creatives whose open auction status is not yet
// checked.
func (c *CreativesListCall) OpenAuctionStatusFilter(openAuctionStatusFilter string) *CreativesListCall {
	c.urlParams_.Set("openAuctionStatusFilter", openAuctionStatusFilter)
	return c
}

// PageToken sets the optional parameter "pageToken": A continuation
// token, used to page through ad clients. To retrieve the next page,
// set this parameter to the value of "nextPageToken" from the previous
// response.
func (c *CreativesListCall) PageToken(pageToken string) *CreativesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CreativesListCall) Fields(s ...googleapi.Field) *CreativesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CreativesListCall) IfNoneMatch(entityTag string) *CreativesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CreativesListCall) Context(ctx context.Context) *CreativesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CreativesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CreativesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "creatives")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.creatives.list" call.
// Exactly one of *CreativesList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CreativesList.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CreativesListCall) Do(opts ...googleapi.CallOption) (*CreativesList, error) {
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
	ret := &CreativesList{
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
	//   "description": "Retrieves a list of the authenticated user's active creatives. A creative will be available 30-40 minutes after submission.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.creatives.list",
	//   "parameters": {
	//     "accountId": {
	//       "description": "When specified, only creatives for the given account ids are returned.",
	//       "format": "int32",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "integer"
	//     },
	//     "buyerCreativeId": {
	//       "description": "When specified, only creatives for the given buyer creative ids are returned.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "dealsStatusFilter": {
	//       "description": "When specified, only creatives having the given deals status are returned.",
	//       "enum": [
	//         "approved",
	//         "conditionally_approved",
	//         "disapproved",
	//         "not_checked"
	//       ],
	//       "enumDescriptions": [
	//         "Creatives which have been approved for serving on deals.",
	//         "Creatives which have been conditionally approved for serving on deals.",
	//         "Creatives which have been disapproved for serving on deals.",
	//         "Creatives whose deals status is not yet checked."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. If not set, the default is 100. Optional.",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "1000",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "openAuctionStatusFilter": {
	//       "description": "When specified, only creatives having the given open auction status are returned.",
	//       "enum": [
	//         "approved",
	//         "conditionally_approved",
	//         "disapproved",
	//         "not_checked"
	//       ],
	//       "enumDescriptions": [
	//         "Creatives which have been approved for serving on the open auction.",
	//         "Creatives which have been conditionally approved for serving on the open auction.",
	//         "Creatives which have been disapproved for serving on the open auction.",
	//         "Creatives whose open auction status is not yet checked."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageToken": {
	//       "description": "A continuation token, used to page through ad clients. To retrieve the next page, set this parameter to the value of \"nextPageToken\" from the previous response. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "creatives",
	//   "response": {
	//     "$ref": "CreativesList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CreativesListCall) Pages(ctx context.Context, f func(*CreativesList) error) error {
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

// method id "adexchangebuyer.creatives.listDeals":

type CreativesListDealsCall struct {
	s               *Service
	accountId       int64
	buyerCreativeId string
	urlParams_      gensupport.URLParams
	ifNoneMatch_    string
	ctx_            context.Context
	header_         http.Header
}

// ListDeals: Lists the external deal ids associated with the creative.
func (r *CreativesService) ListDeals(accountId int64, buyerCreativeId string) *CreativesListDealsCall {
	c := &CreativesListDealsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.buyerCreativeId = buyerCreativeId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CreativesListDealsCall) Fields(s ...googleapi.Field) *CreativesListDealsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CreativesListDealsCall) IfNoneMatch(entityTag string) *CreativesListDealsCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CreativesListDealsCall) Context(ctx context.Context) *CreativesListDealsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CreativesListDealsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CreativesListDealsCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "creatives/{accountId}/{buyerCreativeId}/listDeals")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId":       strconv.FormatInt(c.accountId, 10),
		"buyerCreativeId": c.buyerCreativeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.creatives.listDeals" call.
// Exactly one of *CreativeDealIds or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CreativeDealIds.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CreativesListDealsCall) Do(opts ...googleapi.CallOption) (*CreativeDealIds, error) {
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
	ret := &CreativeDealIds{
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
	//   "description": "Lists the external deal ids associated with the creative.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.creatives.listDeals",
	//   "parameterOrder": [
	//     "accountId",
	//     "buyerCreativeId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The id for the account that will serve this creative.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "buyerCreativeId": {
	//       "description": "The buyer-specific id for this creative.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "creatives/{accountId}/{buyerCreativeId}/listDeals",
	//   "response": {
	//     "$ref": "CreativeDealIds"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.creatives.removeDeal":

type CreativesRemoveDealCall struct {
	s               *Service
	accountId       int64
	buyerCreativeId string
	dealId          int64
	urlParams_      gensupport.URLParams
	ctx_            context.Context
	header_         http.Header
}

// RemoveDeal: Remove a deal id associated with the creative.
func (r *CreativesService) RemoveDeal(accountId int64, buyerCreativeId string, dealId int64) *CreativesRemoveDealCall {
	c := &CreativesRemoveDealCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.buyerCreativeId = buyerCreativeId
	c.dealId = dealId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CreativesRemoveDealCall) Fields(s ...googleapi.Field) *CreativesRemoveDealCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CreativesRemoveDealCall) Context(ctx context.Context) *CreativesRemoveDealCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CreativesRemoveDealCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CreativesRemoveDealCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "creatives/{accountId}/{buyerCreativeId}/removeDeal/{dealId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId":       strconv.FormatInt(c.accountId, 10),
		"buyerCreativeId": c.buyerCreativeId,
		"dealId":          strconv.FormatInt(c.dealId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.creatives.removeDeal" call.
func (c *CreativesRemoveDealCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove a deal id associated with the creative.",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.creatives.removeDeal",
	//   "parameterOrder": [
	//     "accountId",
	//     "buyerCreativeId",
	//     "dealId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The id for the account that will serve this creative.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "buyerCreativeId": {
	//       "description": "The buyer-specific id for this creative.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "dealId": {
	//       "description": "The id of the deal id to disassociate with this creative.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "creatives/{accountId}/{buyerCreativeId}/removeDeal/{dealId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplacedeals.delete":

type MarketplacedealsDeleteCall struct {
	s                       *Service
	proposalId              string
	deleteorderdealsrequest *DeleteOrderDealsRequest
	urlParams_              gensupport.URLParams
	ctx_                    context.Context
	header_                 http.Header
}

// Delete: Delete the specified deals from the proposal
func (r *MarketplacedealsService) Delete(proposalId string, deleteorderdealsrequest *DeleteOrderDealsRequest) *MarketplacedealsDeleteCall {
	c := &MarketplacedealsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	c.deleteorderdealsrequest = deleteorderdealsrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplacedealsDeleteCall) Fields(s ...googleapi.Field) *MarketplacedealsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplacedealsDeleteCall) Context(ctx context.Context) *MarketplacedealsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplacedealsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplacedealsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.deleteorderdealsrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/deals/delete")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplacedeals.delete" call.
// Exactly one of *DeleteOrderDealsResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *DeleteOrderDealsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MarketplacedealsDeleteCall) Do(opts ...googleapi.CallOption) (*DeleteOrderDealsResponse, error) {
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
	ret := &DeleteOrderDealsResponse{
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
	//   "description": "Delete the specified deals from the proposal",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.marketplacedeals.delete",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "The proposalId to delete deals from.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/deals/delete",
	//   "request": {
	//     "$ref": "DeleteOrderDealsRequest"
	//   },
	//   "response": {
	//     "$ref": "DeleteOrderDealsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplacedeals.insert":

type MarketplacedealsInsertCall struct {
	s                    *Service
	proposalId           string
	addorderdealsrequest *AddOrderDealsRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Insert: Add new deals for the specified proposal
func (r *MarketplacedealsService) Insert(proposalId string, addorderdealsrequest *AddOrderDealsRequest) *MarketplacedealsInsertCall {
	c := &MarketplacedealsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	c.addorderdealsrequest = addorderdealsrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplacedealsInsertCall) Fields(s ...googleapi.Field) *MarketplacedealsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplacedealsInsertCall) Context(ctx context.Context) *MarketplacedealsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplacedealsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplacedealsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.addorderdealsrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/deals/insert")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplacedeals.insert" call.
// Exactly one of *AddOrderDealsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *AddOrderDealsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MarketplacedealsInsertCall) Do(opts ...googleapi.CallOption) (*AddOrderDealsResponse, error) {
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
	ret := &AddOrderDealsResponse{
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
	//   "description": "Add new deals for the specified proposal",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.marketplacedeals.insert",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "proposalId for which deals need to be added.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/deals/insert",
	//   "request": {
	//     "$ref": "AddOrderDealsRequest"
	//   },
	//   "response": {
	//     "$ref": "AddOrderDealsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplacedeals.list":

type MarketplacedealsListCall struct {
	s            *Service
	proposalId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List all the deals for a given proposal
func (r *MarketplacedealsService) List(proposalId string) *MarketplacedealsListCall {
	c := &MarketplacedealsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	return c
}

// PqlQuery sets the optional parameter "pqlQuery": Query string to
// retrieve specific deals.
func (c *MarketplacedealsListCall) PqlQuery(pqlQuery string) *MarketplacedealsListCall {
	c.urlParams_.Set("pqlQuery", pqlQuery)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplacedealsListCall) Fields(s ...googleapi.Field) *MarketplacedealsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MarketplacedealsListCall) IfNoneMatch(entityTag string) *MarketplacedealsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplacedealsListCall) Context(ctx context.Context) *MarketplacedealsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplacedealsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplacedealsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/deals")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplacedeals.list" call.
// Exactly one of *GetOrderDealsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GetOrderDealsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MarketplacedealsListCall) Do(opts ...googleapi.CallOption) (*GetOrderDealsResponse, error) {
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
	ret := &GetOrderDealsResponse{
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
	//   "description": "List all the deals for a given proposal",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.marketplacedeals.list",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "pqlQuery": {
	//       "description": "Query string to retrieve specific deals.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "proposalId": {
	//       "description": "The proposalId to get deals for. To search across all proposals specify order_id = '-' as part of the URL.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/deals",
	//   "response": {
	//     "$ref": "GetOrderDealsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplacedeals.update":

type MarketplacedealsUpdateCall struct {
	s                        *Service
	proposalId               string
	editallorderdealsrequest *EditAllOrderDealsRequest
	urlParams_               gensupport.URLParams
	ctx_                     context.Context
	header_                  http.Header
}

// Update: Replaces all the deals in the proposal with the passed in
// deals
func (r *MarketplacedealsService) Update(proposalId string, editallorderdealsrequest *EditAllOrderDealsRequest) *MarketplacedealsUpdateCall {
	c := &MarketplacedealsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	c.editallorderdealsrequest = editallorderdealsrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplacedealsUpdateCall) Fields(s ...googleapi.Field) *MarketplacedealsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplacedealsUpdateCall) Context(ctx context.Context) *MarketplacedealsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplacedealsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplacedealsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.editallorderdealsrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/deals/update")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplacedeals.update" call.
// Exactly one of *EditAllOrderDealsResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *EditAllOrderDealsResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MarketplacedealsUpdateCall) Do(opts ...googleapi.CallOption) (*EditAllOrderDealsResponse, error) {
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
	ret := &EditAllOrderDealsResponse{
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
	//   "description": "Replaces all the deals in the proposal with the passed in deals",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.marketplacedeals.update",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "The proposalId to edit deals on.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/deals/update",
	//   "request": {
	//     "$ref": "EditAllOrderDealsRequest"
	//   },
	//   "response": {
	//     "$ref": "EditAllOrderDealsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplacenotes.insert":

type MarketplacenotesInsertCall struct {
	s                    *Service
	proposalId           string
	addordernotesrequest *AddOrderNotesRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Insert: Add notes to the proposal
func (r *MarketplacenotesService) Insert(proposalId string, addordernotesrequest *AddOrderNotesRequest) *MarketplacenotesInsertCall {
	c := &MarketplacenotesInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	c.addordernotesrequest = addordernotesrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplacenotesInsertCall) Fields(s ...googleapi.Field) *MarketplacenotesInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplacenotesInsertCall) Context(ctx context.Context) *MarketplacenotesInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplacenotesInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplacenotesInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.addordernotesrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/notes/insert")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplacenotes.insert" call.
// Exactly one of *AddOrderNotesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *AddOrderNotesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MarketplacenotesInsertCall) Do(opts ...googleapi.CallOption) (*AddOrderNotesResponse, error) {
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
	ret := &AddOrderNotesResponse{
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
	//   "description": "Add notes to the proposal",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.marketplacenotes.insert",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "The proposalId to add notes for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/notes/insert",
	//   "request": {
	//     "$ref": "AddOrderNotesRequest"
	//   },
	//   "response": {
	//     "$ref": "AddOrderNotesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplacenotes.list":

type MarketplacenotesListCall struct {
	s            *Service
	proposalId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Get all the notes associated with a proposal
func (r *MarketplacenotesService) List(proposalId string) *MarketplacenotesListCall {
	c := &MarketplacenotesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	return c
}

// PqlQuery sets the optional parameter "pqlQuery": Query string to
// retrieve specific notes. To search the text contents of notes, please
// use syntax like "WHERE note.note = "foo" or "WHERE note.note LIKE
// "%bar%"
func (c *MarketplacenotesListCall) PqlQuery(pqlQuery string) *MarketplacenotesListCall {
	c.urlParams_.Set("pqlQuery", pqlQuery)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplacenotesListCall) Fields(s ...googleapi.Field) *MarketplacenotesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MarketplacenotesListCall) IfNoneMatch(entityTag string) *MarketplacenotesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplacenotesListCall) Context(ctx context.Context) *MarketplacenotesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplacenotesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplacenotesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/notes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplacenotes.list" call.
// Exactly one of *GetOrderNotesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GetOrderNotesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MarketplacenotesListCall) Do(opts ...googleapi.CallOption) (*GetOrderNotesResponse, error) {
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
	ret := &GetOrderNotesResponse{
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
	//   "description": "Get all the notes associated with a proposal",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.marketplacenotes.list",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "pqlQuery": {
	//       "description": "Query string to retrieve specific notes. To search the text contents of notes, please use syntax like \"WHERE note.note = \"foo\" or \"WHERE note.note LIKE \"%bar%\"",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "proposalId": {
	//       "description": "The proposalId to get notes for. To search across all proposals specify order_id = '-' as part of the URL.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/notes",
	//   "response": {
	//     "$ref": "GetOrderNotesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.marketplaceprivateauction.updateproposal":

type MarketplaceprivateauctionUpdateproposalCall struct {
	s                                   *Service
	privateAuctionId                    string
	updateprivateauctionproposalrequest *UpdatePrivateAuctionProposalRequest
	urlParams_                          gensupport.URLParams
	ctx_                                context.Context
	header_                             http.Header
}

// Updateproposal: Update a given private auction proposal
func (r *MarketplaceprivateauctionService) Updateproposal(privateAuctionId string, updateprivateauctionproposalrequest *UpdatePrivateAuctionProposalRequest) *MarketplaceprivateauctionUpdateproposalCall {
	c := &MarketplaceprivateauctionUpdateproposalCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.privateAuctionId = privateAuctionId
	c.updateprivateauctionproposalrequest = updateprivateauctionproposalrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MarketplaceprivateauctionUpdateproposalCall) Fields(s ...googleapi.Field) *MarketplaceprivateauctionUpdateproposalCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MarketplaceprivateauctionUpdateproposalCall) Context(ctx context.Context) *MarketplaceprivateauctionUpdateproposalCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MarketplaceprivateauctionUpdateproposalCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MarketplaceprivateauctionUpdateproposalCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.updateprivateauctionproposalrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "privateauction/{privateAuctionId}/updateproposal")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"privateAuctionId": c.privateAuctionId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.marketplaceprivateauction.updateproposal" call.
func (c *MarketplaceprivateauctionUpdateproposalCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Update a given private auction proposal",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.marketplaceprivateauction.updateproposal",
	//   "parameterOrder": [
	//     "privateAuctionId"
	//   ],
	//   "parameters": {
	//     "privateAuctionId": {
	//       "description": "The private auction id to be updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "privateauction/{privateAuctionId}/updateproposal",
	//   "request": {
	//     "$ref": "UpdatePrivateAuctionProposalRequest"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.performanceReport.list":

type PerformanceReportListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves the authenticated user's list of performance metrics.
func (r *PerformanceReportService) List(accountId int64, endDateTime string, startDateTime string) *PerformanceReportListCall {
	c := &PerformanceReportListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("accountId", fmt.Sprint(accountId))
	c.urlParams_.Set("endDateTime", endDateTime)
	c.urlParams_.Set("startDateTime", startDateTime)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of entries returned on one result page. If not set, the default is
// 100.
func (c *PerformanceReportListCall) MaxResults(maxResults int64) *PerformanceReportListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": A continuation
// token, used to page through performance reports. To retrieve the next
// page, set this parameter to the value of "nextPageToken" from the
// previous response.
func (c *PerformanceReportListCall) PageToken(pageToken string) *PerformanceReportListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PerformanceReportListCall) Fields(s ...googleapi.Field) *PerformanceReportListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PerformanceReportListCall) IfNoneMatch(entityTag string) *PerformanceReportListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PerformanceReportListCall) Context(ctx context.Context) *PerformanceReportListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PerformanceReportListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PerformanceReportListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "performancereport")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.performanceReport.list" call.
// Exactly one of *PerformanceReportList or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PerformanceReportList.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PerformanceReportListCall) Do(opts ...googleapi.CallOption) (*PerformanceReportList, error) {
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
	ret := &PerformanceReportList{
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
	//   "description": "Retrieves the authenticated user's list of performance metrics.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.performanceReport.list",
	//   "parameterOrder": [
	//     "accountId",
	//     "endDateTime",
	//     "startDateTime"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to get the reports.",
	//       "format": "int64",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "endDateTime": {
	//       "description": "The end time of the report in ISO 8601 timestamp format using UTC.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of entries returned on one result page. If not set, the default is 100. Optional.",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "1000",
	//       "minimum": "1",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "A continuation token, used to page through performance reports. To retrieve the next page, set this parameter to the value of \"nextPageToken\" from the previous response. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startDateTime": {
	//       "description": "The start time of the report in ISO 8601 timestamp format using UTC.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "performancereport",
	//   "response": {
	//     "$ref": "PerformanceReportList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pretargetingConfig.delete":

type PretargetingConfigDeleteCall struct {
	s          *Service
	accountId  int64
	configId   int64
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an existing pretargeting config.
func (r *PretargetingConfigService) Delete(accountId int64, configId int64) *PretargetingConfigDeleteCall {
	c := &PretargetingConfigDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.configId = configId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PretargetingConfigDeleteCall) Fields(s ...googleapi.Field) *PretargetingConfigDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PretargetingConfigDeleteCall) Context(ctx context.Context) *PretargetingConfigDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PretargetingConfigDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PretargetingConfigDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "pretargetingconfigs/{accountId}/{configId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"configId":  strconv.FormatInt(c.configId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pretargetingConfig.delete" call.
func (c *PretargetingConfigDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes an existing pretargeting config.",
	//   "httpMethod": "DELETE",
	//   "id": "adexchangebuyer.pretargetingConfig.delete",
	//   "parameterOrder": [
	//     "accountId",
	//     "configId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to delete the pretargeting config for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "configId": {
	//       "description": "The specific id of the configuration to delete.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "pretargetingconfigs/{accountId}/{configId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pretargetingConfig.get":

type PretargetingConfigGetCall struct {
	s            *Service
	accountId    int64
	configId     int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a specific pretargeting configuration
func (r *PretargetingConfigService) Get(accountId int64, configId int64) *PretargetingConfigGetCall {
	c := &PretargetingConfigGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.configId = configId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PretargetingConfigGetCall) Fields(s ...googleapi.Field) *PretargetingConfigGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PretargetingConfigGetCall) IfNoneMatch(entityTag string) *PretargetingConfigGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PretargetingConfigGetCall) Context(ctx context.Context) *PretargetingConfigGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PretargetingConfigGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PretargetingConfigGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "pretargetingconfigs/{accountId}/{configId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"configId":  strconv.FormatInt(c.configId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pretargetingConfig.get" call.
// Exactly one of *PretargetingConfig or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PretargetingConfig.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PretargetingConfigGetCall) Do(opts ...googleapi.CallOption) (*PretargetingConfig, error) {
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
	ret := &PretargetingConfig{
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
	//   "description": "Gets a specific pretargeting configuration",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.pretargetingConfig.get",
	//   "parameterOrder": [
	//     "accountId",
	//     "configId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to get the pretargeting config for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "configId": {
	//       "description": "The specific id of the configuration to retrieve.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "pretargetingconfigs/{accountId}/{configId}",
	//   "response": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pretargetingConfig.insert":

type PretargetingConfigInsertCall struct {
	s                  *Service
	accountId          int64
	pretargetingconfig *PretargetingConfig
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Insert: Inserts a new pretargeting configuration.
func (r *PretargetingConfigService) Insert(accountId int64, pretargetingconfig *PretargetingConfig) *PretargetingConfigInsertCall {
	c := &PretargetingConfigInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.pretargetingconfig = pretargetingconfig
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PretargetingConfigInsertCall) Fields(s ...googleapi.Field) *PretargetingConfigInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PretargetingConfigInsertCall) Context(ctx context.Context) *PretargetingConfigInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PretargetingConfigInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PretargetingConfigInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pretargetingconfig)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "pretargetingconfigs/{accountId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pretargetingConfig.insert" call.
// Exactly one of *PretargetingConfig or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PretargetingConfig.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PretargetingConfigInsertCall) Do(opts ...googleapi.CallOption) (*PretargetingConfig, error) {
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
	ret := &PretargetingConfig{
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
	//   "description": "Inserts a new pretargeting configuration.",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.pretargetingConfig.insert",
	//   "parameterOrder": [
	//     "accountId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to insert the pretargeting config for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "pretargetingconfigs/{accountId}",
	//   "request": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "response": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pretargetingConfig.list":

type PretargetingConfigListCall struct {
	s            *Service
	accountId    int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of the authenticated user's pretargeting
// configurations.
func (r *PretargetingConfigService) List(accountId int64) *PretargetingConfigListCall {
	c := &PretargetingConfigListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PretargetingConfigListCall) Fields(s ...googleapi.Field) *PretargetingConfigListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PretargetingConfigListCall) IfNoneMatch(entityTag string) *PretargetingConfigListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PretargetingConfigListCall) Context(ctx context.Context) *PretargetingConfigListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PretargetingConfigListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PretargetingConfigListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "pretargetingconfigs/{accountId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pretargetingConfig.list" call.
// Exactly one of *PretargetingConfigList or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PretargetingConfigList.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PretargetingConfigListCall) Do(opts ...googleapi.CallOption) (*PretargetingConfigList, error) {
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
	ret := &PretargetingConfigList{
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
	//   "description": "Retrieves a list of the authenticated user's pretargeting configurations.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.pretargetingConfig.list",
	//   "parameterOrder": [
	//     "accountId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to get the pretargeting configs for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "pretargetingconfigs/{accountId}",
	//   "response": {
	//     "$ref": "PretargetingConfigList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pretargetingConfig.patch":

type PretargetingConfigPatchCall struct {
	s                  *Service
	accountId          int64
	configId           int64
	pretargetingconfig *PretargetingConfig
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Patch: Updates an existing pretargeting config. This method supports
// patch semantics.
func (r *PretargetingConfigService) Patch(accountId int64, configId int64, pretargetingconfig *PretargetingConfig) *PretargetingConfigPatchCall {
	c := &PretargetingConfigPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.configId = configId
	c.pretargetingconfig = pretargetingconfig
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PretargetingConfigPatchCall) Fields(s ...googleapi.Field) *PretargetingConfigPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PretargetingConfigPatchCall) Context(ctx context.Context) *PretargetingConfigPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PretargetingConfigPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PretargetingConfigPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pretargetingconfig)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "pretargetingconfigs/{accountId}/{configId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"configId":  strconv.FormatInt(c.configId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pretargetingConfig.patch" call.
// Exactly one of *PretargetingConfig or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PretargetingConfig.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PretargetingConfigPatchCall) Do(opts ...googleapi.CallOption) (*PretargetingConfig, error) {
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
	ret := &PretargetingConfig{
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
	//   "description": "Updates an existing pretargeting config. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "adexchangebuyer.pretargetingConfig.patch",
	//   "parameterOrder": [
	//     "accountId",
	//     "configId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to update the pretargeting config for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "configId": {
	//       "description": "The specific id of the configuration to update.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "pretargetingconfigs/{accountId}/{configId}",
	//   "request": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "response": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pretargetingConfig.update":

type PretargetingConfigUpdateCall struct {
	s                  *Service
	accountId          int64
	configId           int64
	pretargetingconfig *PretargetingConfig
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Update: Updates an existing pretargeting config.
func (r *PretargetingConfigService) Update(accountId int64, configId int64, pretargetingconfig *PretargetingConfig) *PretargetingConfigUpdateCall {
	c := &PretargetingConfigUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	c.configId = configId
	c.pretargetingconfig = pretargetingconfig
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PretargetingConfigUpdateCall) Fields(s ...googleapi.Field) *PretargetingConfigUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PretargetingConfigUpdateCall) Context(ctx context.Context) *PretargetingConfigUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PretargetingConfigUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PretargetingConfigUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.pretargetingconfig)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "pretargetingconfigs/{accountId}/{configId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
		"configId":  strconv.FormatInt(c.configId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pretargetingConfig.update" call.
// Exactly one of *PretargetingConfig or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *PretargetingConfig.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PretargetingConfigUpdateCall) Do(opts ...googleapi.CallOption) (*PretargetingConfig, error) {
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
	ret := &PretargetingConfig{
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
	//   "description": "Updates an existing pretargeting config.",
	//   "httpMethod": "PUT",
	//   "id": "adexchangebuyer.pretargetingConfig.update",
	//   "parameterOrder": [
	//     "accountId",
	//     "configId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The account id to update the pretargeting config for.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "configId": {
	//       "description": "The specific id of the configuration to update.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "pretargetingconfigs/{accountId}/{configId}",
	//   "request": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "response": {
	//     "$ref": "PretargetingConfig"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.products.get":

type ProductsGetCall struct {
	s            *Service
	productId    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the requested product by id.
func (r *ProductsService) Get(productId string) *ProductsGetCall {
	c := &ProductsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.productId = productId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProductsGetCall) Fields(s ...googleapi.Field) *ProductsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProductsGetCall) IfNoneMatch(entityTag string) *ProductsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProductsGetCall) Context(ctx context.Context) *ProductsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProductsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProductsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "products/{productId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"productId": c.productId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.products.get" call.
// Exactly one of *Product or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Product.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProductsGetCall) Do(opts ...googleapi.CallOption) (*Product, error) {
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
	ret := &Product{
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
	//   "description": "Gets the requested product by id.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.products.get",
	//   "parameterOrder": [
	//     "productId"
	//   ],
	//   "parameters": {
	//     "productId": {
	//       "description": "The id for the product to get the head revision for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "products/{productId}",
	//   "response": {
	//     "$ref": "Product"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.products.search":

type ProductsSearchCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Search: Gets the requested product.
func (r *ProductsService) Search() *ProductsSearchCall {
	c := &ProductsSearchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// PqlQuery sets the optional parameter "pqlQuery": The pql query used
// to query for products.
func (c *ProductsSearchCall) PqlQuery(pqlQuery string) *ProductsSearchCall {
	c.urlParams_.Set("pqlQuery", pqlQuery)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProductsSearchCall) Fields(s ...googleapi.Field) *ProductsSearchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProductsSearchCall) IfNoneMatch(entityTag string) *ProductsSearchCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProductsSearchCall) Context(ctx context.Context) *ProductsSearchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProductsSearchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProductsSearchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "products/search")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.products.search" call.
// Exactly one of *GetOffersResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GetOffersResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProductsSearchCall) Do(opts ...googleapi.CallOption) (*GetOffersResponse, error) {
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
	ret := &GetOffersResponse{
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
	//   "description": "Gets the requested product.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.products.search",
	//   "parameters": {
	//     "pqlQuery": {
	//       "description": "The pql query used to query for products.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "products/search",
	//   "response": {
	//     "$ref": "GetOffersResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.proposals.get":

type ProposalsGetCall struct {
	s            *Service
	proposalId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Get a proposal given its id
func (r *ProposalsService) Get(proposalId string) *ProposalsGetCall {
	c := &ProposalsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProposalsGetCall) Fields(s ...googleapi.Field) *ProposalsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProposalsGetCall) IfNoneMatch(entityTag string) *ProposalsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProposalsGetCall) Context(ctx context.Context) *ProposalsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProposalsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProposalsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.proposals.get" call.
// Exactly one of *Proposal or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Proposal.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProposalsGetCall) Do(opts ...googleapi.CallOption) (*Proposal, error) {
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
	ret := &Proposal{
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
	//   "description": "Get a proposal given its id",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.proposals.get",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "Id of the proposal to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}",
	//   "response": {
	//     "$ref": "Proposal"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.proposals.insert":

type ProposalsInsertCall struct {
	s                   *Service
	createordersrequest *CreateOrdersRequest
	urlParams_          gensupport.URLParams
	ctx_                context.Context
	header_             http.Header
}

// Insert: Create the given list of proposals
func (r *ProposalsService) Insert(createordersrequest *CreateOrdersRequest) *ProposalsInsertCall {
	c := &ProposalsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.createordersrequest = createordersrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProposalsInsertCall) Fields(s ...googleapi.Field) *ProposalsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProposalsInsertCall) Context(ctx context.Context) *ProposalsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProposalsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProposalsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.createordersrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/insert")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.proposals.insert" call.
// Exactly one of *CreateOrdersResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CreateOrdersResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProposalsInsertCall) Do(opts ...googleapi.CallOption) (*CreateOrdersResponse, error) {
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
	ret := &CreateOrdersResponse{
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
	//   "description": "Create the given list of proposals",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.proposals.insert",
	//   "path": "proposals/insert",
	//   "request": {
	//     "$ref": "CreateOrdersRequest"
	//   },
	//   "response": {
	//     "$ref": "CreateOrdersResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.proposals.patch":

type ProposalsPatchCall struct {
	s              *Service
	proposalId     string
	revisionNumber int64
	updateAction   string
	proposal       *Proposal
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Patch: Update the given proposal. This method supports patch
// semantics.
func (r *ProposalsService) Patch(proposalId string, revisionNumber int64, updateAction string, proposal *Proposal) *ProposalsPatchCall {
	c := &ProposalsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	c.revisionNumber = revisionNumber
	c.updateAction = updateAction
	c.proposal = proposal
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProposalsPatchCall) Fields(s ...googleapi.Field) *ProposalsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProposalsPatchCall) Context(ctx context.Context) *ProposalsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProposalsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProposalsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.proposal)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/{revisionNumber}/{updateAction}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId":     c.proposalId,
		"revisionNumber": strconv.FormatInt(c.revisionNumber, 10),
		"updateAction":   c.updateAction,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.proposals.patch" call.
// Exactly one of *Proposal or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Proposal.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProposalsPatchCall) Do(opts ...googleapi.CallOption) (*Proposal, error) {
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
	ret := &Proposal{
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
	//   "description": "Update the given proposal. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "adexchangebuyer.proposals.patch",
	//   "parameterOrder": [
	//     "proposalId",
	//     "revisionNumber",
	//     "updateAction"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "The proposal id to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "revisionNumber": {
	//       "description": "The last known revision number to update. If the head revision in the marketplace database has since changed, an error will be thrown. The caller should then fetch the latest proposal at head revision and retry the update at that revision.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "updateAction": {
	//       "description": "The proposed action to take on the proposal. This field is required and it must be set when updating a proposal.",
	//       "enum": [
	//         "accept",
	//         "cancel",
	//         "propose",
	//         "proposeAndAccept",
	//         "unknownAction",
	//         "updateFinalized"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/{revisionNumber}/{updateAction}",
	//   "request": {
	//     "$ref": "Proposal"
	//   },
	//   "response": {
	//     "$ref": "Proposal"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.proposals.search":

type ProposalsSearchCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Search: Search for proposals using pql query
func (r *ProposalsService) Search() *ProposalsSearchCall {
	c := &ProposalsSearchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// PqlQuery sets the optional parameter "pqlQuery": Query string to
// retrieve specific proposals.
func (c *ProposalsSearchCall) PqlQuery(pqlQuery string) *ProposalsSearchCall {
	c.urlParams_.Set("pqlQuery", pqlQuery)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProposalsSearchCall) Fields(s ...googleapi.Field) *ProposalsSearchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProposalsSearchCall) IfNoneMatch(entityTag string) *ProposalsSearchCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProposalsSearchCall) Context(ctx context.Context) *ProposalsSearchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProposalsSearchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProposalsSearchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/search")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.proposals.search" call.
// Exactly one of *GetOrdersResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GetOrdersResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProposalsSearchCall) Do(opts ...googleapi.CallOption) (*GetOrdersResponse, error) {
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
	ret := &GetOrdersResponse{
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
	//   "description": "Search for proposals using pql query",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.proposals.search",
	//   "parameters": {
	//     "pqlQuery": {
	//       "description": "Query string to retrieve specific proposals.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/search",
	//   "response": {
	//     "$ref": "GetOrdersResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.proposals.setupcomplete":

type ProposalsSetupcompleteCall struct {
	s          *Service
	proposalId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Setupcomplete: Update the given proposal to indicate that setup has
// been completed.
func (r *ProposalsService) Setupcomplete(proposalId string) *ProposalsSetupcompleteCall {
	c := &ProposalsSetupcompleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProposalsSetupcompleteCall) Fields(s ...googleapi.Field) *ProposalsSetupcompleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProposalsSetupcompleteCall) Context(ctx context.Context) *ProposalsSetupcompleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProposalsSetupcompleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProposalsSetupcompleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/setupcomplete")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId": c.proposalId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.proposals.setupcomplete" call.
func (c *ProposalsSetupcompleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Update the given proposal to indicate that setup has been completed.",
	//   "httpMethod": "POST",
	//   "id": "adexchangebuyer.proposals.setupcomplete",
	//   "parameterOrder": [
	//     "proposalId"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "The proposal id for which the setup is complete",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/setupcomplete",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.proposals.update":

type ProposalsUpdateCall struct {
	s              *Service
	proposalId     string
	revisionNumber int64
	updateAction   string
	proposal       *Proposal
	urlParams_     gensupport.URLParams
	ctx_           context.Context
	header_        http.Header
}

// Update: Update the given proposal
func (r *ProposalsService) Update(proposalId string, revisionNumber int64, updateAction string, proposal *Proposal) *ProposalsUpdateCall {
	c := &ProposalsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.proposalId = proposalId
	c.revisionNumber = revisionNumber
	c.updateAction = updateAction
	c.proposal = proposal
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProposalsUpdateCall) Fields(s ...googleapi.Field) *ProposalsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProposalsUpdateCall) Context(ctx context.Context) *ProposalsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProposalsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProposalsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.proposal)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "proposals/{proposalId}/{revisionNumber}/{updateAction}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"proposalId":     c.proposalId,
		"revisionNumber": strconv.FormatInt(c.revisionNumber, 10),
		"updateAction":   c.updateAction,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.proposals.update" call.
// Exactly one of *Proposal or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Proposal.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProposalsUpdateCall) Do(opts ...googleapi.CallOption) (*Proposal, error) {
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
	ret := &Proposal{
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
	//   "description": "Update the given proposal",
	//   "httpMethod": "PUT",
	//   "id": "adexchangebuyer.proposals.update",
	//   "parameterOrder": [
	//     "proposalId",
	//     "revisionNumber",
	//     "updateAction"
	//   ],
	//   "parameters": {
	//     "proposalId": {
	//       "description": "The proposal id to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "revisionNumber": {
	//       "description": "The last known revision number to update. If the head revision in the marketplace database has since changed, an error will be thrown. The caller should then fetch the latest proposal at head revision and retry the update at that revision.",
	//       "format": "int64",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "updateAction": {
	//       "description": "The proposed action to take on the proposal. This field is required and it must be set when updating a proposal.",
	//       "enum": [
	//         "accept",
	//         "cancel",
	//         "propose",
	//         "proposeAndAccept",
	//         "unknownAction",
	//         "updateFinalized"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "proposals/{proposalId}/{revisionNumber}/{updateAction}",
	//   "request": {
	//     "$ref": "Proposal"
	//   },
	//   "response": {
	//     "$ref": "Proposal"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

// method id "adexchangebuyer.pubprofiles.list":

type PubprofilesListCall struct {
	s            *Service
	accountId    int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Gets the requested publisher profile(s) by publisher accountId.
func (r *PubprofilesService) List(accountId int64) *PubprofilesListCall {
	c := &PubprofilesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.accountId = accountId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PubprofilesListCall) Fields(s ...googleapi.Field) *PubprofilesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PubprofilesListCall) IfNoneMatch(entityTag string) *PubprofilesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PubprofilesListCall) Context(ctx context.Context) *PubprofilesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PubprofilesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PubprofilesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "publisher/{accountId}/profiles")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"accountId": strconv.FormatInt(c.accountId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "adexchangebuyer.pubprofiles.list" call.
// Exactly one of *GetPublisherProfilesByAccountIdResponse or error will
// be non-nil. Any non-2xx status code is an error. Response headers are
// in either
// *GetPublisherProfilesByAccountIdResponse.ServerResponse.Header or (if
// a response was returned at all) in error.(*googleapi.Error).Header.
// Use googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PubprofilesListCall) Do(opts ...googleapi.CallOption) (*GetPublisherProfilesByAccountIdResponse, error) {
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
	ret := &GetPublisherProfilesByAccountIdResponse{
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
	//   "description": "Gets the requested publisher profile(s) by publisher accountId.",
	//   "httpMethod": "GET",
	//   "id": "adexchangebuyer.pubprofiles.list",
	//   "parameterOrder": [
	//     "accountId"
	//   ],
	//   "parameters": {
	//     "accountId": {
	//       "description": "The accountId of the publisher to get profiles for.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "publisher/{accountId}/profiles",
	//   "response": {
	//     "$ref": "GetPublisherProfilesByAccountIdResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/adexchange.buyer"
	//   ]
	// }

}

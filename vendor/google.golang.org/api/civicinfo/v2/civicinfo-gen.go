// Package civicinfo provides access to the Google Civic Information API.
//
// See https://developers.google.com/civic-information
//
// Usage example:
//
//   import "google.golang.org/api/civicinfo/v2"
//   ...
//   civicinfoService, err := civicinfo.New(oauthHttpClient)
package civicinfo // import "google.golang.org/api/civicinfo/v2"

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

const apiId = "civicinfo:v2"
const apiName = "civicinfo"
const apiVersion = "v2"
const basePath = "https://www.googleapis.com/civicinfo/v2/"

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Divisions = NewDivisionsService(s)
	s.Elections = NewElectionsService(s)
	s.Representatives = NewRepresentativesService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Divisions *DivisionsService

	Elections *ElectionsService

	Representatives *RepresentativesService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewDivisionsService(s *Service) *DivisionsService {
	rs := &DivisionsService{s: s}
	return rs
}

type DivisionsService struct {
	s *Service
}

func NewElectionsService(s *Service) *ElectionsService {
	rs := &ElectionsService{s: s}
	return rs
}

type ElectionsService struct {
	s *Service
}

func NewRepresentativesService(s *Service) *RepresentativesService {
	rs := &RepresentativesService{s: s}
	return rs
}

type RepresentativesService struct {
	s *Service
}

// AdministrationRegion: Describes information about a regional election
// administrative area.
type AdministrationRegion struct {
	// ElectionAdministrationBody: The election administration body for this
	// area.
	ElectionAdministrationBody *AdministrativeBody `json:"electionAdministrationBody,omitempty"`

	// Id: An ID for this object. IDs may change in future requests and
	// should not be cached. Access to this field requires special access
	// that can be requested from the Request more link on the Quotas page.
	Id string `json:"id,omitempty"`

	// LocalJurisdiction: The city or county that provides election
	// information for this voter. This object can have the same elements as
	// state.
	LocalJurisdiction *AdministrationRegion `json:"local_jurisdiction,omitempty"`

	// Name: The name of the jurisdiction.
	Name string `json:"name,omitempty"`

	// Sources: A list of sources for this area. If multiple sources are
	// listed the data has been aggregated from those sources.
	Sources []*Source `json:"sources,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "ElectionAdministrationBody") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "ElectionAdministrationBody") to include in API requests with the
	// JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AdministrationRegion) MarshalJSON() ([]byte, error) {
	type noMethod AdministrationRegion
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AdministrativeBody: Information about an election administrative body
// (e.g. County Board of Elections).
type AdministrativeBody struct {
	// AbsenteeVotingInfoUrl: A URL provided by this administrative body for
	// information on absentee voting.
	AbsenteeVotingInfoUrl string `json:"absenteeVotingInfoUrl,omitempty"`

	AddressLines []string `json:"addressLines,omitempty"`

	// BallotInfoUrl: A URL provided by this administrative body to give
	// contest information to the voter.
	BallotInfoUrl string `json:"ballotInfoUrl,omitempty"`

	// CorrespondenceAddress: The mailing address of this administrative
	// body.
	CorrespondenceAddress *SimpleAddressType `json:"correspondenceAddress,omitempty"`

	// ElectionInfoUrl: A URL provided by this administrative body for
	// looking up general election information.
	ElectionInfoUrl string `json:"electionInfoUrl,omitempty"`

	// ElectionOfficials: The election officials for this election
	// administrative body.
	ElectionOfficials []*ElectionOfficial `json:"electionOfficials,omitempty"`

	// ElectionRegistrationConfirmationUrl: A URL provided by this
	// administrative body for confirming that the voter is registered to
	// vote.
	ElectionRegistrationConfirmationUrl string `json:"electionRegistrationConfirmationUrl,omitempty"`

	// ElectionRegistrationUrl: A URL provided by this administrative body
	// for looking up how to register to vote.
	ElectionRegistrationUrl string `json:"electionRegistrationUrl,omitempty"`

	// ElectionRulesUrl: A URL provided by this administrative body
	// describing election rules to the voter.
	ElectionRulesUrl string `json:"electionRulesUrl,omitempty"`

	// HoursOfOperation: A description of the hours of operation for this
	// administrative body.
	HoursOfOperation string `json:"hoursOfOperation,omitempty"`

	// Name: The name of this election administrative body.
	Name string `json:"name,omitempty"`

	// PhysicalAddress: The physical address of this administrative body.
	PhysicalAddress *SimpleAddressType `json:"physicalAddress,omitempty"`

	// VoterServices: A description of the services this administrative body
	// may provide.
	VoterServices []string `json:"voter_services,omitempty"`

	// VotingLocationFinderUrl: A URL provided by this administrative body
	// for looking up where to vote.
	VotingLocationFinderUrl string `json:"votingLocationFinderUrl,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AbsenteeVotingInfoUrl") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AbsenteeVotingInfoUrl") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *AdministrativeBody) MarshalJSON() ([]byte, error) {
	type noMethod AdministrativeBody
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Candidate: Information about a candidate running for elected office.
type Candidate struct {
	// CandidateUrl: The URL for the candidate's campaign web site.
	CandidateUrl string `json:"candidateUrl,omitempty"`

	// Channels: A list of known (social) media channels for this candidate.
	Channels []*Channel `json:"channels,omitempty"`

	// Email: The email address for the candidate's campaign.
	Email string `json:"email,omitempty"`

	// Name: The candidate's name. If this is a joint ticket it will
	// indicate the name of the candidate at the top of a ticket followed by
	// a / and that name of candidate at the bottom of the ticket. e.g.
	// "Mitt Romney / Paul Ryan"
	Name string `json:"name,omitempty"`

	// OrderOnBallot: The order the candidate appears on the ballot for this
	// contest.
	OrderOnBallot int64 `json:"orderOnBallot,omitempty,string"`

	// Party: The full name of the party the candidate is a member of.
	Party string `json:"party,omitempty"`

	// Phone: The voice phone number for the candidate's campaign office.
	Phone string `json:"phone,omitempty"`

	// PhotoUrl: A URL for a photo of the candidate.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CandidateUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CandidateUrl") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Candidate) MarshalJSON() ([]byte, error) {
	type noMethod Candidate
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Channel: A social media or web channel for a candidate.
type Channel struct {
	// Id: The unique public identifier for the candidate's channel.
	Id string `json:"id,omitempty"`

	// Type: The type of channel. The following is a list of types of
	// channels, but is not exhaustive. More channel types may be added at a
	// later time. One of: GooglePlus, YouTube, Facebook, Twitter
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

func (s *Channel) MarshalJSON() ([]byte, error) {
	type noMethod Channel
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Contest: Information about a contest that appears on a voter's
// ballot.
type Contest struct {
	// BallotPlacement: A number specifying the position of this contest on
	// the voter's ballot.
	BallotPlacement int64 `json:"ballotPlacement,omitempty,string"`

	// Candidates: The candidate choices for this contest.
	Candidates []*Candidate `json:"candidates,omitempty"`

	// District: Information about the electoral district that this contest
	// is in.
	District *ElectoralDistrict `json:"district,omitempty"`

	// ElectorateSpecifications: A description of any additional eligibility
	// requirements for voting in this contest.
	ElectorateSpecifications string `json:"electorateSpecifications,omitempty"`

	// Id: An ID for this object. IDs may change in future requests and
	// should not be cached. Access to this field requires special access
	// that can be requested from the Request more link on the Quotas page.
	Id string `json:"id,omitempty"`

	// Level: The levels of government of the office for this contest. There
	// may be more than one in cases where a jurisdiction effectively acts
	// at two different levels of government; for example, the mayor of the
	// District of Columbia acts at "locality" level, but also effectively
	// at both "administrative-area-2" and "administrative-area-1".
	Level []string `json:"level,omitempty"`

	// NumberElected: The number of candidates that will be elected to
	// office in this contest.
	NumberElected int64 `json:"numberElected,omitempty,string"`

	// NumberVotingFor: The number of candidates that a voter may vote for
	// in this contest.
	NumberVotingFor int64 `json:"numberVotingFor,omitempty,string"`

	// Office: The name of the office for this contest.
	Office string `json:"office,omitempty"`

	// PrimaryParty: If this is a partisan election, the name of the party
	// it is for.
	PrimaryParty string `json:"primaryParty,omitempty"`

	// ReferendumBallotResponses: The set of ballot responses for the
	// referendum. A ballot response represents a line on the ballot. Common
	// examples might include "yes" or "no" for referenda. This field is
	// only populated for contests of type 'Referendum'.
	ReferendumBallotResponses []string `json:"referendumBallotResponses,omitempty"`

	// ReferendumBrief: Specifies a short summary of the referendum that is
	// typically on the ballot below the title but above the text. This
	// field is only populated for contests of type 'Referendum'.
	ReferendumBrief string `json:"referendumBrief,omitempty"`

	// ReferendumConStatement: A statement in opposition to the referendum.
	// It does not necessarily appear on the ballot. This field is only
	// populated for contests of type 'Referendum'.
	ReferendumConStatement string `json:"referendumConStatement,omitempty"`

	// ReferendumEffectOfAbstain: Specifies what effect abstaining (not
	// voting) on the proposition will have (i.e. whether abstaining is
	// considered a vote against it). This field is only populated for
	// contests of type 'Referendum'.
	ReferendumEffectOfAbstain string `json:"referendumEffectOfAbstain,omitempty"`

	// ReferendumPassageThreshold: The threshold of votes that the
	// referendum needs in order to pass, e.g. "two-thirds". This field is
	// only populated for contests of type 'Referendum'.
	ReferendumPassageThreshold string `json:"referendumPassageThreshold,omitempty"`

	// ReferendumProStatement: A statement in favor of the referendum. It
	// does not necessarily appear on the ballot. This field is only
	// populated for contests of type 'Referendum'.
	ReferendumProStatement string `json:"referendumProStatement,omitempty"`

	// ReferendumSubtitle: A brief description of the referendum. This field
	// is only populated for contests of type 'Referendum'.
	ReferendumSubtitle string `json:"referendumSubtitle,omitempty"`

	// ReferendumText: The full text of the referendum. This field is only
	// populated for contests of type 'Referendum'.
	ReferendumText string `json:"referendumText,omitempty"`

	// ReferendumTitle: The title of the referendum (e.g. 'Proposition 42').
	// This field is only populated for contests of type 'Referendum'.
	ReferendumTitle string `json:"referendumTitle,omitempty"`

	// ReferendumUrl: A link to the referendum. This field is only populated
	// for contests of type 'Referendum'.
	ReferendumUrl string `json:"referendumUrl,omitempty"`

	// Roles: The roles which this office fulfills.
	Roles []string `json:"roles,omitempty"`

	// Sources: A list of sources for this contest. If multiple sources are
	// listed, the data has been aggregated from those sources.
	Sources []*Source `json:"sources,omitempty"`

	// Special: "Yes" or "No" depending on whether this a contest being held
	// outside the normal election cycle.
	Special string `json:"special,omitempty"`

	// Type: The type of contest. Usually this will be 'General', 'Primary',
	// or 'Run-off' for contests with candidates. For referenda this will be
	// 'Referendum'. For Retention contests this will typically be
	// 'Retention'.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BallotPlacement") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BallotPlacement") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Contest) MarshalJSON() ([]byte, error) {
	type noMethod Contest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ContextParams struct {
	ClientProfile string `json:"clientProfile,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ClientProfile") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ClientProfile") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ContextParams) MarshalJSON() ([]byte, error) {
	type noMethod ContextParams
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DivisionRepresentativeInfoRequest: A request to look up
// representative information for a single division.
type DivisionRepresentativeInfoRequest struct {
	ContextParams *ContextParams `json:"contextParams,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContextParams") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContextParams") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DivisionRepresentativeInfoRequest) MarshalJSON() ([]byte, error) {
	type noMethod DivisionRepresentativeInfoRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DivisionSearchRequest: A search request for political geographies.
type DivisionSearchRequest struct {
	ContextParams *ContextParams `json:"contextParams,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContextParams") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContextParams") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DivisionSearchRequest) MarshalJSON() ([]byte, error) {
	type noMethod DivisionSearchRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DivisionSearchResponse: The result of a division search query.
type DivisionSearchResponse struct {
	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "civicinfo#divisionSearchResponse".
	Kind string `json:"kind,omitempty"`

	Results []*DivisionSearchResult `json:"results,omitempty"`

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

func (s *DivisionSearchResponse) MarshalJSON() ([]byte, error) {
	type noMethod DivisionSearchResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DivisionSearchResult: Represents a political geographic division that
// matches the requested query.
type DivisionSearchResult struct {
	// Aliases: Other Open Civic Data identifiers that refer to the same
	// division -- for example, those that refer to other political
	// divisions whose boundaries are defined to be coterminous with this
	// one. For example, ocd-division/country:us/state:wy will include an
	// alias of ocd-division/country:us/state:wy/cd:1, since Wyoming has
	// only one Congressional district.
	Aliases []string `json:"aliases,omitempty"`

	// Name: The name of the division.
	Name string `json:"name,omitempty"`

	// OcdId: The unique Open Civic Data identifier for this division.
	OcdId string `json:"ocdId,omitempty"`

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

func (s *DivisionSearchResult) MarshalJSON() ([]byte, error) {
	type noMethod DivisionSearchResult
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Election: Information about the election that was queried.
type Election struct {
	// ElectionDay: Day of the election in YYYY-MM-DD format.
	ElectionDay string `json:"electionDay,omitempty"`

	// Id: The unique ID of this election.
	Id int64 `json:"id,omitempty,string"`

	// Name: A displayable name for the election.
	Name string `json:"name,omitempty"`

	// OcdDivisionId: The political division of the election. Represented as
	// an OCD Division ID. Voters within these political jurisdictions are
	// covered by this election. This is typically a state such as
	// ocd-division/country:us/state:ca or for the midterms or general
	// election the entire US (i.e. ocd-division/country:us).
	OcdDivisionId string `json:"ocdDivisionId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ElectionDay") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ElectionDay") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Election) MarshalJSON() ([]byte, error) {
	type noMethod Election
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ElectionOfficial: Information about individual election officials.
type ElectionOfficial struct {
	// EmailAddress: The email address of the election official.
	EmailAddress string `json:"emailAddress,omitempty"`

	// FaxNumber: The fax number of the election official.
	FaxNumber string `json:"faxNumber,omitempty"`

	// Name: The full name of the election official.
	Name string `json:"name,omitempty"`

	// OfficePhoneNumber: The office phone number of the election official.
	OfficePhoneNumber string `json:"officePhoneNumber,omitempty"`

	// Title: The title of the election official.
	Title string `json:"title,omitempty"`

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

func (s *ElectionOfficial) MarshalJSON() ([]byte, error) {
	type noMethod ElectionOfficial
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ElectionsQueryRequest struct {
	ContextParams *ContextParams `json:"contextParams,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContextParams") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContextParams") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ElectionsQueryRequest) MarshalJSON() ([]byte, error) {
	type noMethod ElectionsQueryRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ElectionsQueryResponse: The list of elections available for this
// version of the API.
type ElectionsQueryResponse struct {
	// Elections: A list of available elections
	Elections []*Election `json:"elections,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "civicinfo#electionsQueryResponse".
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Elections") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Elections") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ElectionsQueryResponse) MarshalJSON() ([]byte, error) {
	type noMethod ElectionsQueryResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ElectoralDistrict: Describes the geographic scope of a contest.
type ElectoralDistrict struct {
	// Id: An identifier for this district, relative to its scope. For
	// example, the 34th State Senate district would have id "34" and a
	// scope of stateUpper.
	Id string `json:"id,omitempty"`

	KgForeignKey string `json:"kgForeignKey,omitempty"`

	// Name: The name of the district.
	Name string `json:"name,omitempty"`

	// Scope: The geographic scope of this district. If unspecified the
	// district's geography is not known. One of: national, statewide,
	// congressional, stateUpper, stateLower, countywide, judicial,
	// schoolBoard, cityWide, township, countyCouncil, cityCouncil, ward,
	// special
	Scope string `json:"scope,omitempty"`

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

func (s *ElectoralDistrict) MarshalJSON() ([]byte, error) {
	type noMethod ElectoralDistrict
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeographicDivision: Describes a political geography.
type GeographicDivision struct {
	// AlsoKnownAs: Any other valid OCD IDs that refer to the same
	// division.
	//
	// Because OCD IDs are meant to be human-readable and at least somewhat
	// predictable, there are occasionally several identifiers for a single
	// division. These identifiers are defined to be equivalent to one
	// another, and one is always indicated as the primary identifier. The
	// primary identifier will be returned in ocd_id above, and any other
	// equivalent valid identifiers will be returned in this list.
	//
	// For example, if this division's OCD ID is
	// ocd-division/country:us/district:dc, this will contain
	// ocd-division/country:us/state:dc.
	AlsoKnownAs []string `json:"alsoKnownAs,omitempty"`

	// Name: The name of the division.
	Name string `json:"name,omitempty"`

	// OfficeIndices: List of indices in the offices array, one for each
	// office elected from this division. Will only be present if
	// includeOffices was true (or absent) in the request.
	OfficeIndices []int64 `json:"officeIndices,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AlsoKnownAs") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlsoKnownAs") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeographicDivision) MarshalJSON() ([]byte, error) {
	type noMethod GeographicDivision
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Office: Information about an Office held by one or more Officials.
type Office struct {
	// DivisionId: The OCD ID of the division with which this office is
	// associated.
	DivisionId string `json:"divisionId,omitempty"`

	// Levels: The levels of government of which this office is part. There
	// may be more than one in cases where a jurisdiction effectively acts
	// at two different levels of government; for example, the mayor of the
	// District of Columbia acts at "locality" level, but also effectively
	// at both "administrative-area-2" and "administrative-area-1".
	Levels []string `json:"levels,omitempty"`

	// Name: The human-readable name of the office.
	Name string `json:"name,omitempty"`

	// OfficialIndices: List of indices in the officials array of people who
	// presently hold this office.
	OfficialIndices []int64 `json:"officialIndices,omitempty"`

	// Roles: The roles which this office fulfills. Roles are not meant to
	// be exhaustive, or to exactly specify the entire set of
	// responsibilities of a given office, but are meant to be rough
	// categories that are useful for general selection from or sorting of a
	// list of offices.
	Roles []string `json:"roles,omitempty"`

	// Sources: A list of sources for this office. If multiple sources are
	// listed, the data has been aggregated from those sources.
	Sources []*Source `json:"sources,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DivisionId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DivisionId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Office) MarshalJSON() ([]byte, error) {
	type noMethod Office
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Official: Information about a person holding an elected office.
type Official struct {
	// Address: Addresses at which to contact the official.
	Address []*SimpleAddressType `json:"address,omitempty"`

	// Channels: A list of known (social) media channels for this official.
	Channels []*Channel `json:"channels,omitempty"`

	// Emails: The direct email addresses for the official.
	Emails []string `json:"emails,omitempty"`

	// Name: The official's name.
	Name string `json:"name,omitempty"`

	// Party: The full name of the party the official belongs to.
	Party string `json:"party,omitempty"`

	// Phones: The official's public contact phone numbers.
	Phones []string `json:"phones,omitempty"`

	// PhotoUrl: A URL for a photo of the official.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// Urls: The official's public website URLs.
	Urls []string `json:"urls,omitempty"`

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

func (s *Official) MarshalJSON() ([]byte, error) {
	type noMethod Official
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PollingLocation: A location where a voter can vote. This may be an
// early vote site, an election day voting location, or a drop off
// location for a completed ballot.
type PollingLocation struct {
	// Address: The address of the location.
	Address *SimpleAddressType `json:"address,omitempty"`

	// EndDate: The last date that this early vote site or drop off location
	// may be used. This field is not populated for polling locations.
	EndDate string `json:"endDate,omitempty"`

	// Id: An ID for this object. IDs may change in future requests and
	// should not be cached. Access to this field requires special access
	// that can be requested from the Request more link on the Quotas page.
	Id string `json:"id,omitempty"`

	// Name: The name of the early vote site or drop off location. This
	// field is not populated for polling locations.
	Name string `json:"name,omitempty"`

	// Notes: Notes about this location (e.g. accessibility ramp or entrance
	// to use).
	Notes string `json:"notes,omitempty"`

	// PollingHours: A description of when this location is open.
	PollingHours string `json:"pollingHours,omitempty"`

	// Sources: A list of sources for this location. If multiple sources are
	// listed the data has been aggregated from those sources.
	Sources []*Source `json:"sources,omitempty"`

	// StartDate: The first date that this early vote site or drop off
	// location may be used. This field is not populated for polling
	// locations.
	StartDate string `json:"startDate,omitempty"`

	// VoterServices: The services provided by this early vote site or drop
	// off location. This field is not populated for polling locations.
	VoterServices string `json:"voterServices,omitempty"`

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

func (s *PollingLocation) MarshalJSON() ([]byte, error) {
	type noMethod PollingLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type PostalAddress struct {
	AddressLines []string `json:"addressLines,omitempty"`

	AdministrativeAreaName string `json:"administrativeAreaName,omitempty"`

	CountryName string `json:"countryName,omitempty"`

	CountryNameCode string `json:"countryNameCode,omitempty"`

	DependentLocalityName string `json:"dependentLocalityName,omitempty"`

	DependentThoroughfareLeadingType string `json:"dependentThoroughfareLeadingType,omitempty"`

	DependentThoroughfareName string `json:"dependentThoroughfareName,omitempty"`

	DependentThoroughfarePostDirection string `json:"dependentThoroughfarePostDirection,omitempty"`

	DependentThoroughfarePreDirection string `json:"dependentThoroughfarePreDirection,omitempty"`

	DependentThoroughfareTrailingType string `json:"dependentThoroughfareTrailingType,omitempty"`

	DependentThoroughfaresConnector string `json:"dependentThoroughfaresConnector,omitempty"`

	DependentThoroughfaresIndicator string `json:"dependentThoroughfaresIndicator,omitempty"`

	DependentThoroughfaresType string `json:"dependentThoroughfaresType,omitempty"`

	FirmName string `json:"firmName,omitempty"`

	IsDisputed bool `json:"isDisputed,omitempty"`

	LanguageCode string `json:"languageCode,omitempty"`

	LocalityName string `json:"localityName,omitempty"`

	PostBoxNumber string `json:"postBoxNumber,omitempty"`

	PostalCodeNumber string `json:"postalCodeNumber,omitempty"`

	PostalCodeNumberExtension string `json:"postalCodeNumberExtension,omitempty"`

	PremiseName string `json:"premiseName,omitempty"`

	RecipientName string `json:"recipientName,omitempty"`

	SortingCode string `json:"sortingCode,omitempty"`

	SubAdministrativeAreaName string `json:"subAdministrativeAreaName,omitempty"`

	SubPremiseName string `json:"subPremiseName,omitempty"`

	ThoroughfareLeadingType string `json:"thoroughfareLeadingType,omitempty"`

	ThoroughfareName string `json:"thoroughfareName,omitempty"`

	ThoroughfareNumber string `json:"thoroughfareNumber,omitempty"`

	ThoroughfarePostDirection string `json:"thoroughfarePostDirection,omitempty"`

	ThoroughfarePreDirection string `json:"thoroughfarePreDirection,omitempty"`

	ThoroughfareTrailingType string `json:"thoroughfareTrailingType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AddressLines") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AddressLines") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PostalAddress) MarshalJSON() ([]byte, error) {
	type noMethod PostalAddress
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type RepresentativeInfoData struct {
	// Divisions: Political geographic divisions that contain the requested
	// address.
	Divisions map[string]GeographicDivision `json:"divisions,omitempty"`

	// Offices: Elected offices referenced by the divisions listed above.
	// Will only be present if includeOffices was true in the request.
	Offices []*Office `json:"offices,omitempty"`

	// Officials: Officials holding the offices listed above. Will only be
	// present if includeOffices was true in the request.
	Officials []*Official `json:"officials,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Divisions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Divisions") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RepresentativeInfoData) MarshalJSON() ([]byte, error) {
	type noMethod RepresentativeInfoData
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RepresentativeInfoRequest: A request for political geography and
// representative information for an address.
type RepresentativeInfoRequest struct {
	ContextParams *ContextParams `json:"contextParams,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContextParams") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContextParams") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RepresentativeInfoRequest) MarshalJSON() ([]byte, error) {
	type noMethod RepresentativeInfoRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RepresentativeInfoResponse: The result of a representative info
// lookup query.
type RepresentativeInfoResponse struct {
	// Divisions: Political geographic divisions that contain the requested
	// address.
	Divisions map[string]GeographicDivision `json:"divisions,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "civicinfo#representativeInfoResponse".
	Kind string `json:"kind,omitempty"`

	// NormalizedInput: The normalized version of the requested address
	NormalizedInput *SimpleAddressType `json:"normalizedInput,omitempty"`

	// Offices: Elected offices referenced by the divisions listed above.
	// Will only be present if includeOffices was true in the request.
	Offices []*Office `json:"offices,omitempty"`

	// Officials: Officials holding the offices listed above. Will only be
	// present if includeOffices was true in the request.
	Officials []*Official `json:"officials,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Divisions") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Divisions") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RepresentativeInfoResponse) MarshalJSON() ([]byte, error) {
	type noMethod RepresentativeInfoResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SimpleAddressType: A simple representation of an address.
type SimpleAddressType struct {
	// City: The city or town for the address.
	City string `json:"city,omitempty"`

	// Line1: The street name and number of this address.
	Line1 string `json:"line1,omitempty"`

	// Line2: The second line the address, if needed.
	Line2 string `json:"line2,omitempty"`

	// Line3: The third line of the address, if needed.
	Line3 string `json:"line3,omitempty"`

	// LocationName: The name of the location.
	LocationName string `json:"locationName,omitempty"`

	// State: The US two letter state abbreviation of the address.
	State string `json:"state,omitempty"`

	// Zip: The US Postal Zip Code of the address.
	Zip string `json:"zip,omitempty"`

	// ForceSendFields is a list of field names (e.g. "City") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "City") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SimpleAddressType) MarshalJSON() ([]byte, error) {
	type noMethod SimpleAddressType
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Source: Contains information about the data source for the element
// containing it.
type Source struct {
	// Name: The name of the data source.
	Name string `json:"name,omitempty"`

	// Official: Whether this data comes from an official government source.
	Official bool `json:"official,omitempty"`

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

func (s *Source) MarshalJSON() ([]byte, error) {
	type noMethod Source
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VoterInfoRequest: A request for information about a voter.
type VoterInfoRequest struct {
	ContextParams *ContextParams `json:"contextParams,omitempty"`

	VoterInfoSegmentResult *VoterInfoSegmentResult `json:"voterInfoSegmentResult,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContextParams") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContextParams") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VoterInfoRequest) MarshalJSON() ([]byte, error) {
	type noMethod VoterInfoRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VoterInfoResponse: The result of a voter info lookup query.
type VoterInfoResponse struct {
	// Contests: Contests that will appear on the voter's ballot.
	Contests []*Contest `json:"contests,omitempty"`

	// DropOffLocations: Locations where a voter is eligible to drop off a
	// completed ballot. The voter must have received and completed a ballot
	// prior to arriving at the location. The location may not have ballots
	// available on the premises. These locations could be open on or before
	// election day as indicated in the pollingHours field.
	DropOffLocations []*PollingLocation `json:"dropOffLocations,omitempty"`

	// EarlyVoteSites: Locations where the voter is eligible to vote early,
	// prior to election day.
	EarlyVoteSites []*PollingLocation `json:"earlyVoteSites,omitempty"`

	// Election: The election that was queried.
	Election *Election `json:"election,omitempty"`

	// Kind: Identifies what kind of resource this is. Value: the fixed
	// string "civicinfo#voterInfoResponse".
	Kind string `json:"kind,omitempty"`

	// MailOnly: Specifies whether voters in the precinct vote only by
	// mailing their ballots (with the possible option of dropping off their
	// ballots as well).
	MailOnly bool `json:"mailOnly,omitempty"`

	// NormalizedInput: The normalized version of the requested address
	NormalizedInput *SimpleAddressType `json:"normalizedInput,omitempty"`

	// OtherElections: If no election ID was specified in the query, and
	// there was more than one election with data for the given voter, this
	// will contain information about the other elections that could apply.
	OtherElections []*Election `json:"otherElections,omitempty"`

	// PollingLocations: Locations where the voter is eligible to vote on
	// election day.
	PollingLocations []*PollingLocation `json:"pollingLocations,omitempty"`

	PrecinctId string `json:"precinctId,omitempty"`

	// State: Local Election Information for the state that the voter votes
	// in. For the US, there will only be one element in this array.
	State []*AdministrationRegion `json:"state,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Contests") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Contests") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VoterInfoResponse) MarshalJSON() ([]byte, error) {
	type noMethod VoterInfoResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VoterInfoSegmentResult struct {
	GeneratedMillis int64 `json:"generatedMillis,omitempty,string"`

	PostalAddress *PostalAddress `json:"postalAddress,omitempty"`

	Request *VoterInfoRequest `json:"request,omitempty"`

	Response *VoterInfoResponse `json:"response,omitempty"`

	// ForceSendFields is a list of field names (e.g. "GeneratedMillis") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GeneratedMillis") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VoterInfoSegmentResult) MarshalJSON() ([]byte, error) {
	type noMethod VoterInfoSegmentResult
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "civicinfo.divisions.search":

type DivisionsSearchCall struct {
	s                     *Service
	divisionsearchrequest *DivisionSearchRequest
	urlParams_            gensupport.URLParams
	ifNoneMatch_          string
	ctx_                  context.Context
	header_               http.Header
}

// Search: Searches for political divisions by their natural name or OCD
// ID.
func (r *DivisionsService) Search(divisionsearchrequest *DivisionSearchRequest) *DivisionsSearchCall {
	c := &DivisionsSearchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.divisionsearchrequest = divisionsearchrequest
	return c
}

// Query sets the optional parameter "query": The search query. Queries
// can cover any parts of a OCD ID or a human readable division name.
// All words given in the query are treated as required patterns. In
// addition to that, most query operators of the Apache Lucene library
// are supported. See
// http://lucene.apache.org/core/2_9_4/queryparsersyntax.html
func (c *DivisionsSearchCall) Query(query string) *DivisionsSearchCall {
	c.urlParams_.Set("query", query)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DivisionsSearchCall) Fields(s ...googleapi.Field) *DivisionsSearchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DivisionsSearchCall) IfNoneMatch(entityTag string) *DivisionsSearchCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DivisionsSearchCall) Context(ctx context.Context) *DivisionsSearchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DivisionsSearchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DivisionsSearchCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "divisions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "civicinfo.divisions.search" call.
// Exactly one of *DivisionSearchResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *DivisionSearchResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *DivisionsSearchCall) Do(opts ...googleapi.CallOption) (*DivisionSearchResponse, error) {
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
	ret := &DivisionSearchResponse{
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
	//   "description": "Searches for political divisions by their natural name or OCD ID.",
	//   "httpMethod": "GET",
	//   "id": "civicinfo.divisions.search",
	//   "parameters": {
	//     "query": {
	//       "description": "The search query. Queries can cover any parts of a OCD ID or a human readable division name. All words given in the query are treated as required patterns. In addition to that, most query operators of the Apache Lucene library are supported. See http://lucene.apache.org/core/2_9_4/queryparsersyntax.html",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "divisions",
	//   "request": {
	//     "$ref": "DivisionSearchRequest"
	//   },
	//   "response": {
	//     "$ref": "DivisionSearchResponse"
	//   }
	// }

}

// method id "civicinfo.elections.electionQuery":

type ElectionsElectionQueryCall struct {
	s                     *Service
	electionsqueryrequest *ElectionsQueryRequest
	urlParams_            gensupport.URLParams
	ifNoneMatch_          string
	ctx_                  context.Context
	header_               http.Header
}

// ElectionQuery: List of available elections to query.
func (r *ElectionsService) ElectionQuery(electionsqueryrequest *ElectionsQueryRequest) *ElectionsElectionQueryCall {
	c := &ElectionsElectionQueryCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.electionsqueryrequest = electionsqueryrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ElectionsElectionQueryCall) Fields(s ...googleapi.Field) *ElectionsElectionQueryCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ElectionsElectionQueryCall) IfNoneMatch(entityTag string) *ElectionsElectionQueryCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ElectionsElectionQueryCall) Context(ctx context.Context) *ElectionsElectionQueryCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ElectionsElectionQueryCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ElectionsElectionQueryCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "elections")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "civicinfo.elections.electionQuery" call.
// Exactly one of *ElectionsQueryResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ElectionsQueryResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ElectionsElectionQueryCall) Do(opts ...googleapi.CallOption) (*ElectionsQueryResponse, error) {
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
	ret := &ElectionsQueryResponse{
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
	//   "description": "List of available elections to query.",
	//   "httpMethod": "GET",
	//   "id": "civicinfo.elections.electionQuery",
	//   "path": "elections",
	//   "request": {
	//     "$ref": "ElectionsQueryRequest"
	//   },
	//   "response": {
	//     "$ref": "ElectionsQueryResponse"
	//   }
	// }

}

// method id "civicinfo.elections.voterInfoQuery":

type ElectionsVoterInfoQueryCall struct {
	s                *Service
	voterinforequest *VoterInfoRequest
	urlParams_       gensupport.URLParams
	ifNoneMatch_     string
	ctx_             context.Context
	header_          http.Header
}

// VoterInfoQuery: Looks up information relevant to a voter based on the
// voter's registered address.
func (r *ElectionsService) VoterInfoQuery(address string, voterinforequest *VoterInfoRequest) *ElectionsVoterInfoQueryCall {
	c := &ElectionsVoterInfoQueryCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("address", address)
	c.voterinforequest = voterinforequest
	return c
}

// ElectionId sets the optional parameter "electionId": The unique ID of
// the election to look up. A list of election IDs can be obtained at
// https://www.googleapis.com/civicinfo/{version}/elections
func (c *ElectionsVoterInfoQueryCall) ElectionId(electionId int64) *ElectionsVoterInfoQueryCall {
	c.urlParams_.Set("electionId", fmt.Sprint(electionId))
	return c
}

// OfficialOnly sets the optional parameter "officialOnly": If set to
// true, only data from official state sources will be returned.
func (c *ElectionsVoterInfoQueryCall) OfficialOnly(officialOnly bool) *ElectionsVoterInfoQueryCall {
	c.urlParams_.Set("officialOnly", fmt.Sprint(officialOnly))
	return c
}

// ReturnAllAvailableData sets the optional parameter
// "returnAllAvailableData": If set to true, the query will return the
// success codeand include any partial information when it is unable to
// determine a matching address or unable to determine the election for
// electionId=0 queries.
func (c *ElectionsVoterInfoQueryCall) ReturnAllAvailableData(returnAllAvailableData bool) *ElectionsVoterInfoQueryCall {
	c.urlParams_.Set("returnAllAvailableData", fmt.Sprint(returnAllAvailableData))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ElectionsVoterInfoQueryCall) Fields(s ...googleapi.Field) *ElectionsVoterInfoQueryCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ElectionsVoterInfoQueryCall) IfNoneMatch(entityTag string) *ElectionsVoterInfoQueryCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ElectionsVoterInfoQueryCall) Context(ctx context.Context) *ElectionsVoterInfoQueryCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ElectionsVoterInfoQueryCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ElectionsVoterInfoQueryCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "voterinfo")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "civicinfo.elections.voterInfoQuery" call.
// Exactly one of *VoterInfoResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *VoterInfoResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ElectionsVoterInfoQueryCall) Do(opts ...googleapi.CallOption) (*VoterInfoResponse, error) {
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
	ret := &VoterInfoResponse{
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
	//   "description": "Looks up information relevant to a voter based on the voter's registered address.",
	//   "httpMethod": "GET",
	//   "id": "civicinfo.elections.voterInfoQuery",
	//   "parameterOrder": [
	//     "address"
	//   ],
	//   "parameters": {
	//     "address": {
	//       "description": "The registered address of the voter to look up.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "electionId": {
	//       "default": "0",
	//       "description": "The unique ID of the election to look up. A list of election IDs can be obtained at https://www.googleapis.com/civicinfo/{version}/elections",
	//       "format": "int64",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "officialOnly": {
	//       "default": "false",
	//       "description": "If set to true, only data from official state sources will be returned.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "returnAllAvailableData": {
	//       "default": "false",
	//       "description": "If set to true, the query will return the success codeand include any partial information when it is unable to determine a matching address or unable to determine the election for electionId=0 queries.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "voterinfo",
	//   "request": {
	//     "$ref": "VoterInfoRequest"
	//   },
	//   "response": {
	//     "$ref": "VoterInfoResponse"
	//   }
	// }

}

// method id "civicinfo.representatives.representativeInfoByAddress":

type RepresentativesRepresentativeInfoByAddressCall struct {
	s                         *Service
	representativeinforequest *RepresentativeInfoRequest
	urlParams_                gensupport.URLParams
	ifNoneMatch_              string
	ctx_                      context.Context
	header_                   http.Header
}

// RepresentativeInfoByAddress: Looks up political geography and
// representative information for a single address.
func (r *RepresentativesService) RepresentativeInfoByAddress(representativeinforequest *RepresentativeInfoRequest) *RepresentativesRepresentativeInfoByAddressCall {
	c := &RepresentativesRepresentativeInfoByAddressCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.representativeinforequest = representativeinforequest
	return c
}

// Address sets the optional parameter "address": The address to look
// up. May only be specified if the field ocdId is not given in the URL.
func (c *RepresentativesRepresentativeInfoByAddressCall) Address(address string) *RepresentativesRepresentativeInfoByAddressCall {
	c.urlParams_.Set("address", address)
	return c
}

// IncludeOffices sets the optional parameter "includeOffices": Whether
// to return information about offices and officials. If false, only the
// top-level district information will be returned.
func (c *RepresentativesRepresentativeInfoByAddressCall) IncludeOffices(includeOffices bool) *RepresentativesRepresentativeInfoByAddressCall {
	c.urlParams_.Set("includeOffices", fmt.Sprint(includeOffices))
	return c
}

// Levels sets the optional parameter "levels": A list of office levels
// to filter by. Only offices that serve at least one of these levels
// will be returned. Divisions that don't contain a matching office will
// not be returned.
//
// Possible values:
//   "administrativeArea1"
//   "administrativeArea2"
//   "country"
//   "international"
//   "locality"
//   "regional"
//   "special"
//   "subLocality1"
//   "subLocality2"
func (c *RepresentativesRepresentativeInfoByAddressCall) Levels(levels ...string) *RepresentativesRepresentativeInfoByAddressCall {
	c.urlParams_.SetMulti("levels", append([]string{}, levels...))
	return c
}

// Roles sets the optional parameter "roles": A list of office roles to
// filter by. Only offices fulfilling one of these roles will be
// returned. Divisions that don't contain a matching office will not be
// returned.
//
// Possible values:
//   "deputyHeadOfGovernment"
//   "executiveCouncil"
//   "governmentOfficer"
//   "headOfGovernment"
//   "headOfState"
//   "highestCourtJudge"
//   "judge"
//   "legislatorLowerBody"
//   "legislatorUpperBody"
//   "schoolBoard"
//   "specialPurposeOfficer"
func (c *RepresentativesRepresentativeInfoByAddressCall) Roles(roles ...string) *RepresentativesRepresentativeInfoByAddressCall {
	c.urlParams_.SetMulti("roles", append([]string{}, roles...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RepresentativesRepresentativeInfoByAddressCall) Fields(s ...googleapi.Field) *RepresentativesRepresentativeInfoByAddressCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RepresentativesRepresentativeInfoByAddressCall) IfNoneMatch(entityTag string) *RepresentativesRepresentativeInfoByAddressCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RepresentativesRepresentativeInfoByAddressCall) Context(ctx context.Context) *RepresentativesRepresentativeInfoByAddressCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RepresentativesRepresentativeInfoByAddressCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RepresentativesRepresentativeInfoByAddressCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "representatives")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "civicinfo.representatives.representativeInfoByAddress" call.
// Exactly one of *RepresentativeInfoResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *RepresentativeInfoResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RepresentativesRepresentativeInfoByAddressCall) Do(opts ...googleapi.CallOption) (*RepresentativeInfoResponse, error) {
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
	ret := &RepresentativeInfoResponse{
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
	//   "description": "Looks up political geography and representative information for a single address.",
	//   "httpMethod": "GET",
	//   "id": "civicinfo.representatives.representativeInfoByAddress",
	//   "parameters": {
	//     "address": {
	//       "description": "The address to look up. May only be specified if the field ocdId is not given in the URL.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "includeOffices": {
	//       "default": "true",
	//       "description": "Whether to return information about offices and officials. If false, only the top-level district information will be returned.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "levels": {
	//       "description": "A list of office levels to filter by. Only offices that serve at least one of these levels will be returned. Divisions that don't contain a matching office will not be returned.",
	//       "enum": [
	//         "administrativeArea1",
	//         "administrativeArea2",
	//         "country",
	//         "international",
	//         "locality",
	//         "regional",
	//         "special",
	//         "subLocality1",
	//         "subLocality2"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "roles": {
	//       "description": "A list of office roles to filter by. Only offices fulfilling one of these roles will be returned. Divisions that don't contain a matching office will not be returned.",
	//       "enum": [
	//         "deputyHeadOfGovernment",
	//         "executiveCouncil",
	//         "governmentOfficer",
	//         "headOfGovernment",
	//         "headOfState",
	//         "highestCourtJudge",
	//         "judge",
	//         "legislatorLowerBody",
	//         "legislatorUpperBody",
	//         "schoolBoard",
	//         "specialPurposeOfficer"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "representatives",
	//   "request": {
	//     "$ref": "RepresentativeInfoRequest"
	//   },
	//   "response": {
	//     "$ref": "RepresentativeInfoResponse"
	//   }
	// }

}

// method id "civicinfo.representatives.representativeInfoByDivision":

type RepresentativesRepresentativeInfoByDivisionCall struct {
	s                                 *Service
	ocdId                             string
	divisionrepresentativeinforequest *DivisionRepresentativeInfoRequest
	urlParams_                        gensupport.URLParams
	ifNoneMatch_                      string
	ctx_                              context.Context
	header_                           http.Header
}

// RepresentativeInfoByDivision: Looks up representative information for
// a single geographic division.
func (r *RepresentativesService) RepresentativeInfoByDivision(ocdId string, divisionrepresentativeinforequest *DivisionRepresentativeInfoRequest) *RepresentativesRepresentativeInfoByDivisionCall {
	c := &RepresentativesRepresentativeInfoByDivisionCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.ocdId = ocdId
	c.divisionrepresentativeinforequest = divisionrepresentativeinforequest
	return c
}

// Levels sets the optional parameter "levels": A list of office levels
// to filter by. Only offices that serve at least one of these levels
// will be returned. Divisions that don't contain a matching office will
// not be returned.
//
// Possible values:
//   "administrativeArea1"
//   "administrativeArea2"
//   "country"
//   "international"
//   "locality"
//   "regional"
//   "special"
//   "subLocality1"
//   "subLocality2"
func (c *RepresentativesRepresentativeInfoByDivisionCall) Levels(levels ...string) *RepresentativesRepresentativeInfoByDivisionCall {
	c.urlParams_.SetMulti("levels", append([]string{}, levels...))
	return c
}

// Recursive sets the optional parameter "recursive": If true,
// information about all divisions contained in the division requested
// will be included as well. For example, if querying
// ocd-division/country:us/district:dc, this would also return all DC's
// wards and ANCs.
func (c *RepresentativesRepresentativeInfoByDivisionCall) Recursive(recursive bool) *RepresentativesRepresentativeInfoByDivisionCall {
	c.urlParams_.Set("recursive", fmt.Sprint(recursive))
	return c
}

// Roles sets the optional parameter "roles": A list of office roles to
// filter by. Only offices fulfilling one of these roles will be
// returned. Divisions that don't contain a matching office will not be
// returned.
//
// Possible values:
//   "deputyHeadOfGovernment"
//   "executiveCouncil"
//   "governmentOfficer"
//   "headOfGovernment"
//   "headOfState"
//   "highestCourtJudge"
//   "judge"
//   "legislatorLowerBody"
//   "legislatorUpperBody"
//   "schoolBoard"
//   "specialPurposeOfficer"
func (c *RepresentativesRepresentativeInfoByDivisionCall) Roles(roles ...string) *RepresentativesRepresentativeInfoByDivisionCall {
	c.urlParams_.SetMulti("roles", append([]string{}, roles...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RepresentativesRepresentativeInfoByDivisionCall) Fields(s ...googleapi.Field) *RepresentativesRepresentativeInfoByDivisionCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RepresentativesRepresentativeInfoByDivisionCall) IfNoneMatch(entityTag string) *RepresentativesRepresentativeInfoByDivisionCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RepresentativesRepresentativeInfoByDivisionCall) Context(ctx context.Context) *RepresentativesRepresentativeInfoByDivisionCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RepresentativesRepresentativeInfoByDivisionCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RepresentativesRepresentativeInfoByDivisionCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "representatives/{ocdId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"ocdId": c.ocdId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "civicinfo.representatives.representativeInfoByDivision" call.
// Exactly one of *RepresentativeInfoData or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *RepresentativeInfoData.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RepresentativesRepresentativeInfoByDivisionCall) Do(opts ...googleapi.CallOption) (*RepresentativeInfoData, error) {
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
	ret := &RepresentativeInfoData{
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
	//   "description": "Looks up representative information for a single geographic division.",
	//   "httpMethod": "GET",
	//   "id": "civicinfo.representatives.representativeInfoByDivision",
	//   "parameterOrder": [
	//     "ocdId"
	//   ],
	//   "parameters": {
	//     "levels": {
	//       "description": "A list of office levels to filter by. Only offices that serve at least one of these levels will be returned. Divisions that don't contain a matching office will not be returned.",
	//       "enum": [
	//         "administrativeArea1",
	//         "administrativeArea2",
	//         "country",
	//         "international",
	//         "locality",
	//         "regional",
	//         "special",
	//         "subLocality1",
	//         "subLocality2"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "ocdId": {
	//       "description": "The Open Civic Data division identifier of the division to look up.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "recursive": {
	//       "description": "If true, information about all divisions contained in the division requested will be included as well. For example, if querying ocd-division/country:us/district:dc, this would also return all DC's wards and ANCs.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "roles": {
	//       "description": "A list of office roles to filter by. Only offices fulfilling one of these roles will be returned. Divisions that don't contain a matching office will not be returned.",
	//       "enum": [
	//         "deputyHeadOfGovernment",
	//         "executiveCouncil",
	//         "governmentOfficer",
	//         "headOfGovernment",
	//         "headOfState",
	//         "highestCourtJudge",
	//         "judge",
	//         "legislatorLowerBody",
	//         "legislatorUpperBody",
	//         "schoolBoard",
	//         "specialPurposeOfficer"
	//       ],
	//       "enumDescriptions": [
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         "",
	//         ""
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "representatives/{ocdId}",
	//   "request": {
	//     "$ref": "DivisionRepresentativeInfoRequest"
	//   },
	//   "response": {
	//     "$ref": "RepresentativeInfoData"
	//   }
	// }

}

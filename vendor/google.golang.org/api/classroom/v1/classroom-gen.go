// Package classroom provides access to the Google Classroom API.
//
// See https://developers.google.com/classroom/
//
// Usage example:
//
//   import "google.golang.org/api/classroom/v1"
//   ...
//   classroomService, err := classroom.New(oauthHttpClient)
package classroom // import "google.golang.org/api/classroom/v1"

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

const apiId = "classroom:v1"
const apiName = "classroom"
const apiVersion = "v1"
const basePath = "https://classroom.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View instructions for teacher-assigned work in your Google Classroom
	// classes
	ClassroomCourseWorkReadonlyScope = "https://www.googleapis.com/auth/classroom.course-work.readonly"

	// Manage your Google Classroom classes
	ClassroomCoursesScope = "https://www.googleapis.com/auth/classroom.courses"

	// View your Google Classroom classes
	ClassroomCoursesReadonlyScope = "https://www.googleapis.com/auth/classroom.courses.readonly"

	// Manage your course work and view your grades in Google Classroom
	ClassroomCourseworkMeScope = "https://www.googleapis.com/auth/classroom.coursework.me"

	// View your course work and grades in Google Classroom
	ClassroomCourseworkMeReadonlyScope = "https://www.googleapis.com/auth/classroom.coursework.me.readonly"

	// Manage course work and grades for students in the Google Classroom
	// classes you teach and view the course work and grades for classes you
	// administer
	ClassroomCourseworkStudentsScope = "https://www.googleapis.com/auth/classroom.coursework.students"

	// View course work and grades for students in the Google Classroom
	// classes you teach or administer
	ClassroomCourseworkStudentsReadonlyScope = "https://www.googleapis.com/auth/classroom.coursework.students.readonly"

	// View the email addresses of people in your classes
	ClassroomProfileEmailsScope = "https://www.googleapis.com/auth/classroom.profile.emails"

	// View the profile photos of people in your classes
	ClassroomProfilePhotosScope = "https://www.googleapis.com/auth/classroom.profile.photos"

	// Manage your Google Classroom class rosters
	ClassroomRostersScope = "https://www.googleapis.com/auth/classroom.rosters"

	// View your Google Classroom class rosters
	ClassroomRostersReadonlyScope = "https://www.googleapis.com/auth/classroom.rosters.readonly"

	// View your course work and grades in Google Classroom
	ClassroomStudentSubmissionsMeReadonlyScope = "https://www.googleapis.com/auth/classroom.student-submissions.me.readonly"

	// View course work and grades for students in the Google Classroom
	// classes you teach or administer
	ClassroomStudentSubmissionsStudentsReadonlyScope = "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Courses = NewCoursesService(s)
	s.Invitations = NewInvitationsService(s)
	s.UserProfiles = NewUserProfilesService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Courses *CoursesService

	Invitations *InvitationsService

	UserProfiles *UserProfilesService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewCoursesService(s *Service) *CoursesService {
	rs := &CoursesService{s: s}
	rs.Aliases = NewCoursesAliasesService(s)
	rs.CourseWork = NewCoursesCourseWorkService(s)
	rs.Students = NewCoursesStudentsService(s)
	rs.Teachers = NewCoursesTeachersService(s)
	return rs
}

type CoursesService struct {
	s *Service

	Aliases *CoursesAliasesService

	CourseWork *CoursesCourseWorkService

	Students *CoursesStudentsService

	Teachers *CoursesTeachersService
}

func NewCoursesAliasesService(s *Service) *CoursesAliasesService {
	rs := &CoursesAliasesService{s: s}
	return rs
}

type CoursesAliasesService struct {
	s *Service
}

func NewCoursesCourseWorkService(s *Service) *CoursesCourseWorkService {
	rs := &CoursesCourseWorkService{s: s}
	rs.StudentSubmissions = NewCoursesCourseWorkStudentSubmissionsService(s)
	return rs
}

type CoursesCourseWorkService struct {
	s *Service

	StudentSubmissions *CoursesCourseWorkStudentSubmissionsService
}

func NewCoursesCourseWorkStudentSubmissionsService(s *Service) *CoursesCourseWorkStudentSubmissionsService {
	rs := &CoursesCourseWorkStudentSubmissionsService{s: s}
	return rs
}

type CoursesCourseWorkStudentSubmissionsService struct {
	s *Service
}

func NewCoursesStudentsService(s *Service) *CoursesStudentsService {
	rs := &CoursesStudentsService{s: s}
	return rs
}

type CoursesStudentsService struct {
	s *Service
}

func NewCoursesTeachersService(s *Service) *CoursesTeachersService {
	rs := &CoursesTeachersService{s: s}
	return rs
}

type CoursesTeachersService struct {
	s *Service
}

func NewInvitationsService(s *Service) *InvitationsService {
	rs := &InvitationsService{s: s}
	return rs
}

type InvitationsService struct {
	s *Service
}

func NewUserProfilesService(s *Service) *UserProfilesService {
	rs := &UserProfilesService{s: s}
	rs.GuardianInvitations = NewUserProfilesGuardianInvitationsService(s)
	rs.Guardians = NewUserProfilesGuardiansService(s)
	return rs
}

type UserProfilesService struct {
	s *Service

	GuardianInvitations *UserProfilesGuardianInvitationsService

	Guardians *UserProfilesGuardiansService
}

func NewUserProfilesGuardianInvitationsService(s *Service) *UserProfilesGuardianInvitationsService {
	rs := &UserProfilesGuardianInvitationsService{s: s}
	return rs
}

type UserProfilesGuardianInvitationsService struct {
	s *Service
}

func NewUserProfilesGuardiansService(s *Service) *UserProfilesGuardiansService {
	rs := &UserProfilesGuardiansService{s: s}
	return rs
}

type UserProfilesGuardiansService struct {
	s *Service
}

// Assignment: Additional details for assignments.
type Assignment struct {
	// StudentWorkFolder: Drive folder where attachments from student
	// submissions are placed. This is only populated for course teachers.
	StudentWorkFolder *DriveFolder `json:"studentWorkFolder,omitempty"`

	// ForceSendFields is a list of field names (e.g. "StudentWorkFolder")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "StudentWorkFolder") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Assignment) MarshalJSON() ([]byte, error) {
	type noMethod Assignment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AssignmentSubmission: Student work for an assignment.
type AssignmentSubmission struct {
	// Attachments: Attachments added by the student. Drive files that
	// correspond to materials with a share mode of SUBMISSION_COPY may not
	// exist yet if the student has not accessed the assignment in
	// Classroom. Some attachment metadata is only populated if the
	// requesting user has permission to access it. Identifier and
	// alternate_link fields are available, but others (e.g. title) may not
	// be.
	Attachments []*Attachment `json:"attachments,omitempty"`

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

func (s *AssignmentSubmission) MarshalJSON() ([]byte, error) {
	type noMethod AssignmentSubmission
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Attachment: Attachment added to student assignment work. When
// creating attachments, only the Link field may be specified.
type Attachment struct {
	// DriveFile: Google Drive file attachment.
	DriveFile *DriveFile `json:"driveFile,omitempty"`

	// Form: Google Forms attachment.
	Form *Form `json:"form,omitempty"`

	// Link: Link attachment.
	Link *Link `json:"link,omitempty"`

	// YouTubeVideo: Youtube video attachment.
	YouTubeVideo *YouTubeVideo `json:"youTubeVideo,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DriveFile") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DriveFile") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Attachment) MarshalJSON() ([]byte, error) {
	type noMethod Attachment
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Course: A Course in Classroom.
type Course struct {
	// AlternateLink: Absolute link to this course in the Classroom web UI.
	// Read-only.
	AlternateLink string `json:"alternateLink,omitempty"`

	// CourseGroupEmail: The email address of a Google group containing all
	// members of the course. This group does not accept email and can only
	// be used for permissions. Read-only.
	CourseGroupEmail string `json:"courseGroupEmail,omitempty"`

	// CourseMaterialSets: Sets of materials that appear on the "about" page
	// of this course. Read-only.
	CourseMaterialSets []*CourseMaterialSet `json:"courseMaterialSets,omitempty"`

	// CourseState: State of the course. If unspecified, the default state
	// is `PROVISIONED`.
	//
	// Possible values:
	//   "COURSE_STATE_UNSPECIFIED"
	//   "ACTIVE"
	//   "ARCHIVED"
	//   "PROVISIONED"
	//   "DECLINED"
	CourseState string `json:"courseState,omitempty"`

	// CreationTime: Creation time of the course. Specifying this field in a
	// course update mask results in an error. Read-only.
	CreationTime string `json:"creationTime,omitempty"`

	// Description: Optional description. For example, "We'll be learning
	// about the structure of living creatures from a combination of
	// textbooks, guest lectures, and lab work. Expect to be excited!" If
	// set, this field must be a valid UTF-8 string and no longer than
	// 30,000 characters.
	Description string `json:"description,omitempty"`

	// DescriptionHeading: Optional heading for the description. For
	// example, "Welcome to 10th Grade Biology." If set, this field must be
	// a valid UTF-8 string and no longer than 3600 characters.
	DescriptionHeading string `json:"descriptionHeading,omitempty"`

	// EnrollmentCode: Enrollment code to use when joining this course.
	// Specifying this field in a course update mask results in an error.
	// Read-only.
	EnrollmentCode string `json:"enrollmentCode,omitempty"`

	// GuardiansEnabled: Whether or not guardian notifications are enabled
	// for this course. Read-only.
	GuardiansEnabled bool `json:"guardiansEnabled,omitempty"`

	// Id: Identifier for this course assigned by Classroom. When creating a
	// course, you may optionally set this identifier to an alias string in
	// the request to create a corresponding alias. The `id` is still
	// assigned by Classroom and cannot be updated after the course is
	// created. Specifying this field in a course update mask results in an
	// error.
	Id string `json:"id,omitempty"`

	// Name: Name of the course. For example, "10th Grade Biology". The name
	// is required. It must be between 1 and 750 characters and a valid
	// UTF-8 string.
	Name string `json:"name,omitempty"`

	// OwnerId: The identifier of the owner of a course. When specified as a
	// parameter of a create course request, this field is required. The
	// identifier can be one of the following: * the numeric identifier for
	// the user * the email address of the user * the string literal "me",
	// indicating the requesting user This must be set in a create request.
	// Specifying this field in a course update mask results in an
	// `INVALID_ARGUMENT` error.
	OwnerId string `json:"ownerId,omitempty"`

	// Room: Optional room location. For example, "301". If set, this field
	// must be a valid UTF-8 string and no longer than 650 characters.
	Room string `json:"room,omitempty"`

	// Section: Section of the course. For example, "Period 2". If set, this
	// field must be a valid UTF-8 string and no longer than 2800
	// characters.
	Section string `json:"section,omitempty"`

	// TeacherFolder: Information about a Drive Folder that is shared with
	// all teachers of the course. This field will only be set for teachers
	// of the course and domain administrators. Read-only.
	TeacherFolder *DriveFolder `json:"teacherFolder,omitempty"`

	// TeacherGroupEmail: The email address of a Google group containing all
	// teachers of the course. This group does not accept email and can only
	// be used for permissions. Read-only.
	TeacherGroupEmail string `json:"teacherGroupEmail,omitempty"`

	// UpdateTime: Time of the most recent update to this course. Specifying
	// this field in a course update mask results in an error. Read-only.
	UpdateTime string `json:"updateTime,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AlternateLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Course) MarshalJSON() ([]byte, error) {
	type noMethod Course
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CourseAlias: Alternative identifier for a course. An alias uniquely
// identifies a course. It must be unique within one of the following
// scopes: * domain: A domain-scoped alias is visible to all users
// within the alias creator's domain and can be created only by a domain
// admin. A domain-scoped alias is often used when a course has an
// identifier external to Classroom. * project: A project-scoped alias
// is visible to any request from an application using the Developer
// Console project ID that created the alias and can be created by any
// project. A project-scoped alias is often used when an application has
// alternative identifiers. A random value can also be used to avoid
// duplicate courses in the event of transmission failures, as retrying
// a request will return `ALREADY_EXISTS` if a previous one has
// succeeded.
type CourseAlias struct {
	// Alias: Alias string. The format of the string indicates the desired
	// alias scoping. * `d:` indicates a domain-scoped alias. Example:
	// `d:math_101` * `p:` indicates a project-scoped alias. Example:
	// `p:abc123` This field has a maximum length of 256 characters.
	Alias string `json:"alias,omitempty"`

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

func (s *CourseAlias) MarshalJSON() ([]byte, error) {
	type noMethod CourseAlias
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CourseMaterial: A material attached to a course as part of a material
// set.
type CourseMaterial struct {
	// DriveFile: Google Drive file attachment.
	DriveFile *DriveFile `json:"driveFile,omitempty"`

	// Form: Google Forms attachment.
	Form *Form `json:"form,omitempty"`

	// Link: Link atatchment.
	Link *Link `json:"link,omitempty"`

	// YouTubeVideo: Youtube video attachment.
	YouTubeVideo *YouTubeVideo `json:"youTubeVideo,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DriveFile") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DriveFile") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CourseMaterial) MarshalJSON() ([]byte, error) {
	type noMethod CourseMaterial
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CourseMaterialSet: A set of materials that appears on the "About"
// page of the course. These materials might include a syllabus,
// schedule, or other background information relating to the course as a
// whole.
type CourseMaterialSet struct {
	// Materials: Materials attached to this set.
	Materials []*CourseMaterial `json:"materials,omitempty"`

	// Title: Title for this set.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Materials") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Materials") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CourseMaterialSet) MarshalJSON() ([]byte, error) {
	type noMethod CourseMaterialSet
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CourseWork: Course work created by a teacher for students of the
// course.
type CourseWork struct {
	// AlternateLink: Absolute link to this course work in the Classroom web
	// UI. This is only populated if `state` is `PUBLISHED`. Read-only.
	AlternateLink string `json:"alternateLink,omitempty"`

	// Assignment: Assignment details. This is populated only when
	// `work_type` is `ASSIGNMENT`.
	Assignment *Assignment `json:"assignment,omitempty"`

	// AssociatedWithDeveloper: Whether this course work item is associated
	// with the Developer Console project making the request. See
	// google.classroom.Work.CreateCourseWork for more details. Read-only.
	AssociatedWithDeveloper bool `json:"associatedWithDeveloper,omitempty"`

	// CourseId: Identifier of the course. Read-only.
	CourseId string `json:"courseId,omitempty"`

	// CreationTime: Timestamp when this course work was created. Read-only.
	CreationTime string `json:"creationTime,omitempty"`

	// Description: Optional description of this course work. If set, the
	// description must be a valid UTF-8 string containing no more than
	// 30,000 characters.
	Description string `json:"description,omitempty"`

	// DueDate: Optional date, in UTC, that submissions for this this course
	// work are due. This must be specified if `due_time` is specified.
	DueDate *Date `json:"dueDate,omitempty"`

	// DueTime: Optional time of day, in UTC, that submissions for this this
	// course work are due. This must be specified if `due_date` is
	// specified.
	DueTime *TimeOfDay `json:"dueTime,omitempty"`

	// Id: Classroom-assigned identifier of this course work, unique per
	// course. Read-only.
	Id string `json:"id,omitempty"`

	// Materials: Additional materials. CourseWork must have no more than 20
	// material items.
	Materials []*Material `json:"materials,omitempty"`

	// MaxPoints: Maximum grade for this course work. If zero or
	// unspecified, this assignment is considered ungraded. This must be a
	// non-negative integer value.
	MaxPoints float64 `json:"maxPoints,omitempty"`

	// MultipleChoiceQuestion: Multiple choice question details. This is
	// populated only when `work_type` is `MULTIPLE_CHOICE_QUESTION`.
	MultipleChoiceQuestion *MultipleChoiceQuestion `json:"multipleChoiceQuestion,omitempty"`

	// State: Status of this course work. If unspecified, the default state
	// is `DRAFT`.
	//
	// Possible values:
	//   "COURSE_WORK_STATE_UNSPECIFIED"
	//   "PUBLISHED"
	//   "DRAFT"
	//   "DELETED"
	State string `json:"state,omitempty"`

	// SubmissionModificationMode: Setting to determine when students are
	// allowed to modify submissions. If unspecified, the default value is
	// `MODIFIABLE_UNTIL_TURNED_IN`.
	//
	// Possible values:
	//   "SUBMISSION_MODIFICATION_MODE_UNSPECIFIED"
	//   "MODIFIABLE_UNTIL_TURNED_IN"
	//   "MODIFIABLE"
	SubmissionModificationMode string `json:"submissionModificationMode,omitempty"`

	// Title: Title of this course work. The title must be a valid UTF-8
	// string containing between 1 and 3000 characters.
	Title string `json:"title,omitempty"`

	// UpdateTime: Timestamp of the most recent change to this course work.
	// Read-only.
	UpdateTime string `json:"updateTime,omitempty"`

	// WorkType: Type of this course work. The type is set when the course
	// work is created and cannot be changed. When creating course work,
	// this must be `ASSIGNMENT`.
	//
	// Possible values:
	//   "COURSE_WORK_TYPE_UNSPECIFIED"
	//   "ASSIGNMENT"
	//   "SHORT_ANSWER_QUESTION"
	//   "MULTIPLE_CHOICE_QUESTION"
	WorkType string `json:"workType,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AlternateLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CourseWork) MarshalJSON() ([]byte, error) {
	type noMethod CourseWork
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

// DriveFile: Representation of a Google Drive file.
type DriveFile struct {
	// AlternateLink: URL that can be used to access the Drive item.
	// Read-only.
	AlternateLink string `json:"alternateLink,omitempty"`

	// Id: Drive API resource ID.
	Id string `json:"id,omitempty"`

	// ThumbnailUrl: URL of a thumbnail image of the Drive item. Read-only.
	ThumbnailUrl string `json:"thumbnailUrl,omitempty"`

	// Title: Title of the Drive item. Read-only.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AlternateLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DriveFile) MarshalJSON() ([]byte, error) {
	type noMethod DriveFile
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DriveFolder: Representation of a Google Drive folder.
type DriveFolder struct {
	// AlternateLink: URL that can be used to access the Drive folder.
	// Read-only.
	AlternateLink string `json:"alternateLink,omitempty"`

	// Id: Drive API resource ID.
	Id string `json:"id,omitempty"`

	// Title: Title of the Drive folder. Read-only.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AlternateLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DriveFolder) MarshalJSON() ([]byte, error) {
	type noMethod DriveFolder
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

// Form: Google Forms item.
type Form struct {
	// FormUrl: URL of the form.
	FormUrl string `json:"formUrl,omitempty"`

	// ResponseUrl: URL of the form responses document. Only set if
	// respsonses have been recorded and only when the requesting user is an
	// editor of the form. Read-only.
	ResponseUrl string `json:"responseUrl,omitempty"`

	// ThumbnailUrl: URL of a thumbnail image of the Form. Read-only.
	ThumbnailUrl string `json:"thumbnailUrl,omitempty"`

	// Title: Title of the Form. Read-only.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FormUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FormUrl") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Form) MarshalJSON() ([]byte, error) {
	type noMethod Form
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GlobalPermission: Global user permission description.
type GlobalPermission struct {
	// Permission: Permission value.
	//
	// Possible values:
	//   "PERMISSION_UNSPECIFIED"
	//   "CREATE_COURSE"
	Permission string `json:"permission,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Permission") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Permission") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GlobalPermission) MarshalJSON() ([]byte, error) {
	type noMethod GlobalPermission
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Guardian: Association between a student and a guardian of that
// student. The guardian may receive information about the student's
// course work.
type Guardian struct {
	// GuardianId: Identifier for the guardian.
	GuardianId string `json:"guardianId,omitempty"`

	// GuardianProfile: User profile for the guardian.
	GuardianProfile *UserProfile `json:"guardianProfile,omitempty"`

	// InvitedEmailAddress: The email address to which the initial guardian
	// invitation was sent. This field is only visible to domain
	// administrators.
	InvitedEmailAddress string `json:"invitedEmailAddress,omitempty"`

	// StudentId: Identifier for the student to whom the guardian
	// relationship applies.
	StudentId string `json:"studentId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "GuardianId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GuardianId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Guardian) MarshalJSON() ([]byte, error) {
	type noMethod Guardian
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GuardianInvitation: An invitation to become the guardian of a
// specified user, sent to a specified email address.
type GuardianInvitation struct {
	// CreationTime: The time that this invitation was created. Read-only.
	CreationTime string `json:"creationTime,omitempty"`

	// InvitationId: Unique identifier for this invitation. Read-only.
	InvitationId string `json:"invitationId,omitempty"`

	// InvitedEmailAddress: Email address that the invitation was sent to.
	// This field is only visible to domain administrators.
	InvitedEmailAddress string `json:"invitedEmailAddress,omitempty"`

	// State: The state that this invitation is in.
	//
	// Possible values:
	//   "GUARDIAN_INVITATION_STATE_UNSPECIFIED"
	//   "PENDING"
	//   "COMPLETE"
	State string `json:"state,omitempty"`

	// StudentId: ID of the student (in standard format)
	StudentId string `json:"studentId,omitempty"`

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

func (s *GuardianInvitation) MarshalJSON() ([]byte, error) {
	type noMethod GuardianInvitation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Invitation: An invitation to join a course.
type Invitation struct {
	// CourseId: Identifier of the course to invite the user to.
	CourseId string `json:"courseId,omitempty"`

	// Id: Identifier assigned by Classroom. Read-only.
	Id string `json:"id,omitempty"`

	// Role: Role to invite the user to have. Must not be
	// `COURSE_ROLE_UNSPECIFIED`.
	//
	// Possible values:
	//   "COURSE_ROLE_UNSPECIFIED"
	//   "STUDENT"
	//   "TEACHER"
	Role string `json:"role,omitempty"`

	// UserId: Identifier of the invited user. When specified as a parameter
	// of a request, this identifier can be set to one of the following: *
	// the numeric identifier for the user * the email address of the user *
	// the string literal "me", indicating the requesting user
	UserId string `json:"userId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CourseId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CourseId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Invitation) MarshalJSON() ([]byte, error) {
	type noMethod Invitation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Link: URL item.
type Link struct {
	// ThumbnailUrl: URL of a thumbnail image of the target URL. Read-only.
	ThumbnailUrl string `json:"thumbnailUrl,omitempty"`

	// Title: Title of the target of the URL. Read-only.
	Title string `json:"title,omitempty"`

	// Url: URL to link to. This must be a valid UTF-8 string containing
	// between 1 and 2024 characters.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ThumbnailUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ThumbnailUrl") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Link) MarshalJSON() ([]byte, error) {
	type noMethod Link
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListCourseAliasesResponse: Response when listing course aliases.
type ListCourseAliasesResponse struct {
	// Aliases: The course aliases.
	Aliases []*CourseAlias `json:"aliases,omitempty"`

	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *ListCourseAliasesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListCourseAliasesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListCourseWorkResponse: Response when listing course work.
type ListCourseWorkResponse struct {
	// CourseWork: Course work items that match the request.
	CourseWork []*CourseWork `json:"courseWork,omitempty"`

	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CourseWork") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CourseWork") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListCourseWorkResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListCourseWorkResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListCoursesResponse: Response when listing courses.
type ListCoursesResponse struct {
	// Courses: Courses that match the list request.
	Courses []*Course `json:"courses,omitempty"`

	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Courses") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Courses") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListCoursesResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListCoursesResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListGuardianInvitationsResponse: Response when listing guardian
// invitations.
type ListGuardianInvitationsResponse struct {
	// GuardianInvitations: Guardian invitations that matched the list
	// request.
	GuardianInvitations []*GuardianInvitation `json:"guardianInvitations,omitempty"`

	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "GuardianInvitations")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GuardianInvitations") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ListGuardianInvitationsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListGuardianInvitationsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListGuardiansResponse: Response when listing guardians.
type ListGuardiansResponse struct {
	// Guardians: Guardians on this page of results that met the criteria
	// specified in the request.
	Guardians []*Guardian `json:"guardians,omitempty"`

	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Guardians") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Guardians") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListGuardiansResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListGuardiansResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListInvitationsResponse: Response when listing invitations.
type ListInvitationsResponse struct {
	// Invitations: Invitations that match the list request.
	Invitations []*Invitation `json:"invitations,omitempty"`

	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Invitations") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Invitations") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListInvitationsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListInvitationsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListStudentSubmissionsResponse: Response when listing student
// submissions.
type ListStudentSubmissionsResponse struct {
	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// StudentSubmissions: Student work that matches the request.
	StudentSubmissions []*StudentSubmission `json:"studentSubmissions,omitempty"`

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

func (s *ListStudentSubmissionsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListStudentSubmissionsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListStudentsResponse: Response when listing students.
type ListStudentsResponse struct {
	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Students: Students who match the list request.
	Students []*Student `json:"students,omitempty"`

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

func (s *ListStudentsResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListStudentsResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListTeachersResponse: Response when listing teachers.
type ListTeachersResponse struct {
	// NextPageToken: Token identifying the next page of results to return.
	// If empty, no further results are available.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Teachers: Teachers who match the list request.
	Teachers []*Teacher `json:"teachers,omitempty"`

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

func (s *ListTeachersResponse) MarshalJSON() ([]byte, error) {
	type noMethod ListTeachersResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Material: Material attached to course work. When creating
// attachments, only the Link field may be specified.
type Material struct {
	// DriveFile: Google Drive file material.
	DriveFile *SharedDriveFile `json:"driveFile,omitempty"`

	// Form: Google Forms material.
	Form *Form `json:"form,omitempty"`

	// Link: Link material.
	Link *Link `json:"link,omitempty"`

	// YoutubeVideo: YouTube video material.
	YoutubeVideo *YouTubeVideo `json:"youtubeVideo,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DriveFile") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DriveFile") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Material) MarshalJSON() ([]byte, error) {
	type noMethod Material
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ModifyAttachmentsRequest: Request to modify the attachments of a
// student submission.
type ModifyAttachmentsRequest struct {
	// AddAttachments: Attachments to add. A student submission may not have
	// more than 20 attachments. This may only contain link attachments.
	AddAttachments []*Attachment `json:"addAttachments,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AddAttachments") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AddAttachments") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ModifyAttachmentsRequest) MarshalJSON() ([]byte, error) {
	type noMethod ModifyAttachmentsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MultipleChoiceQuestion: Additional details for multiple-choice
// questions.
type MultipleChoiceQuestion struct {
	// Choices: Possible choices.
	Choices []string `json:"choices,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Choices") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Choices") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MultipleChoiceQuestion) MarshalJSON() ([]byte, error) {
	type noMethod MultipleChoiceQuestion
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// MultipleChoiceSubmission: Student work for a multiple-choice
// question.
type MultipleChoiceSubmission struct {
	// Answer: Student's select choice.
	Answer string `json:"answer,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Answer") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Answer") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MultipleChoiceSubmission) MarshalJSON() ([]byte, error) {
	type noMethod MultipleChoiceSubmission
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Name: Details of the user's name.
type Name struct {
	// FamilyName: The user's last name. Read-only.
	FamilyName string `json:"familyName,omitempty"`

	// FullName: The user's full name formed by concatenating the first and
	// last name values. Read-only.
	FullName string `json:"fullName,omitempty"`

	// GivenName: The user's first name. Read-only.
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

func (s *Name) MarshalJSON() ([]byte, error) {
	type noMethod Name
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReclaimStudentSubmissionRequest: Request to reclaim a student
// submission.
type ReclaimStudentSubmissionRequest struct {
}

// ReturnStudentSubmissionRequest: Request to return a student
// submission.
type ReturnStudentSubmissionRequest struct {
}

// SharedDriveFile: Drive file that is used as material for course work.
type SharedDriveFile struct {
	// DriveFile: Drive file details.
	DriveFile *DriveFile `json:"driveFile,omitempty"`

	// ShareMode: Mechanism by which students access the Drive item.
	//
	// Possible values:
	//   "UNKNOWN_SHARE_MODE"
	//   "VIEW"
	//   "EDIT"
	//   "STUDENT_COPY"
	ShareMode string `json:"shareMode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DriveFile") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DriveFile") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SharedDriveFile) MarshalJSON() ([]byte, error) {
	type noMethod SharedDriveFile
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ShortAnswerSubmission: Student work for a short answer question.
type ShortAnswerSubmission struct {
	// Answer: Student response to a short-answer question.
	Answer string `json:"answer,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Answer") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Answer") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ShortAnswerSubmission) MarshalJSON() ([]byte, error) {
	type noMethod ShortAnswerSubmission
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Student: Student in a course.
type Student struct {
	// CourseId: Identifier of the course. Read-only.
	CourseId string `json:"courseId,omitempty"`

	// Profile: Global user information for the student. Read-only.
	Profile *UserProfile `json:"profile,omitempty"`

	// StudentWorkFolder: Information about a Drive Folder for this
	// student's work in this course. Only visible to the student and domain
	// administrators. Read-only.
	StudentWorkFolder *DriveFolder `json:"studentWorkFolder,omitempty"`

	// UserId: Identifier of the user. When specified as a parameter of a
	// request, this identifier can be one of the following: * the numeric
	// identifier for the user * the email address of the user * the string
	// literal "me", indicating the requesting user
	UserId string `json:"userId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CourseId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CourseId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Student) MarshalJSON() ([]byte, error) {
	type noMethod Student
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StudentSubmission: Student submission for course work.
// StudentSubmission items are generated when a CourseWork item is
// created. StudentSubmissions that have never been accessed (i.e. with
// `state` = NEW) may not have a creation time or update time.
type StudentSubmission struct {
	// AlternateLink: Absolute link to the submission in the Classroom web
	// UI. Read-only.
	AlternateLink string `json:"alternateLink,omitempty"`

	// AssignedGrade: Optional grade. If unset, no grade was set. This must
	// be a non-negative integer value. This may be modified only by course
	// teachers.
	AssignedGrade float64 `json:"assignedGrade,omitempty"`

	// AssignmentSubmission: Submission content when course_work_type is
	// ASSIGNMENT .
	AssignmentSubmission *AssignmentSubmission `json:"assignmentSubmission,omitempty"`

	// AssociatedWithDeveloper: Whether this student submission is
	// associated with the Developer Console project making the request. See
	// google.classroom.Work.CreateCourseWork for more details. Read-only.
	AssociatedWithDeveloper bool `json:"associatedWithDeveloper,omitempty"`

	// CourseId: Identifier of the course. Read-only.
	CourseId string `json:"courseId,omitempty"`

	// CourseWorkId: Identifier for the course work this corresponds to.
	// Read-only.
	CourseWorkId string `json:"courseWorkId,omitempty"`

	// CourseWorkType: Type of course work this submission is for.
	// Read-only.
	//
	// Possible values:
	//   "COURSE_WORK_TYPE_UNSPECIFIED"
	//   "ASSIGNMENT"
	//   "SHORT_ANSWER_QUESTION"
	//   "MULTIPLE_CHOICE_QUESTION"
	CourseWorkType string `json:"courseWorkType,omitempty"`

	// CreationTime: Creation time of this submission. This may be unset if
	// the student has not accessed this item. Read-only.
	CreationTime string `json:"creationTime,omitempty"`

	// DraftGrade: Optional pending grade. If unset, no grade was set. This
	// must be a non-negative integer value. This is only visible to and
	// modifiable by course teachers.
	DraftGrade float64 `json:"draftGrade,omitempty"`

	// Id: Classroom-assigned Identifier for the student submission. This is
	// unique among submissions for the relevant course work. Read-only.
	Id string `json:"id,omitempty"`

	// Late: Whether this submission is late. Read-only.
	Late bool `json:"late,omitempty"`

	// MultipleChoiceSubmission: Submission content when course_work_type is
	// MULTIPLE_CHOICE_QUESTION.
	MultipleChoiceSubmission *MultipleChoiceSubmission `json:"multipleChoiceSubmission,omitempty"`

	// ShortAnswerSubmission: Submission content when course_work_type is
	// SHORT_ANSWER_QUESTION.
	ShortAnswerSubmission *ShortAnswerSubmission `json:"shortAnswerSubmission,omitempty"`

	// State: State of this submission. Read-only.
	//
	// Possible values:
	//   "SUBMISSION_STATE_UNSPECIFIED"
	//   "NEW"
	//   "CREATED"
	//   "TURNED_IN"
	//   "RETURNED"
	//   "RECLAIMED_BY_STUDENT"
	State string `json:"state,omitempty"`

	// UpdateTime: Last update time of this submission. This may be unset if
	// the student has not accessed this item. Read-only.
	UpdateTime string `json:"updateTime,omitempty"`

	// UserId: Identifier for the student that owns this submission.
	// Read-only.
	UserId string `json:"userId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AlternateLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StudentSubmission) MarshalJSON() ([]byte, error) {
	type noMethod StudentSubmission
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Teacher: Teacher of a course.
type Teacher struct {
	// CourseId: Identifier of the course. Read-only.
	CourseId string `json:"courseId,omitempty"`

	// Profile: Global user information for the teacher. Read-only.
	Profile *UserProfile `json:"profile,omitempty"`

	// UserId: Identifier of the user. When specified as a parameter of a
	// request, this identifier can be one of the following: * the numeric
	// identifier for the user * the email address of the user * the string
	// literal "me", indicating the requesting user
	UserId string `json:"userId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "CourseId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CourseId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Teacher) MarshalJSON() ([]byte, error) {
	type noMethod Teacher
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TimeOfDay: Represents a time of day. The date and time zone are
// either not significant or are specified elsewhere. An API may chose
// to allow leap seconds. Related types are google.type.Date and
// `google.protobuf.Timestamp`.
type TimeOfDay struct {
	// Hours: Hours of day in 24 hour format. Should be from 0 to 23. An API
	// may choose to allow the value "24:00:00" for scenarios like business
	// closing time.
	Hours int64 `json:"hours,omitempty"`

	// Minutes: Minutes of hour of day. Must be from 0 to 59.
	Minutes int64 `json:"minutes,omitempty"`

	// Nanos: Fractions of seconds in nanoseconds. Must be from 0 to
	// 999,999,999.
	Nanos int64 `json:"nanos,omitempty"`

	// Seconds: Seconds of minutes of the time. Must normally be from 0 to
	// 59. An API may allow the value 60 if it allows leap-seconds.
	Seconds int64 `json:"seconds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Hours") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Hours") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TimeOfDay) MarshalJSON() ([]byte, error) {
	type noMethod TimeOfDay
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TurnInStudentSubmissionRequest: Request to turn in a student
// submission.
type TurnInStudentSubmissionRequest struct {
}

// UserProfile: Global information for a user.
type UserProfile struct {
	// EmailAddress: Email address of the user. Read-only.
	EmailAddress string `json:"emailAddress,omitempty"`

	// Id: Identifier of the user. Read-only.
	Id string `json:"id,omitempty"`

	// Name: Name of the user. Read-only.
	Name *Name `json:"name,omitempty"`

	// Permissions: Global permissions of the user. Read-only.
	Permissions []*GlobalPermission `json:"permissions,omitempty"`

	// PhotoUrl: URL of user's profile photo. Read-only.
	PhotoUrl string `json:"photoUrl,omitempty"`

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

func (s *UserProfile) MarshalJSON() ([]byte, error) {
	type noMethod UserProfile
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// YouTubeVideo: YouTube video item.
type YouTubeVideo struct {
	// AlternateLink: URL that can be used to view the YouTube video.
	// Read-only.
	AlternateLink string `json:"alternateLink,omitempty"`

	// Id: YouTube API resource ID.
	Id string `json:"id,omitempty"`

	// ThumbnailUrl: URL of a thumbnail image of the YouTube video.
	// Read-only.
	ThumbnailUrl string `json:"thumbnailUrl,omitempty"`

	// Title: Title of the YouTube video. Read-only.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AlternateLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternateLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *YouTubeVideo) MarshalJSON() ([]byte, error) {
	type noMethod YouTubeVideo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "classroom.courses.create":

type CoursesCreateCall struct {
	s          *Service
	course     *Course
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a course. The user specified in `ownerId` is the
// owner of the created course and added as a teacher. This method
// returns the following error codes: * `PERMISSION_DENIED` if the
// requesting user is not permitted to create courses or for access
// errors. * `NOT_FOUND` if the primary teacher is not a valid user. *
// `FAILED_PRECONDITION` if the course owner's account is disabled or
// for the following request errors: * UserGroupsMembershipLimitReached
// * `ALREADY_EXISTS` if an alias was specified in the `id` and already
// exists.
func (r *CoursesService) Create(course *Course) *CoursesCreateCall {
	c := &CoursesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.course = course
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCreateCall) Fields(s ...googleapi.Field) *CoursesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCreateCall) Context(ctx context.Context) *CoursesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.course)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.create" call.
// Exactly one of *Course or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Course.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesCreateCall) Do(opts ...googleapi.CallOption) (*Course, error) {
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
	ret := &Course{
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
	//   "description": "Creates a course. The user specified in `ownerId` is the owner of the created course and added as a teacher. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to create courses or for access errors. * `NOT_FOUND` if the primary teacher is not a valid user. * `FAILED_PRECONDITION` if the course owner's account is disabled or for the following request errors: * UserGroupsMembershipLimitReached * `ALREADY_EXISTS` if an alias was specified in the `id` and already exists.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.create",
	//   "path": "v1/courses",
	//   "request": {
	//     "$ref": "Course"
	//   },
	//   "response": {
	//     "$ref": "Course"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses"
	//   ]
	// }

}

// method id "classroom.courses.delete":

type CoursesDeleteCall struct {
	s          *Service
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a course. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to delete the requested course or for access errors. * `NOT_FOUND` if
// no course exists with the requested ID.
func (r *CoursesService) Delete(id string) *CoursesDeleteCall {
	c := &CoursesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesDeleteCall) Fields(s ...googleapi.Field) *CoursesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesDeleteCall) Context(ctx context.Context) *CoursesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Deletes a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to delete the requested course or for access errors. * `NOT_FOUND` if no course exists with the requested ID.",
	//   "httpMethod": "DELETE",
	//   "id": "classroom.courses.delete",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the course to delete. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{id}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses"
	//   ]
	// }

}

// method id "classroom.courses.get":

type CoursesGetCall struct {
	s            *Service
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a course. This method returns the following error codes:
// * `PERMISSION_DENIED` if the requesting user is not permitted to
// access the requested course or for access errors. * `NOT_FOUND` if no
// course exists with the requested ID.
func (r *CoursesService) Get(id string) *CoursesGetCall {
	c := &CoursesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesGetCall) Fields(s ...googleapi.Field) *CoursesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesGetCall) IfNoneMatch(entityTag string) *CoursesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesGetCall) Context(ctx context.Context) *CoursesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.get" call.
// Exactly one of *Course or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Course.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesGetCall) Do(opts ...googleapi.CallOption) (*Course, error) {
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
	ret := &Course{
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
	//   "description": "Returns a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or for access errors. * `NOT_FOUND` if no course exists with the requested ID.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the course to return. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{id}",
	//   "response": {
	//     "$ref": "Course"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses",
	//     "https://www.googleapis.com/auth/classroom.courses.readonly"
	//   ]
	// }

}

// method id "classroom.courses.list":

type CoursesListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of courses that the requesting user is permitted
// to view, restricted to those that match the request. This method
// returns the following error codes: * `PERMISSION_DENIED` for access
// errors. * `INVALID_ARGUMENT` if the query argument is malformed. *
// `NOT_FOUND` if any users specified in the query arguments do not
// exist.
func (r *CoursesService) List() *CoursesListCall {
	c := &CoursesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// CourseStates sets the optional parameter "courseStates": Restricts
// returned courses to those in one of the specified states
//
// Possible values:
//   "COURSE_STATE_UNSPECIFIED"
//   "ACTIVE"
//   "ARCHIVED"
//   "PROVISIONED"
//   "DECLINED"
func (c *CoursesListCall) CourseStates(courseStates ...string) *CoursesListCall {
	c.urlParams_.SetMulti("courseStates", append([]string{}, courseStates...))
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero or unspecified indicates that the server may
// assign a maximum. The server may return fewer than the specified
// number of results.
func (c *CoursesListCall) PageSize(pageSize int64) *CoursesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *CoursesListCall) PageToken(pageToken string) *CoursesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// StudentId sets the optional parameter "studentId": Restricts returned
// courses to those having a student with the specified identifier. The
// identifier can be one of the following: * the numeric identifier for
// the user * the email address of the user * the string literal "me",
// indicating the requesting user
func (c *CoursesListCall) StudentId(studentId string) *CoursesListCall {
	c.urlParams_.Set("studentId", studentId)
	return c
}

// TeacherId sets the optional parameter "teacherId": Restricts returned
// courses to those having a teacher with the specified identifier. The
// identifier can be one of the following: * the numeric identifier for
// the user * the email address of the user * the string literal "me",
// indicating the requesting user
func (c *CoursesListCall) TeacherId(teacherId string) *CoursesListCall {
	c.urlParams_.Set("teacherId", teacherId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesListCall) Fields(s ...googleapi.Field) *CoursesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesListCall) IfNoneMatch(entityTag string) *CoursesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesListCall) Context(ctx context.Context) *CoursesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.list" call.
// Exactly one of *ListCoursesResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListCoursesResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesListCall) Do(opts ...googleapi.CallOption) (*ListCoursesResponse, error) {
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
	ret := &ListCoursesResponse{
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
	//   "description": "Returns a list of courses that the requesting user is permitted to view, restricted to those that match the request. This method returns the following error codes: * `PERMISSION_DENIED` for access errors. * `INVALID_ARGUMENT` if the query argument is malformed. * `NOT_FOUND` if any users specified in the query arguments do not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.list",
	//   "parameters": {
	//     "courseStates": {
	//       "description": "Restricts returned courses to those in one of the specified states",
	//       "enum": [
	//         "COURSE_STATE_UNSPECIFIED",
	//         "ACTIVE",
	//         "ARCHIVED",
	//         "PROVISIONED",
	//         "DECLINED"
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero or unspecified indicates that the server may assign a maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "Restricts returned courses to those having a student with the specified identifier. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "teacherId": {
	//       "description": "Restricts returned courses to those having a teacher with the specified identifier. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses",
	//   "response": {
	//     "$ref": "ListCoursesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses",
	//     "https://www.googleapis.com/auth/classroom.courses.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CoursesListCall) Pages(ctx context.Context, f func(*ListCoursesResponse) error) error {
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

// method id "classroom.courses.patch":

type CoursesPatchCall struct {
	s          *Service
	id         string
	course     *Course
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates one or more fields in a course. This method returns
// the following error codes: * `PERMISSION_DENIED` if the requesting
// user is not permitted to modify the requested course or for access
// errors. * `NOT_FOUND` if no course exists with the requested ID. *
// `INVALID_ARGUMENT` if invalid fields are specified in the update mask
// or if no update mask is supplied. * `FAILED_PRECONDITION` for the
// following request errors: * CourseNotModifiable
func (r *CoursesService) Patch(id string, course *Course) *CoursesPatchCall {
	c := &CoursesPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.course = course
	return c
}

// UpdateMask sets the optional parameter "updateMask": Mask that
// identifies which fields on the course to update. This field is
// required to do an update. The update will fail if invalid fields are
// specified. The following fields are valid: * `name` * `section` *
// `descriptionHeading` * `description` * `room` * `courseState` When
// set in a query parameter, this field should be specified as
// `updateMask=,,...`
func (c *CoursesPatchCall) UpdateMask(updateMask string) *CoursesPatchCall {
	c.urlParams_.Set("updateMask", updateMask)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesPatchCall) Fields(s ...googleapi.Field) *CoursesPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesPatchCall) Context(ctx context.Context) *CoursesPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.course)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.patch" call.
// Exactly one of *Course or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Course.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesPatchCall) Do(opts ...googleapi.CallOption) (*Course, error) {
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
	ret := &Course{
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
	//   "description": "Updates one or more fields in a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to modify the requested course or for access errors. * `NOT_FOUND` if no course exists with the requested ID. * `INVALID_ARGUMENT` if invalid fields are specified in the update mask or if no update mask is supplied. * `FAILED_PRECONDITION` for the following request errors: * CourseNotModifiable",
	//   "httpMethod": "PATCH",
	//   "id": "classroom.courses.patch",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the course to update. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "updateMask": {
	//       "description": "Mask that identifies which fields on the course to update. This field is required to do an update. The update will fail if invalid fields are specified. The following fields are valid: * `name` * `section` * `descriptionHeading` * `description` * `room` * `courseState` When set in a query parameter, this field should be specified as `updateMask=,,...`",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{id}",
	//   "request": {
	//     "$ref": "Course"
	//   },
	//   "response": {
	//     "$ref": "Course"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses"
	//   ]
	// }

}

// method id "classroom.courses.update":

type CoursesUpdateCall struct {
	s          *Service
	id         string
	course     *Course
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates a course. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to modify the requested course or for access errors. * `NOT_FOUND` if
// no course exists with the requested ID. * `FAILED_PRECONDITION` for
// the following request errors: * CourseNotModifiable
func (r *CoursesService) Update(id string, course *Course) *CoursesUpdateCall {
	c := &CoursesUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	c.course = course
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesUpdateCall) Fields(s ...googleapi.Field) *CoursesUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesUpdateCall) Context(ctx context.Context) *CoursesUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.course)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.update" call.
// Exactly one of *Course or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Course.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesUpdateCall) Do(opts ...googleapi.CallOption) (*Course, error) {
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
	ret := &Course{
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
	//   "description": "Updates a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to modify the requested course or for access errors. * `NOT_FOUND` if no course exists with the requested ID. * `FAILED_PRECONDITION` for the following request errors: * CourseNotModifiable",
	//   "httpMethod": "PUT",
	//   "id": "classroom.courses.update",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the course to update. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{id}",
	//   "request": {
	//     "$ref": "Course"
	//   },
	//   "response": {
	//     "$ref": "Course"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses"
	//   ]
	// }

}

// method id "classroom.courses.aliases.create":

type CoursesAliasesCreateCall struct {
	s           *Service
	courseId    string
	coursealias *CourseAlias
	urlParams_  gensupport.URLParams
	ctx_        context.Context
	header_     http.Header
}

// Create: Creates an alias for a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to create the alias or for access errors. *
// `NOT_FOUND` if the course does not exist. * `ALREADY_EXISTS` if the
// alias already exists.
func (r *CoursesAliasesService) Create(courseId string, coursealias *CourseAlias) *CoursesAliasesCreateCall {
	c := &CoursesAliasesCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.coursealias = coursealias
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesAliasesCreateCall) Fields(s ...googleapi.Field) *CoursesAliasesCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesAliasesCreateCall) Context(ctx context.Context) *CoursesAliasesCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesAliasesCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesAliasesCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.coursealias)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/aliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.aliases.create" call.
// Exactly one of *CourseAlias or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CourseAlias.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CoursesAliasesCreateCall) Do(opts ...googleapi.CallOption) (*CourseAlias, error) {
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
	ret := &CourseAlias{
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
	//   "description": "Creates an alias for a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to create the alias or for access errors. * `NOT_FOUND` if the course does not exist. * `ALREADY_EXISTS` if the alias already exists.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.aliases.create",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course to alias. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/aliases",
	//   "request": {
	//     "$ref": "CourseAlias"
	//   },
	//   "response": {
	//     "$ref": "CourseAlias"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses"
	//   ]
	// }

}

// method id "classroom.courses.aliases.delete":

type CoursesAliasesDeleteCall struct {
	s          *Service
	courseId   string
	aliasid    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an alias of a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to remove the alias or for access errors. *
// `NOT_FOUND` if the alias does not exist.
func (r *CoursesAliasesService) Delete(courseId string, aliasid string) *CoursesAliasesDeleteCall {
	c := &CoursesAliasesDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.aliasid = aliasid
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesAliasesDeleteCall) Fields(s ...googleapi.Field) *CoursesAliasesDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesAliasesDeleteCall) Context(ctx context.Context) *CoursesAliasesDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesAliasesDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesAliasesDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/aliases/{alias}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
		"alias":    c.aliasid,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.aliases.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesAliasesDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Deletes an alias of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to remove the alias or for access errors. * `NOT_FOUND` if the alias does not exist.",
	//   "httpMethod": "DELETE",
	//   "id": "classroom.courses.aliases.delete",
	//   "parameterOrder": [
	//     "courseId",
	//     "alias"
	//   ],
	//   "parameters": {
	//     "alias": {
	//       "description": "Alias to delete. This may not be the Classroom-assigned identifier.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseId": {
	//       "description": "Identifier of the course whose alias should be deleted. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/aliases/{alias}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses"
	//   ]
	// }

}

// method id "classroom.courses.aliases.list":

type CoursesAliasesListCall struct {
	s            *Service
	courseId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of aliases for a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to access the course or for access errors. *
// `NOT_FOUND` if the course does not exist.
func (r *CoursesAliasesService) List(courseId string) *CoursesAliasesListCall {
	c := &CoursesAliasesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero or unspecified indicates that the server may
// assign a maximum. The server may return fewer than the specified
// number of results.
func (c *CoursesAliasesListCall) PageSize(pageSize int64) *CoursesAliasesListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *CoursesAliasesListCall) PageToken(pageToken string) *CoursesAliasesListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesAliasesListCall) Fields(s ...googleapi.Field) *CoursesAliasesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesAliasesListCall) IfNoneMatch(entityTag string) *CoursesAliasesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesAliasesListCall) Context(ctx context.Context) *CoursesAliasesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesAliasesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesAliasesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/aliases")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.aliases.list" call.
// Exactly one of *ListCourseAliasesResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ListCourseAliasesResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesAliasesListCall) Do(opts ...googleapi.CallOption) (*ListCourseAliasesResponse, error) {
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
	ret := &ListCourseAliasesResponse{
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
	//   "description": "Returns a list of aliases for a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the course or for access errors. * `NOT_FOUND` if the course does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.aliases.list",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "The identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero or unspecified indicates that the server may assign a maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/aliases",
	//   "response": {
	//     "$ref": "ListCourseAliasesResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.courses",
	//     "https://www.googleapis.com/auth/classroom.courses.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CoursesAliasesListCall) Pages(ctx context.Context, f func(*ListCourseAliasesResponse) error) error {
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

// method id "classroom.courses.courseWork.create":

type CoursesCourseWorkCreateCall struct {
	s          *Service
	courseId   string
	coursework *CourseWork
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates course work. The resulting course work (and
// corresponding student submissions) are associated with the Developer
// Console project of the [OAuth client
// ID](https://support.google.com/cloud/answer/6158849) used to make the
// request. Classroom API requests to modify course work and student
// submissions must be made with an OAuth client ID from the associated
// Developer Console project. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to access the requested course, create course work in the requested
// course, or for access errors. * `INVALID_ARGUMENT` if the request is
// malformed. * `NOT_FOUND` if the requested course does not exist.
func (r *CoursesCourseWorkService) Create(courseId string, coursework *CourseWork) *CoursesCourseWorkCreateCall {
	c := &CoursesCourseWorkCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.coursework = coursework
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkCreateCall) Fields(s ...googleapi.Field) *CoursesCourseWorkCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkCreateCall) Context(ctx context.Context) *CoursesCourseWorkCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.coursework)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.create" call.
// Exactly one of *CourseWork or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CourseWork.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CoursesCourseWorkCreateCall) Do(opts ...googleapi.CallOption) (*CourseWork, error) {
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
	ret := &CourseWork{
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
	//   "description": "Creates course work. The resulting course work (and corresponding student submissions) are associated with the Developer Console project of the [OAuth client ID](https://support.google.com/cloud/answer/6158849) used to make the request. Classroom API requests to modify course work and student submissions must be made with an OAuth client ID from the associated Developer Console project. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course, create course work in the requested course, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course does not exist.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.courseWork.create",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork",
	//   "request": {
	//     "$ref": "CourseWork"
	//   },
	//   "response": {
	//     "$ref": "CourseWork"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.students"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.get":

type CoursesCourseWorkGetCall struct {
	s            *Service
	courseId     string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns course work. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to access the requested course or course work, or for access errors.
// * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if
// the requested course or course work does not exist.
func (r *CoursesCourseWorkService) Get(courseId string, id string) *CoursesCourseWorkGetCall {
	c := &CoursesCourseWorkGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkGetCall) Fields(s ...googleapi.Field) *CoursesCourseWorkGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesCourseWorkGetCall) IfNoneMatch(entityTag string) *CoursesCourseWorkGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkGetCall) Context(ctx context.Context) *CoursesCourseWorkGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
		"id":       c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.get" call.
// Exactly one of *CourseWork or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *CourseWork.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *CoursesCourseWorkGetCall) Do(opts ...googleapi.CallOption) (*CourseWork, error) {
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
	ret := &CourseWork{
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
	//   "description": "Returns course work. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or course work, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course or course work does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.courseWork.get",
	//   "parameterOrder": [
	//     "courseId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{id}",
	//   "response": {
	//     "$ref": "CourseWork"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.course-work.readonly",
	//     "https://www.googleapis.com/auth/classroom.coursework.me",
	//     "https://www.googleapis.com/auth/classroom.coursework.me.readonly",
	//     "https://www.googleapis.com/auth/classroom.coursework.students",
	//     "https://www.googleapis.com/auth/classroom.coursework.students.readonly"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.list":

type CoursesCourseWorkListCall struct {
	s            *Service
	courseId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of course work that the requester is permitted
// to view. Course students may only view `PUBLISHED` course work.
// Course teachers and domain administrators may view all course work.
// This method returns the following error codes: * `PERMISSION_DENIED`
// if the requesting user is not permitted to access the requested
// course or for access errors. * `INVALID_ARGUMENT` if the request is
// malformed. * `NOT_FOUND` if the requested course does not exist.
func (r *CoursesCourseWorkService) List(courseId string) *CoursesCourseWorkListCall {
	c := &CoursesCourseWorkListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	return c
}

// CourseWorkStates sets the optional parameter "courseWorkStates":
// Restriction on the work status to return. Only courseWork that
// matches is returned. If unspecified, items with a work status of
// `PUBLISHED` is returned.
//
// Possible values:
//   "COURSE_WORK_STATE_UNSPECIFIED"
//   "PUBLISHED"
//   "DRAFT"
//   "DELETED"
func (c *CoursesCourseWorkListCall) CourseWorkStates(courseWorkStates ...string) *CoursesCourseWorkListCall {
	c.urlParams_.SetMulti("courseWorkStates", append([]string{}, courseWorkStates...))
	return c
}

// OrderBy sets the optional parameter "orderBy": Optional sort ordering
// for results. A comma-separated list of fields with an optional sort
// direction keyword. Supported fields are `updateTime` and `dueDate`.
// Supported direction keywords are `asc` and `desc`. If not specified,
// `updateTime desc` is the default behavior. Examples: `dueDate
// asc,updateTime desc`, `updateTime,dueDate desc`
func (c *CoursesCourseWorkListCall) OrderBy(orderBy string) *CoursesCourseWorkListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero or unspecified indicates that the server may
// assign a maximum. The server may return fewer than the specified
// number of results.
func (c *CoursesCourseWorkListCall) PageSize(pageSize int64) *CoursesCourseWorkListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *CoursesCourseWorkListCall) PageToken(pageToken string) *CoursesCourseWorkListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkListCall) Fields(s ...googleapi.Field) *CoursesCourseWorkListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesCourseWorkListCall) IfNoneMatch(entityTag string) *CoursesCourseWorkListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkListCall) Context(ctx context.Context) *CoursesCourseWorkListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.list" call.
// Exactly one of *ListCourseWorkResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListCourseWorkResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesCourseWorkListCall) Do(opts ...googleapi.CallOption) (*ListCourseWorkResponse, error) {
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
	ret := &ListCourseWorkResponse{
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
	//   "description": "Returns a list of course work that the requester is permitted to view. Course students may only view `PUBLISHED` course work. Course teachers and domain administrators may view all course work. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.courseWork.list",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkStates": {
	//       "description": "Restriction on the work status to return. Only courseWork that matches is returned. If unspecified, items with a work status of `PUBLISHED` is returned.",
	//       "enum": [
	//         "COURSE_WORK_STATE_UNSPECIFIED",
	//         "PUBLISHED",
	//         "DRAFT",
	//         "DELETED"
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "orderBy": {
	//       "description": "Optional sort ordering for results. A comma-separated list of fields with an optional sort direction keyword. Supported fields are `updateTime` and `dueDate`. Supported direction keywords are `asc` and `desc`. If not specified, `updateTime desc` is the default behavior. Examples: `dueDate asc,updateTime desc`, `updateTime,dueDate desc`",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero or unspecified indicates that the server may assign a maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork",
	//   "response": {
	//     "$ref": "ListCourseWorkResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.course-work.readonly",
	//     "https://www.googleapis.com/auth/classroom.coursework.me",
	//     "https://www.googleapis.com/auth/classroom.coursework.me.readonly",
	//     "https://www.googleapis.com/auth/classroom.coursework.students",
	//     "https://www.googleapis.com/auth/classroom.coursework.students.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CoursesCourseWorkListCall) Pages(ctx context.Context, f func(*ListCourseWorkResponse) error) error {
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

// method id "classroom.courses.courseWork.studentSubmissions.get":

type CoursesCourseWorkStudentSubmissionsGetCall struct {
	s            *Service
	courseId     string
	courseWorkId string
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a student submission. * `PERMISSION_DENIED` if the
// requesting user is not permitted to access the requested course,
// course work, or student submission or for access errors. *
// `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the
// requested course, course work, or student submission does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) Get(courseId string, courseWorkId string, id string) *CoursesCourseWorkStudentSubmissionsGetCall {
	c := &CoursesCourseWorkStudentSubmissionsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsGetCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesCourseWorkStudentSubmissionsGetCall) IfNoneMatch(entityTag string) *CoursesCourseWorkStudentSubmissionsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsGetCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
		"id":           c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.get" call.
// Exactly one of *StudentSubmission or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *StudentSubmission.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesCourseWorkStudentSubmissionsGetCall) Do(opts ...googleapi.CallOption) (*StudentSubmission, error) {
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
	ret := &StudentSubmission{
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
	//   "description": "Returns a student submission. * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course, course work, or student submission or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course, course work, or student submission does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.courseWork.studentSubmissions.get",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the student submission.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}",
	//   "response": {
	//     "$ref": "StudentSubmission"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.me",
	//     "https://www.googleapis.com/auth/classroom.coursework.me.readonly",
	//     "https://www.googleapis.com/auth/classroom.coursework.students",
	//     "https://www.googleapis.com/auth/classroom.coursework.students.readonly",
	//     "https://www.googleapis.com/auth/classroom.student-submissions.me.readonly",
	//     "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.studentSubmissions.list":

type CoursesCourseWorkStudentSubmissionsListCall struct {
	s            *Service
	courseId     string
	courseWorkId string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of student submissions that the requester is
// permitted to view, factoring in the OAuth scopes of the request. `-`
// may be specified as the `course_work_id` to include student
// submissions for multiple course work items. Course students may only
// view their own work. Course teachers and domain administrators may
// view all student submissions. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to access the requested course or course work, or for access errors.
// * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if
// the requested course does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) List(courseId string, courseWorkId string) *CoursesCourseWorkStudentSubmissionsListCall {
	c := &CoursesCourseWorkStudentSubmissionsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	return c
}

// Late sets the optional parameter "late": Requested lateness value. If
// specified, returned student submissions are restricted by the
// requested value. If unspecified, submissions are returned regardless
// of `late` value.
//
// Possible values:
//   "LATE_VALUES_UNSPECIFIED"
//   "LATE_ONLY"
//   "NOT_LATE_ONLY"
func (c *CoursesCourseWorkStudentSubmissionsListCall) Late(late string) *CoursesCourseWorkStudentSubmissionsListCall {
	c.urlParams_.Set("late", late)
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero or unspecified indicates that the server may
// assign a maximum. The server may return fewer than the specified
// number of results.
func (c *CoursesCourseWorkStudentSubmissionsListCall) PageSize(pageSize int64) *CoursesCourseWorkStudentSubmissionsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *CoursesCourseWorkStudentSubmissionsListCall) PageToken(pageToken string) *CoursesCourseWorkStudentSubmissionsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// States sets the optional parameter "states": Requested submission
// states. If specified, returned student submissions match one of the
// specified submission states.
//
// Possible values:
//   "SUBMISSION_STATE_UNSPECIFIED"
//   "NEW"
//   "CREATED"
//   "TURNED_IN"
//   "RETURNED"
//   "RECLAIMED_BY_STUDENT"
func (c *CoursesCourseWorkStudentSubmissionsListCall) States(states ...string) *CoursesCourseWorkStudentSubmissionsListCall {
	c.urlParams_.SetMulti("states", append([]string{}, states...))
	return c
}

// UserId sets the optional parameter "userId": Optional argument to
// restrict returned student work to those owned by the student with the
// specified identifier. The identifier can be one of the following: *
// the numeric identifier for the user * the email address of the user *
// the string literal "me", indicating the requesting user
func (c *CoursesCourseWorkStudentSubmissionsListCall) UserId(userId string) *CoursesCourseWorkStudentSubmissionsListCall {
	c.urlParams_.Set("userId", userId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsListCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesCourseWorkStudentSubmissionsListCall) IfNoneMatch(entityTag string) *CoursesCourseWorkStudentSubmissionsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsListCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.list" call.
// Exactly one of *ListStudentSubmissionsResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ListStudentSubmissionsResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesCourseWorkStudentSubmissionsListCall) Do(opts ...googleapi.CallOption) (*ListStudentSubmissionsResponse, error) {
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
	ret := &ListStudentSubmissionsResponse{
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
	//   "description": "Returns a list of student submissions that the requester is permitted to view, factoring in the OAuth scopes of the request. `-` may be specified as the `course_work_id` to include student submissions for multiple course work items. Course students may only view their own work. Course teachers and domain administrators may view all student submissions. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or course work, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.courseWork.studentSubmissions.list",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifer of the student work to request. This may be set to the string literal `\"-\"` to request student work for all course work in the specified course.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "late": {
	//       "description": "Requested lateness value. If specified, returned student submissions are restricted by the requested value. If unspecified, submissions are returned regardless of `late` value.",
	//       "enum": [
	//         "LATE_VALUES_UNSPECIFIED",
	//         "LATE_ONLY",
	//         "NOT_LATE_ONLY"
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero or unspecified indicates that the server may assign a maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "states": {
	//       "description": "Requested submission states. If specified, returned student submissions match one of the specified submission states.",
	//       "enum": [
	//         "SUBMISSION_STATE_UNSPECIFIED",
	//         "NEW",
	//         "CREATED",
	//         "TURNED_IN",
	//         "RETURNED",
	//         "RECLAIMED_BY_STUDENT"
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "Optional argument to restrict returned student work to those owned by the student with the specified identifier. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions",
	//   "response": {
	//     "$ref": "ListStudentSubmissionsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.me",
	//     "https://www.googleapis.com/auth/classroom.coursework.me.readonly",
	//     "https://www.googleapis.com/auth/classroom.coursework.students",
	//     "https://www.googleapis.com/auth/classroom.coursework.students.readonly",
	//     "https://www.googleapis.com/auth/classroom.student-submissions.me.readonly",
	//     "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CoursesCourseWorkStudentSubmissionsListCall) Pages(ctx context.Context, f func(*ListStudentSubmissionsResponse) error) error {
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

// method id "classroom.courses.courseWork.studentSubmissions.modifyAttachments":

type CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall struct {
	s                        *Service
	courseId                 string
	courseWorkId             string
	id                       string
	modifyattachmentsrequest *ModifyAttachmentsRequest
	urlParams_               gensupport.URLParams
	ctx_                     context.Context
	header_                  http.Header
}

// ModifyAttachments: Modifies attachments of student submission.
// Attachments may only be added to student submissions whose type is
// `ASSIGNMENT`. This request must be made by the Developer Console
// project of the [OAuth client
// ID](https://support.google.com/cloud/answer/6158849) used to create
// the corresponding course work item. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to access the requested course or course work, if the user
// is not permitted to modify attachments on the requested student
// submission, or for access errors. * `INVALID_ARGUMENT` if the request
// is malformed. * `NOT_FOUND` if the requested course, course work, or
// student submission does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) ModifyAttachments(courseId string, courseWorkId string, id string, modifyattachmentsrequest *ModifyAttachmentsRequest) *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall {
	c := &CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	c.id = id
	c.modifyattachmentsrequest = modifyattachmentsrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.modifyattachmentsrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:modifyAttachments")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
		"id":           c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.modifyAttachments" call.
// Exactly one of *StudentSubmission or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *StudentSubmission.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesCourseWorkStudentSubmissionsModifyAttachmentsCall) Do(opts ...googleapi.CallOption) (*StudentSubmission, error) {
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
	ret := &StudentSubmission{
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
	//   "description": "Modifies attachments of student submission. Attachments may only be added to student submissions whose type is `ASSIGNMENT`. This request must be made by the Developer Console project of the [OAuth client ID](https://support.google.com/cloud/answer/6158849) used to create the corresponding course work item. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or course work, if the user is not permitted to modify attachments on the requested student submission, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course, course work, or student submission does not exist.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.courseWork.studentSubmissions.modifyAttachments",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the student submission.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:modifyAttachments",
	//   "request": {
	//     "$ref": "ModifyAttachmentsRequest"
	//   },
	//   "response": {
	//     "$ref": "StudentSubmission"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.me",
	//     "https://www.googleapis.com/auth/classroom.coursework.students"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.studentSubmissions.patch":

type CoursesCourseWorkStudentSubmissionsPatchCall struct {
	s                 *Service
	courseId          string
	courseWorkId      string
	id                string
	studentsubmission *StudentSubmission
	urlParams_        gensupport.URLParams
	ctx_              context.Context
	header_           http.Header
}

// Patch: Updates one or more fields of a student submission. See
// google.classroom.v1.StudentSubmission for details of which fields may
// be updated and who may change them. This request must be made by the
// Developer Console project of the [OAuth client
// ID](https://support.google.com/cloud/answer/6158849) used to create
// the corresponding course work item. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting developer
// project did not create the corresponding course work, if the user is
// not permitted to make the requested modification to the student
// submission, or for access errors. * `INVALID_ARGUMENT` if the request
// is malformed. * `NOT_FOUND` if the requested course, course work, or
// student submission does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) Patch(courseId string, courseWorkId string, id string, studentsubmission *StudentSubmission) *CoursesCourseWorkStudentSubmissionsPatchCall {
	c := &CoursesCourseWorkStudentSubmissionsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	c.id = id
	c.studentsubmission = studentsubmission
	return c
}

// UpdateMask sets the optional parameter "updateMask": Mask that
// identifies which fields on the student submission to update. This
// field is required to do an update. The update fails if invalid fields
// are specified. The following fields may be specified by teachers: *
// `draft_grade` * `assigned_grade`
func (c *CoursesCourseWorkStudentSubmissionsPatchCall) UpdateMask(updateMask string) *CoursesCourseWorkStudentSubmissionsPatchCall {
	c.urlParams_.Set("updateMask", updateMask)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsPatchCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsPatchCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.studentsubmission)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
		"id":           c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.patch" call.
// Exactly one of *StudentSubmission or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *StudentSubmission.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesCourseWorkStudentSubmissionsPatchCall) Do(opts ...googleapi.CallOption) (*StudentSubmission, error) {
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
	ret := &StudentSubmission{
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
	//   "description": "Updates one or more fields of a student submission. See google.classroom.v1.StudentSubmission for details of which fields may be updated and who may change them. This request must be made by the Developer Console project of the [OAuth client ID](https://support.google.com/cloud/answer/6158849) used to create the corresponding course work item. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting developer project did not create the corresponding course work, if the user is not permitted to make the requested modification to the student submission, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course, course work, or student submission does not exist.",
	//   "httpMethod": "PATCH",
	//   "id": "classroom.courses.courseWork.studentSubmissions.patch",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the student submission.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "updateMask": {
	//       "description": "Mask that identifies which fields on the student submission to update. This field is required to do an update. The update fails if invalid fields are specified. The following fields may be specified by teachers: * `draft_grade` * `assigned_grade`",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}",
	//   "request": {
	//     "$ref": "StudentSubmission"
	//   },
	//   "response": {
	//     "$ref": "StudentSubmission"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.me",
	//     "https://www.googleapis.com/auth/classroom.coursework.students"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.studentSubmissions.reclaim":

type CoursesCourseWorkStudentSubmissionsReclaimCall struct {
	s                               *Service
	courseId                        string
	courseWorkId                    string
	id                              string
	reclaimstudentsubmissionrequest *ReclaimStudentSubmissionRequest
	urlParams_                      gensupport.URLParams
	ctx_                            context.Context
	header_                         http.Header
}

// Reclaim: Reclaims a student submission on behalf of the student that
// owns it. Reclaiming a student submission transfers ownership of
// attached Drive files to the student and update the submission state.
// Only the student that ownes the requested student submission may call
// this method, and only for a student submission that has been turned
// in. This request must be made by the Developer Console project of the
// [OAuth client ID](https://support.google.com/cloud/answer/6158849)
// used to create the corresponding course work item. This method
// returns the following error codes: * `PERMISSION_DENIED` if the
// requesting user is not permitted to access the requested course or
// course work, unsubmit the requested student submission, or for access
// errors. * `FAILED_PRECONDITION` if the student submission has not
// been turned in. * `INVALID_ARGUMENT` if the request is malformed. *
// `NOT_FOUND` if the requested course, course work, or student
// submission does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) Reclaim(courseId string, courseWorkId string, id string, reclaimstudentsubmissionrequest *ReclaimStudentSubmissionRequest) *CoursesCourseWorkStudentSubmissionsReclaimCall {
	c := &CoursesCourseWorkStudentSubmissionsReclaimCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	c.id = id
	c.reclaimstudentsubmissionrequest = reclaimstudentsubmissionrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsReclaimCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsReclaimCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsReclaimCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsReclaimCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsReclaimCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsReclaimCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.reclaimstudentsubmissionrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:reclaim")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
		"id":           c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.reclaim" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesCourseWorkStudentSubmissionsReclaimCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Reclaims a student submission on behalf of the student that owns it. Reclaiming a student submission transfers ownership of attached Drive files to the student and update the submission state. Only the student that ownes the requested student submission may call this method, and only for a student submission that has been turned in. This request must be made by the Developer Console project of the [OAuth client ID](https://support.google.com/cloud/answer/6158849) used to create the corresponding course work item. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or course work, unsubmit the requested student submission, or for access errors. * `FAILED_PRECONDITION` if the student submission has not been turned in. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course, course work, or student submission does not exist.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.courseWork.studentSubmissions.reclaim",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the student submission.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:reclaim",
	//   "request": {
	//     "$ref": "ReclaimStudentSubmissionRequest"
	//   },
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.me"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.studentSubmissions.return":

type CoursesCourseWorkStudentSubmissionsReturnCall struct {
	s                              *Service
	courseId                       string
	courseWorkId                   string
	id                             string
	returnstudentsubmissionrequest *ReturnStudentSubmissionRequest
	urlParams_                     gensupport.URLParams
	ctx_                           context.Context
	header_                        http.Header
}

// Return: Returns a student submission. Returning a student submission
// transfers ownership of attached Drive files to the student and may
// also update the submission state. Unlike the Classroom application,
// returning a student submission does not set assignedGrade to the
// draftGrade value. Only a teacher of the course that contains the
// requested student submission may call this method. This request must
// be made by the Developer Console project of the [OAuth client
// ID](https://support.google.com/cloud/answer/6158849) used to create
// the corresponding course work item. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to access the requested course or course work, return the
// requested student submission, or for access errors. *
// `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the
// requested course, course work, or student submission does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) Return(courseId string, courseWorkId string, id string, returnstudentsubmissionrequest *ReturnStudentSubmissionRequest) *CoursesCourseWorkStudentSubmissionsReturnCall {
	c := &CoursesCourseWorkStudentSubmissionsReturnCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	c.id = id
	c.returnstudentsubmissionrequest = returnstudentsubmissionrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsReturnCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsReturnCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsReturnCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsReturnCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsReturnCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsReturnCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.returnstudentsubmissionrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:return")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
		"id":           c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.return" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesCourseWorkStudentSubmissionsReturnCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Returns a student submission. Returning a student submission transfers ownership of attached Drive files to the student and may also update the submission state. Unlike the Classroom application, returning a student submission does not set assignedGrade to the draftGrade value. Only a teacher of the course that contains the requested student submission may call this method. This request must be made by the Developer Console project of the [OAuth client ID](https://support.google.com/cloud/answer/6158849) used to create the corresponding course work item. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or course work, return the requested student submission, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course, course work, or student submission does not exist.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.courseWork.studentSubmissions.return",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the student submission.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:return",
	//   "request": {
	//     "$ref": "ReturnStudentSubmissionRequest"
	//   },
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.students"
	//   ]
	// }

}

// method id "classroom.courses.courseWork.studentSubmissions.turnIn":

type CoursesCourseWorkStudentSubmissionsTurnInCall struct {
	s                              *Service
	courseId                       string
	courseWorkId                   string
	id                             string
	turninstudentsubmissionrequest *TurnInStudentSubmissionRequest
	urlParams_                     gensupport.URLParams
	ctx_                           context.Context
	header_                        http.Header
}

// TurnIn: Turns in a student submission. Turning in a student
// submission transfers ownership of attached Drive files to the teacher
// and may also update the submission state. This may only be called by
// the student that owns the specified student submission. This request
// must be made by the Developer Console project of the [OAuth client
// ID](https://support.google.com/cloud/answer/6158849) used to create
// the corresponding course work item. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to access the requested course or course work, turn in the
// requested student submission, or for access errors. *
// `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the
// requested course, course work, or student submission does not exist.
func (r *CoursesCourseWorkStudentSubmissionsService) TurnIn(courseId string, courseWorkId string, id string, turninstudentsubmissionrequest *TurnInStudentSubmissionRequest) *CoursesCourseWorkStudentSubmissionsTurnInCall {
	c := &CoursesCourseWorkStudentSubmissionsTurnInCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.courseWorkId = courseWorkId
	c.id = id
	c.turninstudentsubmissionrequest = turninstudentsubmissionrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesCourseWorkStudentSubmissionsTurnInCall) Fields(s ...googleapi.Field) *CoursesCourseWorkStudentSubmissionsTurnInCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesCourseWorkStudentSubmissionsTurnInCall) Context(ctx context.Context) *CoursesCourseWorkStudentSubmissionsTurnInCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesCourseWorkStudentSubmissionsTurnInCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesCourseWorkStudentSubmissionsTurnInCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.turninstudentsubmissionrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:turnIn")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId":     c.courseId,
		"courseWorkId": c.courseWorkId,
		"id":           c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.courseWork.studentSubmissions.turnIn" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesCourseWorkStudentSubmissionsTurnInCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Turns in a student submission. Turning in a student submission transfers ownership of attached Drive files to the teacher and may also update the submission state. This may only be called by the student that owns the specified student submission. This request must be made by the Developer Console project of the [OAuth client ID](https://support.google.com/cloud/answer/6158849) used to create the corresponding course work item. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access the requested course or course work, turn in the requested student submission, or for access errors. * `INVALID_ARGUMENT` if the request is malformed. * `NOT_FOUND` if the requested course, course work, or student submission does not exist.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.courseWork.studentSubmissions.turnIn",
	//   "parameterOrder": [
	//     "courseId",
	//     "courseWorkId",
	//     "id"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "courseWorkId": {
	//       "description": "Identifier of the course work.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "id": {
	//       "description": "Identifier of the student submission.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/courseWork/{courseWorkId}/studentSubmissions/{id}:turnIn",
	//   "request": {
	//     "$ref": "TurnInStudentSubmissionRequest"
	//   },
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.coursework.me"
	//   ]
	// }

}

// method id "classroom.courses.students.create":

type CoursesStudentsCreateCall struct {
	s          *Service
	courseId   string
	student    *Student
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Adds a user as a student of a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to create students in this course or for access
// errors. * `NOT_FOUND` if the requested course ID does not exist. *
// `FAILED_PRECONDITION` if the requested user's account is disabled,
// for the following request errors: * CourseMemberLimitReached *
// CourseNotModifiable * UserGroupsMembershipLimitReached *
// `ALREADY_EXISTS` if the user is already a student or teacher in the
// course.
func (r *CoursesStudentsService) Create(courseId string, student *Student) *CoursesStudentsCreateCall {
	c := &CoursesStudentsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.student = student
	return c
}

// EnrollmentCode sets the optional parameter "enrollmentCode":
// Enrollment code of the course to create the student in. This code is
// required if userId corresponds to the requesting user; it may be
// omitted if the requesting user has administrative permissions to
// create students for any user.
func (c *CoursesStudentsCreateCall) EnrollmentCode(enrollmentCode string) *CoursesStudentsCreateCall {
	c.urlParams_.Set("enrollmentCode", enrollmentCode)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesStudentsCreateCall) Fields(s ...googleapi.Field) *CoursesStudentsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesStudentsCreateCall) Context(ctx context.Context) *CoursesStudentsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesStudentsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesStudentsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.student)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/students")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.students.create" call.
// Exactly one of *Student or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Student.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesStudentsCreateCall) Do(opts ...googleapi.CallOption) (*Student, error) {
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
	ret := &Student{
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
	//   "description": "Adds a user as a student of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to create students in this course or for access errors. * `NOT_FOUND` if the requested course ID does not exist. * `FAILED_PRECONDITION` if the requested user's account is disabled, for the following request errors: * CourseMemberLimitReached * CourseNotModifiable * UserGroupsMembershipLimitReached * `ALREADY_EXISTS` if the user is already a student or teacher in the course.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.students.create",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course to create the student in. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "enrollmentCode": {
	//       "description": "Enrollment code of the course to create the student in. This code is required if userId corresponds to the requesting user; it may be omitted if the requesting user has administrative permissions to create students for any user.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/students",
	//   "request": {
	//     "$ref": "Student"
	//   },
	//   "response": {
	//     "$ref": "Student"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.courses.students.delete":

type CoursesStudentsDeleteCall struct {
	s          *Service
	courseId   string
	userId     string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a student of a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to delete students of this course or for access
// errors. * `NOT_FOUND` if no student of this course has the requested
// ID or if the course does not exist.
func (r *CoursesStudentsService) Delete(courseId string, userId string) *CoursesStudentsDeleteCall {
	c := &CoursesStudentsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesStudentsDeleteCall) Fields(s ...googleapi.Field) *CoursesStudentsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesStudentsDeleteCall) Context(ctx context.Context) *CoursesStudentsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesStudentsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesStudentsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/students/{userId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
		"userId":   c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.students.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesStudentsDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Deletes a student of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to delete students of this course or for access errors. * `NOT_FOUND` if no student of this course has the requested ID or if the course does not exist.",
	//   "httpMethod": "DELETE",
	//   "id": "classroom.courses.students.delete",
	//   "parameterOrder": [
	//     "courseId",
	//     "userId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "Identifier of the student to delete. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/students/{userId}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.courses.students.get":

type CoursesStudentsGetCall struct {
	s            *Service
	courseId     string
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a student of a course. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to view students of this course or for access errors. *
// `NOT_FOUND` if no student of this course has the requested ID or if
// the course does not exist.
func (r *CoursesStudentsService) Get(courseId string, userId string) *CoursesStudentsGetCall {
	c := &CoursesStudentsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesStudentsGetCall) Fields(s ...googleapi.Field) *CoursesStudentsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesStudentsGetCall) IfNoneMatch(entityTag string) *CoursesStudentsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesStudentsGetCall) Context(ctx context.Context) *CoursesStudentsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesStudentsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesStudentsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/students/{userId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
		"userId":   c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.students.get" call.
// Exactly one of *Student or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Student.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesStudentsGetCall) Do(opts ...googleapi.CallOption) (*Student, error) {
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
	ret := &Student{
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
	//   "description": "Returns a student of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to view students of this course or for access errors. * `NOT_FOUND` if no student of this course has the requested ID or if the course does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.students.get",
	//   "parameterOrder": [
	//     "courseId",
	//     "userId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "Identifier of the student to return. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/students/{userId}",
	//   "response": {
	//     "$ref": "Student"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// method id "classroom.courses.students.list":

type CoursesStudentsListCall struct {
	s            *Service
	courseId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of students of this course that the requester is
// permitted to view. This method returns the following error codes: *
// `NOT_FOUND` if the course does not exist. * `PERMISSION_DENIED` for
// access errors.
func (r *CoursesStudentsService) List(courseId string) *CoursesStudentsListCall {
	c := &CoursesStudentsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero means no maximum. The server may return fewer
// than the specified number of results.
func (c *CoursesStudentsListCall) PageSize(pageSize int64) *CoursesStudentsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *CoursesStudentsListCall) PageToken(pageToken string) *CoursesStudentsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesStudentsListCall) Fields(s ...googleapi.Field) *CoursesStudentsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesStudentsListCall) IfNoneMatch(entityTag string) *CoursesStudentsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesStudentsListCall) Context(ctx context.Context) *CoursesStudentsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesStudentsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesStudentsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/students")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.students.list" call.
// Exactly one of *ListStudentsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListStudentsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesStudentsListCall) Do(opts ...googleapi.CallOption) (*ListStudentsResponse, error) {
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
	ret := &ListStudentsResponse{
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
	//   "description": "Returns a list of students of this course that the requester is permitted to view. This method returns the following error codes: * `NOT_FOUND` if the course does not exist. * `PERMISSION_DENIED` for access errors.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.students.list",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero means no maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/students",
	//   "response": {
	//     "$ref": "ListStudentsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CoursesStudentsListCall) Pages(ctx context.Context, f func(*ListStudentsResponse) error) error {
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

// method id "classroom.courses.teachers.create":

type CoursesTeachersCreateCall struct {
	s          *Service
	courseId   string
	teacher    *Teacher
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates a teacher of a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to create teachers in this course or for access
// errors. * `NOT_FOUND` if the requested course ID does not exist. *
// `FAILED_PRECONDITION` if the requested user's account is disabled,
// for the following request errors: * CourseMemberLimitReached *
// CourseNotModifiable * CourseTeacherLimitReached *
// UserGroupsMembershipLimitReached * `ALREADY_EXISTS` if the user is
// already a teacher or student in the course.
func (r *CoursesTeachersService) Create(courseId string, teacher *Teacher) *CoursesTeachersCreateCall {
	c := &CoursesTeachersCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.teacher = teacher
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesTeachersCreateCall) Fields(s ...googleapi.Field) *CoursesTeachersCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesTeachersCreateCall) Context(ctx context.Context) *CoursesTeachersCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesTeachersCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesTeachersCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.teacher)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/teachers")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.teachers.create" call.
// Exactly one of *Teacher or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Teacher.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesTeachersCreateCall) Do(opts ...googleapi.CallOption) (*Teacher, error) {
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
	ret := &Teacher{
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
	//   "description": "Creates a teacher of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to create teachers in this course or for access errors. * `NOT_FOUND` if the requested course ID does not exist. * `FAILED_PRECONDITION` if the requested user's account is disabled, for the following request errors: * CourseMemberLimitReached * CourseNotModifiable * CourseTeacherLimitReached * UserGroupsMembershipLimitReached * `ALREADY_EXISTS` if the user is already a teacher or student in the course.",
	//   "httpMethod": "POST",
	//   "id": "classroom.courses.teachers.create",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/teachers",
	//   "request": {
	//     "$ref": "Teacher"
	//   },
	//   "response": {
	//     "$ref": "Teacher"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.courses.teachers.delete":

type CoursesTeachersDeleteCall struct {
	s          *Service
	courseId   string
	userId     string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a teacher of a course. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to delete teachers of this course or for access
// errors. * `NOT_FOUND` if no teacher of this course has the requested
// ID or if the course does not exist. * `FAILED_PRECONDITION` if the
// requested ID belongs to the primary teacher of this course.
func (r *CoursesTeachersService) Delete(courseId string, userId string) *CoursesTeachersDeleteCall {
	c := &CoursesTeachersDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesTeachersDeleteCall) Fields(s ...googleapi.Field) *CoursesTeachersDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesTeachersDeleteCall) Context(ctx context.Context) *CoursesTeachersDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesTeachersDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesTeachersDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/teachers/{userId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
		"userId":   c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.teachers.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesTeachersDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Deletes a teacher of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to delete teachers of this course or for access errors. * `NOT_FOUND` if no teacher of this course has the requested ID or if the course does not exist. * `FAILED_PRECONDITION` if the requested ID belongs to the primary teacher of this course.",
	//   "httpMethod": "DELETE",
	//   "id": "classroom.courses.teachers.delete",
	//   "parameterOrder": [
	//     "courseId",
	//     "userId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "Identifier of the teacher to delete. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/teachers/{userId}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.courses.teachers.get":

type CoursesTeachersGetCall struct {
	s            *Service
	courseId     string
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a teacher of a course. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to view teachers of this course or for access errors. *
// `NOT_FOUND` if no teacher of this course has the requested ID or if
// the course does not exist.
func (r *CoursesTeachersService) Get(courseId string, userId string) *CoursesTeachersGetCall {
	c := &CoursesTeachersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesTeachersGetCall) Fields(s ...googleapi.Field) *CoursesTeachersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesTeachersGetCall) IfNoneMatch(entityTag string) *CoursesTeachersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesTeachersGetCall) Context(ctx context.Context) *CoursesTeachersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesTeachersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesTeachersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/teachers/{userId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
		"userId":   c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.teachers.get" call.
// Exactly one of *Teacher or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Teacher.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *CoursesTeachersGetCall) Do(opts ...googleapi.CallOption) (*Teacher, error) {
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
	ret := &Teacher{
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
	//   "description": "Returns a teacher of a course. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to view teachers of this course or for access errors. * `NOT_FOUND` if no teacher of this course has the requested ID or if the course does not exist.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.teachers.get",
	//   "parameterOrder": [
	//     "courseId",
	//     "userId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "Identifier of the teacher to return. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/teachers/{userId}",
	//   "response": {
	//     "$ref": "Teacher"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// method id "classroom.courses.teachers.list":

type CoursesTeachersListCall struct {
	s            *Service
	courseId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of teachers of this course that the requester is
// permitted to view. This method returns the following error codes: *
// `NOT_FOUND` if the course does not exist. * `PERMISSION_DENIED` for
// access errors.
func (r *CoursesTeachersService) List(courseId string) *CoursesTeachersListCall {
	c := &CoursesTeachersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.courseId = courseId
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero means no maximum. The server may return fewer
// than the specified number of results.
func (c *CoursesTeachersListCall) PageSize(pageSize int64) *CoursesTeachersListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *CoursesTeachersListCall) PageToken(pageToken string) *CoursesTeachersListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CoursesTeachersListCall) Fields(s ...googleapi.Field) *CoursesTeachersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *CoursesTeachersListCall) IfNoneMatch(entityTag string) *CoursesTeachersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CoursesTeachersListCall) Context(ctx context.Context) *CoursesTeachersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CoursesTeachersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CoursesTeachersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/courses/{courseId}/teachers")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"courseId": c.courseId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.courses.teachers.list" call.
// Exactly one of *ListTeachersResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListTeachersResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CoursesTeachersListCall) Do(opts ...googleapi.CallOption) (*ListTeachersResponse, error) {
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
	ret := &ListTeachersResponse{
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
	//   "description": "Returns a list of teachers of this course that the requester is permitted to view. This method returns the following error codes: * `NOT_FOUND` if the course does not exist. * `PERMISSION_DENIED` for access errors.",
	//   "httpMethod": "GET",
	//   "id": "classroom.courses.teachers.list",
	//   "parameterOrder": [
	//     "courseId"
	//   ],
	//   "parameters": {
	//     "courseId": {
	//       "description": "Identifier of the course. This identifier can be either the Classroom-assigned identifier or an alias.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero means no maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/courses/{courseId}/teachers",
	//   "response": {
	//     "$ref": "ListTeachersResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *CoursesTeachersListCall) Pages(ctx context.Context, f func(*ListTeachersResponse) error) error {
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

// method id "classroom.invitations.accept":

type InvitationsAcceptCall struct {
	s          *Service
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Accept: Accepts an invitation, removing it and adding the invited
// user to the teachers or students (as appropriate) of the specified
// course. Only the invited user may accept an invitation. This method
// returns the following error codes: * `PERMISSION_DENIED` if the
// requesting user is not permitted to accept the requested invitation
// or for access errors. * `FAILED_PRECONDITION` for the following
// request errors: * CourseMemberLimitReached * CourseNotModifiable *
// CourseTeacherLimitReached * UserGroupsMembershipLimitReached *
// `NOT_FOUND` if no invitation exists with the requested ID.
func (r *InvitationsService) Accept(id string) *InvitationsAcceptCall {
	c := &InvitationsAcceptCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *InvitationsAcceptCall) Fields(s ...googleapi.Field) *InvitationsAcceptCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *InvitationsAcceptCall) Context(ctx context.Context) *InvitationsAcceptCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *InvitationsAcceptCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *InvitationsAcceptCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/invitations/{id}:accept")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.invitations.accept" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *InvitationsAcceptCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Accepts an invitation, removing it and adding the invited user to the teachers or students (as appropriate) of the specified course. Only the invited user may accept an invitation. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to accept the requested invitation or for access errors. * `FAILED_PRECONDITION` for the following request errors: * CourseMemberLimitReached * CourseNotModifiable * CourseTeacherLimitReached * UserGroupsMembershipLimitReached * `NOT_FOUND` if no invitation exists with the requested ID.",
	//   "httpMethod": "POST",
	//   "id": "classroom.invitations.accept",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the invitation to accept.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/invitations/{id}:accept",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.invitations.create":

type InvitationsCreateCall struct {
	s          *Service
	invitation *Invitation
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Creates an invitation. Only one invitation for a user and
// course may exist at a time. Delete and re-create an invitation to
// make changes. This method returns the following error codes: *
// `PERMISSION_DENIED` if the requesting user is not permitted to create
// invitations for this course or for access errors. * `NOT_FOUND` if
// the course or the user does not exist. * `FAILED_PRECONDITION` if the
// requested user's account is disabled or if the user already has this
// role or a role with greater permissions. * `ALREADY_EXISTS` if an
// invitation for the specified user and course already exists.
func (r *InvitationsService) Create(invitation *Invitation) *InvitationsCreateCall {
	c := &InvitationsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.invitation = invitation
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *InvitationsCreateCall) Fields(s ...googleapi.Field) *InvitationsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *InvitationsCreateCall) Context(ctx context.Context) *InvitationsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *InvitationsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *InvitationsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.invitation)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/invitations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.invitations.create" call.
// Exactly one of *Invitation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Invitation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *InvitationsCreateCall) Do(opts ...googleapi.CallOption) (*Invitation, error) {
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
	ret := &Invitation{
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
	//   "description": "Creates an invitation. Only one invitation for a user and course may exist at a time. Delete and re-create an invitation to make changes. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to create invitations for this course or for access errors. * `NOT_FOUND` if the course or the user does not exist. * `FAILED_PRECONDITION` if the requested user's account is disabled or if the user already has this role or a role with greater permissions. * `ALREADY_EXISTS` if an invitation for the specified user and course already exists.",
	//   "httpMethod": "POST",
	//   "id": "classroom.invitations.create",
	//   "path": "v1/invitations",
	//   "request": {
	//     "$ref": "Invitation"
	//   },
	//   "response": {
	//     "$ref": "Invitation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.invitations.delete":

type InvitationsDeleteCall struct {
	s          *Service
	id         string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes an invitation. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to delete the requested invitation or for access errors. *
// `NOT_FOUND` if no invitation exists with the requested ID.
func (r *InvitationsService) Delete(id string) *InvitationsDeleteCall {
	c := &InvitationsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *InvitationsDeleteCall) Fields(s ...googleapi.Field) *InvitationsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *InvitationsDeleteCall) Context(ctx context.Context) *InvitationsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *InvitationsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *InvitationsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/invitations/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.invitations.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *InvitationsDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Deletes an invitation. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to delete the requested invitation or for access errors. * `NOT_FOUND` if no invitation exists with the requested ID.",
	//   "httpMethod": "DELETE",
	//   "id": "classroom.invitations.delete",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the invitation to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/invitations/{id}",
	//   "response": {
	//     "$ref": "Empty"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters"
	//   ]
	// }

}

// method id "classroom.invitations.get":

type InvitationsGetCall struct {
	s            *Service
	id           string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns an invitation. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to view the requested invitation or for access errors. * `NOT_FOUND`
// if no invitation exists with the requested ID.
func (r *InvitationsService) Get(id string) *InvitationsGetCall {
	c := &InvitationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.id = id
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *InvitationsGetCall) Fields(s ...googleapi.Field) *InvitationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *InvitationsGetCall) IfNoneMatch(entityTag string) *InvitationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *InvitationsGetCall) Context(ctx context.Context) *InvitationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *InvitationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *InvitationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/invitations/{id}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"id": c.id,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.invitations.get" call.
// Exactly one of *Invitation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Invitation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *InvitationsGetCall) Do(opts ...googleapi.CallOption) (*Invitation, error) {
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
	ret := &Invitation{
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
	//   "description": "Returns an invitation. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to view the requested invitation or for access errors. * `NOT_FOUND` if no invitation exists with the requested ID.",
	//   "httpMethod": "GET",
	//   "id": "classroom.invitations.get",
	//   "parameterOrder": [
	//     "id"
	//   ],
	//   "parameters": {
	//     "id": {
	//       "description": "Identifier of the invitation to return.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/invitations/{id}",
	//   "response": {
	//     "$ref": "Invitation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// method id "classroom.invitations.list":

type InvitationsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of invitations that the requesting user is
// permitted to view, restricted to those that match the list request.
// *Note:* At least one of `user_id` or `course_id` must be supplied.
// Both fields can be supplied. This method returns the following error
// codes: * `PERMISSION_DENIED` for access errors.
func (r *InvitationsService) List() *InvitationsListCall {
	c := &InvitationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// CourseId sets the optional parameter "courseId": Restricts returned
// invitations to those for a course with the specified identifier.
func (c *InvitationsListCall) CourseId(courseId string) *InvitationsListCall {
	c.urlParams_.Set("courseId", courseId)
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero means no maximum. The server may return fewer
// than the specified number of results.
func (c *InvitationsListCall) PageSize(pageSize int64) *InvitationsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *InvitationsListCall) PageToken(pageToken string) *InvitationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// UserId sets the optional parameter "userId": Restricts returned
// invitations to those for a specific user. The identifier can be one
// of the following: * the numeric identifier for the user * the email
// address of the user * the string literal "me", indicating the
// requesting user
func (c *InvitationsListCall) UserId(userId string) *InvitationsListCall {
	c.urlParams_.Set("userId", userId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *InvitationsListCall) Fields(s ...googleapi.Field) *InvitationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *InvitationsListCall) IfNoneMatch(entityTag string) *InvitationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *InvitationsListCall) Context(ctx context.Context) *InvitationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *InvitationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *InvitationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/invitations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.invitations.list" call.
// Exactly one of *ListInvitationsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListInvitationsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *InvitationsListCall) Do(opts ...googleapi.CallOption) (*ListInvitationsResponse, error) {
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
	ret := &ListInvitationsResponse{
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
	//   "description": "Returns a list of invitations that the requesting user is permitted to view, restricted to those that match the list request. *Note:* At least one of `user_id` or `course_id` must be supplied. Both fields can be supplied. This method returns the following error codes: * `PERMISSION_DENIED` for access errors.",
	//   "httpMethod": "GET",
	//   "id": "classroom.invitations.list",
	//   "parameters": {
	//     "courseId": {
	//       "description": "Restricts returned invitations to those for a course with the specified identifier.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero means no maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "Restricts returned invitations to those for a specific user. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/invitations",
	//   "response": {
	//     "$ref": "ListInvitationsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *InvitationsListCall) Pages(ctx context.Context, f func(*ListInvitationsResponse) error) error {
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

// method id "classroom.userProfiles.get":

type UserProfilesGetCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a user profile. This method returns the following error
// codes: * `PERMISSION_DENIED` if the requesting user is not permitted
// to access this user profile or if no profile exists with the
// requested ID or for access errors.
func (r *UserProfilesService) Get(userId string) *UserProfilesGetCall {
	c := &UserProfilesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGetCall) Fields(s ...googleapi.Field) *UserProfilesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UserProfilesGetCall) IfNoneMatch(entityTag string) *UserProfilesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGetCall) Context(ctx context.Context) *UserProfilesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{userId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.get" call.
// Exactly one of *UserProfile or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *UserProfile.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UserProfilesGetCall) Do(opts ...googleapi.CallOption) (*UserProfile, error) {
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
	ret := &UserProfile{
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
	//   "description": "Returns a user profile. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to access this user profile or if no profile exists with the requested ID or for access errors.",
	//   "httpMethod": "GET",
	//   "id": "classroom.userProfiles.get",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "userId": {
	//       "description": "Identifier of the profile to return. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{userId}",
	//   "response": {
	//     "$ref": "UserProfile"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/classroom.profile.emails",
	//     "https://www.googleapis.com/auth/classroom.profile.photos",
	//     "https://www.googleapis.com/auth/classroom.rosters",
	//     "https://www.googleapis.com/auth/classroom.rosters.readonly"
	//   ]
	// }

}

// method id "classroom.userProfiles.guardianInvitations.create":

type UserProfilesGuardianInvitationsCreateCall struct {
	s                  *Service
	studentId          string
	guardianinvitation *GuardianInvitation
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Create: Creates a guardian invitation, and sends an email to the
// guardian asking them to confirm that they are the student's guardian.
// Once the guardian accepts the invitation, their `state` will change
// to `COMPLETED` and they will start receiving guardian notifications.
// A `Guardian` resource will also be created to represent the active
// guardian. The request object must have the `student_id` and
// `invited_email_address` fields set. Failing to set these fields, or
// setting any other fields in the request, will result in an error.
// This method returns the following error codes: * `PERMISSION_DENIED`
// if the current user does not have permission to manage guardians, if
// the guardian in question has already rejected too many requests for
// that student, if guardians are not enabled for the domain in
// question, or for other access errors. * `RESOURCE_EXHAUSTED` if the
// student or guardian has exceeded the guardian link limit. *
// `INVALID_ARGUMENT` if the guardian email address is not valid (for
// example, if it is too long), or if the format of the student ID
// provided cannot be recognized (it is not an email address, nor a
// `user_id` from this API). This error will also be returned if
// read-only fields are set, or if the `state` field is set to to a
// value other than `PENDING`. * `NOT_FOUND` if the student ID provided
// is a valid student ID, but Classroom has no record of that student. *
// `ALREADY_EXISTS` if there is already a pending guardian invitation
// for the student and `invited_email_address` provided, or if the
// provided `invited_email_address` matches the Google account of an
// existing `Guardian` for this user.
func (r *UserProfilesGuardianInvitationsService) Create(studentId string, guardianinvitation *GuardianInvitation) *UserProfilesGuardianInvitationsCreateCall {
	c := &UserProfilesGuardianInvitationsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	c.guardianinvitation = guardianinvitation
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardianInvitationsCreateCall) Fields(s ...googleapi.Field) *UserProfilesGuardianInvitationsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardianInvitationsCreateCall) Context(ctx context.Context) *UserProfilesGuardianInvitationsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardianInvitationsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardianInvitationsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.guardianinvitation)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardianInvitations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId": c.studentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardianInvitations.create" call.
// Exactly one of *GuardianInvitation or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GuardianInvitation.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UserProfilesGuardianInvitationsCreateCall) Do(opts ...googleapi.CallOption) (*GuardianInvitation, error) {
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
	ret := &GuardianInvitation{
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
	//   "description": "Creates a guardian invitation, and sends an email to the guardian asking them to confirm that they are the student's guardian. Once the guardian accepts the invitation, their `state` will change to `COMPLETED` and they will start receiving guardian notifications. A `Guardian` resource will also be created to represent the active guardian. The request object must have the `student_id` and `invited_email_address` fields set. Failing to set these fields, or setting any other fields in the request, will result in an error. This method returns the following error codes: * `PERMISSION_DENIED` if the current user does not have permission to manage guardians, if the guardian in question has already rejected too many requests for that student, if guardians are not enabled for the domain in question, or for other access errors. * `RESOURCE_EXHAUSTED` if the student or guardian has exceeded the guardian link limit. * `INVALID_ARGUMENT` if the guardian email address is not valid (for example, if it is too long), or if the format of the student ID provided cannot be recognized (it is not an email address, nor a `user_id` from this API). This error will also be returned if read-only fields are set, or if the `state` field is set to to a value other than `PENDING`. * `NOT_FOUND` if the student ID provided is a valid student ID, but Classroom has no record of that student. * `ALREADY_EXISTS` if there is already a pending guardian invitation for the student and `invited_email_address` provided, or if the provided `invited_email_address` matches the Google account of an existing `Guardian` for this user.",
	//   "httpMethod": "POST",
	//   "id": "classroom.userProfiles.guardianInvitations.create",
	//   "parameterOrder": [
	//     "studentId"
	//   ],
	//   "parameters": {
	//     "studentId": {
	//       "description": "ID of the student (in standard format)",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardianInvitations",
	//   "request": {
	//     "$ref": "GuardianInvitation"
	//   },
	//   "response": {
	//     "$ref": "GuardianInvitation"
	//   }
	// }

}

// method id "classroom.userProfiles.guardianInvitations.get":

type UserProfilesGuardianInvitationsGetCall struct {
	s            *Service
	studentId    string
	invitationId string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a specific guardian invitation. This method returns the
// following error codes: * `PERMISSION_DENIED` if the requesting user
// is not permitted to view guardian invitations for the student
// identified by the `student_id`, if guardians are not enabled for the
// domain in question, or for other access errors. * `INVALID_ARGUMENT`
// if a `student_id` is specified, but its format cannot be recognized
// (it is not an email address, nor a `student_id` from the API, nor the
// literal string `me`). * `NOT_FOUND` if Classroom cannot find any
// record of the given student or `invitation_id`. May also be returned
// if the student exists, but the requesting user does not have access
// to see that student.
func (r *UserProfilesGuardianInvitationsService) Get(studentId string, invitationId string) *UserProfilesGuardianInvitationsGetCall {
	c := &UserProfilesGuardianInvitationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	c.invitationId = invitationId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardianInvitationsGetCall) Fields(s ...googleapi.Field) *UserProfilesGuardianInvitationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UserProfilesGuardianInvitationsGetCall) IfNoneMatch(entityTag string) *UserProfilesGuardianInvitationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardianInvitationsGetCall) Context(ctx context.Context) *UserProfilesGuardianInvitationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardianInvitationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardianInvitationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardianInvitations/{invitationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId":    c.studentId,
		"invitationId": c.invitationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardianInvitations.get" call.
// Exactly one of *GuardianInvitation or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GuardianInvitation.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UserProfilesGuardianInvitationsGetCall) Do(opts ...googleapi.CallOption) (*GuardianInvitation, error) {
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
	ret := &GuardianInvitation{
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
	//   "description": "Returns a specific guardian invitation. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to view guardian invitations for the student identified by the `student_id`, if guardians are not enabled for the domain in question, or for other access errors. * `INVALID_ARGUMENT` if a `student_id` is specified, but its format cannot be recognized (it is not an email address, nor a `student_id` from the API, nor the literal string `me`). * `NOT_FOUND` if Classroom cannot find any record of the given student or `invitation_id`. May also be returned if the student exists, but the requesting user does not have access to see that student.",
	//   "httpMethod": "GET",
	//   "id": "classroom.userProfiles.guardianInvitations.get",
	//   "parameterOrder": [
	//     "studentId",
	//     "invitationId"
	//   ],
	//   "parameters": {
	//     "invitationId": {
	//       "description": "The `id` field of the `GuardianInvitation` being requested.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "The ID of the student whose guardian invitation is being requested.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardianInvitations/{invitationId}",
	//   "response": {
	//     "$ref": "GuardianInvitation"
	//   }
	// }

}

// method id "classroom.userProfiles.guardianInvitations.list":

type UserProfilesGuardianInvitationsListCall struct {
	s            *Service
	studentId    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of guardian invitations that the requesting user
// is permitted to view, filtered by the parameters provided. This
// method returns the following error codes: * `PERMISSION_DENIED` if a
// `student_id` is specified, and the requesting user is not permitted
// to view guardian invitations for that student, if "-" is specified
// as the `student_id` and the user is not a domain administrator, if
// guardians are not enabled for the domain in question, or for other
// access errors. * `INVALID_ARGUMENT` if a `student_id` is specified,
// but its format cannot be recognized (it is not an email address, nor
// a `student_id` from the API, nor the literal string `me`). May also
// be returned if an invalid `page_token` or `state` is provided. *
// `NOT_FOUND` if a `student_id` is specified, and its format can be
// recognized, but Classroom has no record of that student.
func (r *UserProfilesGuardianInvitationsService) List(studentId string) *UserProfilesGuardianInvitationsListCall {
	c := &UserProfilesGuardianInvitationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	return c
}

// InvitedEmailAddress sets the optional parameter
// "invitedEmailAddress": If specified, only results with the specified
// `invited_email_address` will be returned.
func (c *UserProfilesGuardianInvitationsListCall) InvitedEmailAddress(invitedEmailAddress string) *UserProfilesGuardianInvitationsListCall {
	c.urlParams_.Set("invitedEmailAddress", invitedEmailAddress)
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero or unspecified indicates that the server may
// assign a maximum. The server may return fewer than the specified
// number of results.
func (c *UserProfilesGuardianInvitationsListCall) PageSize(pageSize int64) *UserProfilesGuardianInvitationsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *UserProfilesGuardianInvitationsListCall) PageToken(pageToken string) *UserProfilesGuardianInvitationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// States sets the optional parameter "states": If specified, only
// results with the specified `state` values will be returned.
// Otherwise, results with a `state` of `PENDING` will be returned.
//
// Possible values:
//   "GUARDIAN_INVITATION_STATE_UNSPECIFIED"
//   "PENDING"
//   "COMPLETE"
func (c *UserProfilesGuardianInvitationsListCall) States(states ...string) *UserProfilesGuardianInvitationsListCall {
	c.urlParams_.SetMulti("states", append([]string{}, states...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardianInvitationsListCall) Fields(s ...googleapi.Field) *UserProfilesGuardianInvitationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UserProfilesGuardianInvitationsListCall) IfNoneMatch(entityTag string) *UserProfilesGuardianInvitationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardianInvitationsListCall) Context(ctx context.Context) *UserProfilesGuardianInvitationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardianInvitationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardianInvitationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardianInvitations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId": c.studentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardianInvitations.list" call.
// Exactly one of *ListGuardianInvitationsResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *ListGuardianInvitationsResponse.ServerResponse.Header or (if
// a response was returned at all) in error.(*googleapi.Error).Header.
// Use googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UserProfilesGuardianInvitationsListCall) Do(opts ...googleapi.CallOption) (*ListGuardianInvitationsResponse, error) {
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
	ret := &ListGuardianInvitationsResponse{
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
	//   "description": "Returns a list of guardian invitations that the requesting user is permitted to view, filtered by the parameters provided. This method returns the following error codes: * `PERMISSION_DENIED` if a `student_id` is specified, and the requesting user is not permitted to view guardian invitations for that student, if `\"-\"` is specified as the `student_id` and the user is not a domain administrator, if guardians are not enabled for the domain in question, or for other access errors. * `INVALID_ARGUMENT` if a `student_id` is specified, but its format cannot be recognized (it is not an email address, nor a `student_id` from the API, nor the literal string `me`). May also be returned if an invalid `page_token` or `state` is provided. * `NOT_FOUND` if a `student_id` is specified, and its format can be recognized, but Classroom has no record of that student.",
	//   "httpMethod": "GET",
	//   "id": "classroom.userProfiles.guardianInvitations.list",
	//   "parameterOrder": [
	//     "studentId"
	//   ],
	//   "parameters": {
	//     "invitedEmailAddress": {
	//       "description": "If specified, only results with the specified `invited_email_address` will be returned.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero or unspecified indicates that the server may assign a maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "states": {
	//       "description": "If specified, only results with the specified `state` values will be returned. Otherwise, results with a `state` of `PENDING` will be returned.",
	//       "enum": [
	//         "GUARDIAN_INVITATION_STATE_UNSPECIFIED",
	//         "PENDING",
	//         "COMPLETE"
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "The ID of the student whose guardian invitations are to be returned. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user * the string literal `\"-\"`, indicating that results should be returned for all students that the requesting user is permitted to view guardian invitations.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardianInvitations",
	//   "response": {
	//     "$ref": "ListGuardianInvitationsResponse"
	//   }
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UserProfilesGuardianInvitationsListCall) Pages(ctx context.Context, f func(*ListGuardianInvitationsResponse) error) error {
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

// method id "classroom.userProfiles.guardianInvitations.patch":

type UserProfilesGuardianInvitationsPatchCall struct {
	s                  *Service
	studentId          string
	invitationId       string
	guardianinvitation *GuardianInvitation
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Patch: Modifies a guardian invitation. Currently, the only valid
// modification is to change the `state` from `PENDING` to `COMPLETE`.
// This has the effect of withdrawing the invitation. This method
// returns the following error codes: * `PERMISSION_DENIED` if the
// current user does not have permission to manage guardians, if
// guardians are not enabled for the domain in question or for other
// access errors. * `FAILED_PRECONDITION` if the guardian link is not in
// the `PENDING` state. * `INVALID_ARGUMENT` if the format of the
// student ID provided cannot be recognized (it is not an email address,
// nor a `user_id` from this API), or if the passed `GuardianInvitation`
// has a `state` other than `COMPLETE`, or if it modifies fields other
// than `state`. * `NOT_FOUND` if the student ID provided is a valid
// student ID, but Classroom has no record of that student, or if the
// `id` field does not refer to a guardian invitation known to
// Classroom.
func (r *UserProfilesGuardianInvitationsService) Patch(studentId string, invitationId string, guardianinvitation *GuardianInvitation) *UserProfilesGuardianInvitationsPatchCall {
	c := &UserProfilesGuardianInvitationsPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	c.invitationId = invitationId
	c.guardianinvitation = guardianinvitation
	return c
}

// UpdateMask sets the optional parameter "updateMask": Mask that
// identifies which fields on the course to update. This field is
// required to do an update. The update will fail if invalid fields are
// specified. The following fields are valid: * `state` When set in a
// query parameter, this field should be specified as `updateMask=,,...`
func (c *UserProfilesGuardianInvitationsPatchCall) UpdateMask(updateMask string) *UserProfilesGuardianInvitationsPatchCall {
	c.urlParams_.Set("updateMask", updateMask)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardianInvitationsPatchCall) Fields(s ...googleapi.Field) *UserProfilesGuardianInvitationsPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardianInvitationsPatchCall) Context(ctx context.Context) *UserProfilesGuardianInvitationsPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardianInvitationsPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardianInvitationsPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.guardianinvitation)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardianInvitations/{invitationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId":    c.studentId,
		"invitationId": c.invitationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardianInvitations.patch" call.
// Exactly one of *GuardianInvitation or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GuardianInvitation.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UserProfilesGuardianInvitationsPatchCall) Do(opts ...googleapi.CallOption) (*GuardianInvitation, error) {
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
	ret := &GuardianInvitation{
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
	//   "description": "Modifies a guardian invitation. Currently, the only valid modification is to change the `state` from `PENDING` to `COMPLETE`. This has the effect of withdrawing the invitation. This method returns the following error codes: * `PERMISSION_DENIED` if the current user does not have permission to manage guardians, if guardians are not enabled for the domain in question or for other access errors. * `FAILED_PRECONDITION` if the guardian link is not in the `PENDING` state. * `INVALID_ARGUMENT` if the format of the student ID provided cannot be recognized (it is not an email address, nor a `user_id` from this API), or if the passed `GuardianInvitation` has a `state` other than `COMPLETE`, or if it modifies fields other than `state`. * `NOT_FOUND` if the student ID provided is a valid student ID, but Classroom has no record of that student, or if the `id` field does not refer to a guardian invitation known to Classroom.",
	//   "httpMethod": "PATCH",
	//   "id": "classroom.userProfiles.guardianInvitations.patch",
	//   "parameterOrder": [
	//     "studentId",
	//     "invitationId"
	//   ],
	//   "parameters": {
	//     "invitationId": {
	//       "description": "The `id` field of the `GuardianInvitation` to be modified.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "The ID of the student whose guardian invitation is to be modified.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "updateMask": {
	//       "description": "Mask that identifies which fields on the course to update. This field is required to do an update. The update will fail if invalid fields are specified. The following fields are valid: * `state` When set in a query parameter, this field should be specified as `updateMask=,,...`",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardianInvitations/{invitationId}",
	//   "request": {
	//     "$ref": "GuardianInvitation"
	//   },
	//   "response": {
	//     "$ref": "GuardianInvitation"
	//   }
	// }

}

// method id "classroom.userProfiles.guardians.delete":

type UserProfilesGuardiansDeleteCall struct {
	s          *Service
	studentId  string
	guardianId string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a guardian. The guardian will no longer receive
// guardian notifications and the guardian will no longer be accessible
// via the API. This method returns the following error codes: *
// `PERMISSION_DENIED` if the requesting user is not permitted to manage
// guardians for the student identified by the `student_id`, if
// guardians are not enabled for the domain in question, or for other
// access errors. * `INVALID_ARGUMENT` if a `student_id` is specified,
// but its format cannot be recognized (it is not an email address, nor
// a `student_id` from the API). * `NOT_FOUND` if Classroom cannot find
// any record of the given `student_id` or `guardian_id`, or if the
// guardian has already been disabled.
func (r *UserProfilesGuardiansService) Delete(studentId string, guardianId string) *UserProfilesGuardiansDeleteCall {
	c := &UserProfilesGuardiansDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	c.guardianId = guardianId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardiansDeleteCall) Fields(s ...googleapi.Field) *UserProfilesGuardiansDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardiansDeleteCall) Context(ctx context.Context) *UserProfilesGuardiansDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardiansDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardiansDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardians/{guardianId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId":  c.studentId,
		"guardianId": c.guardianId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardians.delete" call.
// Exactly one of *Empty or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Empty.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *UserProfilesGuardiansDeleteCall) Do(opts ...googleapi.CallOption) (*Empty, error) {
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
	//   "description": "Deletes a guardian. The guardian will no longer receive guardian notifications and the guardian will no longer be accessible via the API. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to manage guardians for the student identified by the `student_id`, if guardians are not enabled for the domain in question, or for other access errors. * `INVALID_ARGUMENT` if a `student_id` is specified, but its format cannot be recognized (it is not an email address, nor a `student_id` from the API). * `NOT_FOUND` if Classroom cannot find any record of the given `student_id` or `guardian_id`, or if the guardian has already been disabled.",
	//   "httpMethod": "DELETE",
	//   "id": "classroom.userProfiles.guardians.delete",
	//   "parameterOrder": [
	//     "studentId",
	//     "guardianId"
	//   ],
	//   "parameters": {
	//     "guardianId": {
	//       "description": "The `id` field from a `Guardian`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "The student whose guardian is to be deleted. One of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardians/{guardianId}",
	//   "response": {
	//     "$ref": "Empty"
	//   }
	// }

}

// method id "classroom.userProfiles.guardians.get":

type UserProfilesGuardiansGetCall struct {
	s            *Service
	studentId    string
	guardianId   string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a specific guardian. This method returns the following
// error codes: * `PERMISSION_DENIED` if the requesting user is not
// permitted to view guardian information for the student identified by
// the `student_id`, if guardians are not enabled for the domain in
// question, or for other access errors. * `INVALID_ARGUMENT` if a
// `student_id` is specified, but its format cannot be recognized (it is
// not an email address, nor a `student_id` from the API, nor the
// literal string `me`). * `NOT_FOUND` if Classroom cannot find any
// record of the given student or `guardian_id`, or if the guardian has
// been disabled.
func (r *UserProfilesGuardiansService) Get(studentId string, guardianId string) *UserProfilesGuardiansGetCall {
	c := &UserProfilesGuardiansGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	c.guardianId = guardianId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardiansGetCall) Fields(s ...googleapi.Field) *UserProfilesGuardiansGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UserProfilesGuardiansGetCall) IfNoneMatch(entityTag string) *UserProfilesGuardiansGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardiansGetCall) Context(ctx context.Context) *UserProfilesGuardiansGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardiansGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardiansGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardians/{guardianId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId":  c.studentId,
		"guardianId": c.guardianId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardians.get" call.
// Exactly one of *Guardian or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Guardian.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *UserProfilesGuardiansGetCall) Do(opts ...googleapi.CallOption) (*Guardian, error) {
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
	ret := &Guardian{
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
	//   "description": "Returns a specific guardian. This method returns the following error codes: * `PERMISSION_DENIED` if the requesting user is not permitted to view guardian information for the student identified by the `student_id`, if guardians are not enabled for the domain in question, or for other access errors. * `INVALID_ARGUMENT` if a `student_id` is specified, but its format cannot be recognized (it is not an email address, nor a `student_id` from the API, nor the literal string `me`). * `NOT_FOUND` if Classroom cannot find any record of the given student or `guardian_id`, or if the guardian has been disabled.",
	//   "httpMethod": "GET",
	//   "id": "classroom.userProfiles.guardians.get",
	//   "parameterOrder": [
	//     "studentId",
	//     "guardianId"
	//   ],
	//   "parameters": {
	//     "guardianId": {
	//       "description": "The `id` field from a `Guardian`.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "The student whose guardian is being requested. One of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardians/{guardianId}",
	//   "response": {
	//     "$ref": "Guardian"
	//   }
	// }

}

// method id "classroom.userProfiles.guardians.list":

type UserProfilesGuardiansListCall struct {
	s            *Service
	studentId    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Returns a list of guardians that the requesting user is
// permitted to view, restricted to those that match the request. To
// list guardians for any student that the requesting user may view
// guardians for, use the literal character `-` for the student ID. This
// method returns the following error codes: * `PERMISSION_DENIED` if a
// `student_id` is specified, and the requesting user is not permitted
// to view guardian information for that student, if "-" is specified
// as the `student_id` and the user is not a domain administrator, if
// guardians are not enabled for the domain in question, if the
// `invited_email_address` filter is set by a user who is not a domain
// administrator, or for other access errors. * `INVALID_ARGUMENT` if a
// `student_id` is specified, but its format cannot be recognized (it is
// not an email address, nor a `student_id` from the API, nor the
// literal string `me`). May also be returned if an invalid `page_token`
// is provided. * `NOT_FOUND` if a `student_id` is specified, and its
// format can be recognized, but Classroom has no record of that
// student.
func (r *UserProfilesGuardiansService) List(studentId string) *UserProfilesGuardiansListCall {
	c := &UserProfilesGuardiansListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.studentId = studentId
	return c
}

// InvitedEmailAddress sets the optional parameter
// "invitedEmailAddress": Filter results by the email address that the
// original invitation was sent to, resulting in this guardian link.
// This filter can only be used by domain administrators.
func (c *UserProfilesGuardiansListCall) InvitedEmailAddress(invitedEmailAddress string) *UserProfilesGuardiansListCall {
	c.urlParams_.Set("invitedEmailAddress", invitedEmailAddress)
	return c
}

// PageSize sets the optional parameter "pageSize": Maximum number of
// items to return. Zero or unspecified indicates that the server may
// assign a maximum. The server may return fewer than the specified
// number of results.
func (c *UserProfilesGuardiansListCall) PageSize(pageSize int64) *UserProfilesGuardiansListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": nextPageToken
// value returned from a previous list call, indicating that the
// subsequent page of results should be returned. The list request must
// be otherwise identical to the one that resulted in this token.
func (c *UserProfilesGuardiansListCall) PageToken(pageToken string) *UserProfilesGuardiansListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *UserProfilesGuardiansListCall) Fields(s ...googleapi.Field) *UserProfilesGuardiansListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *UserProfilesGuardiansListCall) IfNoneMatch(entityTag string) *UserProfilesGuardiansListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *UserProfilesGuardiansListCall) Context(ctx context.Context) *UserProfilesGuardiansListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *UserProfilesGuardiansListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *UserProfilesGuardiansListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/userProfiles/{studentId}/guardians")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"studentId": c.studentId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "classroom.userProfiles.guardians.list" call.
// Exactly one of *ListGuardiansResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListGuardiansResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *UserProfilesGuardiansListCall) Do(opts ...googleapi.CallOption) (*ListGuardiansResponse, error) {
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
	ret := &ListGuardiansResponse{
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
	//   "description": "Returns a list of guardians that the requesting user is permitted to view, restricted to those that match the request. To list guardians for any student that the requesting user may view guardians for, use the literal character `-` for the student ID. This method returns the following error codes: * `PERMISSION_DENIED` if a `student_id` is specified, and the requesting user is not permitted to view guardian information for that student, if `\"-\"` is specified as the `student_id` and the user is not a domain administrator, if guardians are not enabled for the domain in question, if the `invited_email_address` filter is set by a user who is not a domain administrator, or for other access errors. * `INVALID_ARGUMENT` if a `student_id` is specified, but its format cannot be recognized (it is not an email address, nor a `student_id` from the API, nor the literal string `me`). May also be returned if an invalid `page_token` is provided. * `NOT_FOUND` if a `student_id` is specified, and its format can be recognized, but Classroom has no record of that student.",
	//   "httpMethod": "GET",
	//   "id": "classroom.userProfiles.guardians.list",
	//   "parameterOrder": [
	//     "studentId"
	//   ],
	//   "parameters": {
	//     "invitedEmailAddress": {
	//       "description": "Filter results by the email address that the original invitation was sent to, resulting in this guardian link. This filter can only be used by domain administrators.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Maximum number of items to return. Zero or unspecified indicates that the server may assign a maximum. The server may return fewer than the specified number of results.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "nextPageToken value returned from a previous list call, indicating that the subsequent page of results should be returned. The list request must be otherwise identical to the one that resulted in this token.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "studentId": {
	//       "description": "Filter results by the student who the guardian is linked to. The identifier can be one of the following: * the numeric identifier for the user * the email address of the user * the string literal `\"me\"`, indicating the requesting user * the string literal `\"-\"`, indicating that results should be returned for all students that the requesting user has access to view.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/userProfiles/{studentId}/guardians",
	//   "response": {
	//     "$ref": "ListGuardiansResponse"
	//   }
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *UserProfilesGuardiansListCall) Pages(ctx context.Context, f func(*ListGuardiansResponse) error) error {
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

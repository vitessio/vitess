package mysqlctl

import (
	"os"
	"testing"
)

type testcase struct {
	versionString string
	version       serverVersion
	flavor        mysqlFlavor
}

func TestParseVersionString(t *testing.T) {

	var testcases = []testcase{

		{
			versionString: "mysqld  Ver 5.7.27-0ubuntu0.19.04.1 for Linux on x86_64 ((Ubuntu))",
			version:       serverVersion{5, 7, 27},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.6.43 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			version:       serverVersion{5, 6, 43},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.7.26 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			version:       serverVersion{5, 7, 26},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 8.0.16 for linux-glibc2.12 on x86_64 (MySQL Community Server - GPL)",
			version:       serverVersion{8, 0, 16},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.7.26-29 for Linux on x86_64 (Percona Server (GPL), Release 29, Revision 11ad961)",
			version:       serverVersion{5, 7, 26},
			flavor:        FlavorPercona,
		},
		{
			versionString: "mysqld  Ver 10.0.38-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       serverVersion{10, 0, 38},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.1.40-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       serverVersion{10, 1, 40},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.2.25-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       serverVersion{10, 2, 25},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.3.16-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       serverVersion{10, 3, 16},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.4.6-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       serverVersion{10, 4, 6},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 5.6.42 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			version:       serverVersion{5, 6, 42},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.6.44-86.0 for Linux on x86_64 (Percona Server (GPL), Release 86.0, Revision eba1b3f)",
			version:       serverVersion{5, 6, 44},
			flavor:        FlavorPercona,
		},
		{
			versionString: "mysqld  Ver 8.0.15-6 for Linux on x86_64 (Percona Server (GPL), Release 6, Revision 63abd08)",
			version:       serverVersion{8, 0, 15},
			flavor:        FlavorPercona,
		},
	}

	for _, testcase := range testcases {
		f, v, err := ParseVersionString(testcase.versionString)
		if v != testcase.version || f != testcase.flavor || err != nil {
			t.Errorf("ParseVersionString failed for: %#v, Got: %#v, %#v Expected: %#v, %#v", testcase.versionString, v, f, testcase.version, testcase.flavor)
		}
	}

}

func TestAssumeVersionString(t *testing.T) {

	// In these cases, the versionstring is nonsensical or unspecified.
	// MYSQL_FLAVOR is used instead.

	var testcases = []testcase{
		{
			versionString: "MySQL80",
			version:       serverVersion{8, 0, 11},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "MySQL56",
			version:       serverVersion{5, 7, 10}, // Yes, this has to lie!
			flavor:        FlavorMySQL,             // There was no MySQL57 option
		},
		{
			versionString: "MariaDB",
			version:       serverVersion{10, 0, 10},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "MariaDB103",
			version:       serverVersion{10, 3, 7},
			flavor:        FlavorMariaDB,
		},
	}

	for _, testcase := range testcases {
		os.Setenv("MYSQL_FLAVOR", testcase.versionString)
		f, v, err := GetVersionFromEnv()
		if v != testcase.version || f != testcase.flavor || err != nil {
			t.Errorf("GetVersionFromEnv() failed for: %#v, Got: %#v, %#v Expected: %#v, %#v", testcase.versionString, v, f, testcase.version, testcase.flavor)
		}
	}

}

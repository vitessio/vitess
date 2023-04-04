package datetime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTimeZone(t *testing.T) {
	testCases := []struct {
		tz   string
		want string
	}{
		{
			tz:   "Europe/Amsterdam",
			want: "Europe/Amsterdam",
		},
		{
			tz:   "",
			want: "Unknown or incorrect time zone: ''",
		},
		{
			tz:   "+02:00",
			want: "UTC+02:00",
		},
		{
			tz:   "+14:00",
			want: "UTC+14:00",
		},
		{
			tz:   "+14:01",
			want: "Unknown or incorrect time zone: '+14:01'",
		},
		{
			tz:   "-13:59",
			want: "UTC-13:59",
		},
		{
			tz:   "-14:00",
			want: "Unknown or incorrect time zone: '-14:00'",
		},
		{
			tz:   "-15:00",
			want: "Unknown or incorrect time zone: '-15:00'",
		},
		{
			tz:   "foo",
			want: "Unknown or incorrect time zone: 'foo'",
		},
	}

	for _, tc := range testCases {

		zone, err := ParseTimeZone(tc.tz)
		if err != nil {
			assert.Equal(t, tc.want, err.Error())
		} else {
			assert.Equal(t, tc.want, zone.String())
		}
	}
}

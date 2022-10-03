package inst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:staticcheck
func TestParseTag(t *testing.T) {
	{
		tag, err := ParseTag("")
		require.True(t, tag == nil)
		require.Error(t, err)
	}
	{
		tag, err := ParseTag("=")
		require.True(t, tag == nil)
		require.Error(t, err)
	}
	{
		tag, err := ParseTag("=backup")
		require.True(t, tag == nil)
		require.Error(t, err)
	}
	{
		tag, err := ParseTag("  =backup")
		require.True(t, tag == nil)
		require.Error(t, err)
	}
	{
		tag, err := ParseTag("role")
		require.NoError(t, err)
		require.True(t, tag != nil)
		require.Equal(t, tag.TagName, "role")
		require.Equal(t, tag.TagValue, "")
		require.False(t, tag.Negate)
		require.False(t, tag.HasValue)

		require.Equal(t, tag.String(), "role=")
	}
	{
		tag, err := ParseTag("role=")
		require.NoError(t, err)
		require.True(t, tag != nil)
		require.Equal(t, tag.TagName, "role")
		require.Equal(t, tag.TagValue, "")
		require.False(t, tag.Negate)
		require.True(t, tag.HasValue)

		require.Equal(t, tag.String(), "role=")

	}
	{
		tag, err := ParseTag("role=backup")
		require.NoError(t, err)
		require.True(t, tag != nil)
		require.Equal(t, tag.TagName, "role")
		require.Equal(t, tag.TagValue, "backup")
		require.False(t, tag.Negate)
		require.True(t, tag.HasValue)

		require.Equal(t, tag.String(), "role=backup")
	}
	{
		tag, err := ParseTag("!role")
		require.NoError(t, err)
		require.True(t, tag != nil)
		require.Equal(t, tag.TagName, "role")
		require.True(t, tag.Negate)
		require.False(t, tag.HasValue)
	}
	{
		tag, err := ParseTag("~role=backup")
		require.NoError(t, err)
		require.True(t, tag != nil)
		require.Equal(t, tag.TagName, "role")
		require.Equal(t, tag.TagValue, "backup")
		require.True(t, tag.Negate)
		require.True(t, tag.HasValue)
	}
}

func TestParseIntersectTags(t *testing.T) {
	{
		_, err := ParseIntersectTags("")
		require.Error(t, err)
	}
	{
		_, err := ParseIntersectTags(",")
		require.Error(t, err)
	}
	{
		_, err := ParseIntersectTags(",,,")
		require.Error(t, err)
	}
	{
		_, err := ParseIntersectTags("role,")
		require.Error(t, err)
	}
	{
		tags, err := ParseIntersectTags("role")
		require.NoError(t, err)
		require.Equal(t, len(tags), 1)

		require.Equal(t, tags[0].TagName, "role")
		require.Equal(t, tags[0].TagValue, "")
		require.False(t, tags[0].Negate)
		require.False(t, tags[0].HasValue)
	}
	{
		tags, err := ParseIntersectTags("role,dc")
		require.NoError(t, err)
		require.Equal(t, len(tags), 2)

		require.Equal(t, tags[0].TagName, "role")
		require.Equal(t, tags[0].TagValue, "")
		require.False(t, tags[0].Negate)
		require.False(t, tags[0].HasValue)

		require.Equal(t, tags[1].TagName, "dc")
		require.Equal(t, tags[1].TagValue, "")
		require.False(t, tags[1].Negate)
		require.False(t, tags[1].HasValue)
	}
	{
		tags, err := ParseIntersectTags("role=backup, !dc=ny")
		require.NoError(t, err)
		require.Equal(t, len(tags), 2)

		require.Equal(t, tags[0].TagName, "role")
		require.Equal(t, tags[0].TagValue, "backup")
		require.False(t, tags[0].Negate)
		require.True(t, tags[0].HasValue)

		require.Equal(t, tags[1].TagName, "dc")
		require.Equal(t, tags[1].TagValue, "ny")
		require.True(t, tags[1].Negate)
		require.True(t, tags[1].HasValue)
	}
}

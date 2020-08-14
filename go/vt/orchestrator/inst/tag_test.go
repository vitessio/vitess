package inst

import (
	"testing"

	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

//nolint:staticcheck
func TestParseTag(t *testing.T) {
	{
		tag, err := ParseTag("")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := ParseTag("=")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := ParseTag("=backup")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := ParseTag("  =backup")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := ParseTag("role")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "")
		test.S(t).ExpectFalse(tag.Negate)
		test.S(t).ExpectFalse(tag.HasValue)

		test.S(t).ExpectEquals(tag.String(), "role=")
	}
	{
		tag, err := ParseTag("role=")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "")
		test.S(t).ExpectFalse(tag.Negate)
		test.S(t).ExpectTrue(tag.HasValue)

		test.S(t).ExpectEquals(tag.String(), "role=")

	}
	{
		tag, err := ParseTag("role=backup")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "backup")
		test.S(t).ExpectFalse(tag.Negate)
		test.S(t).ExpectTrue(tag.HasValue)

		test.S(t).ExpectEquals(tag.String(), "role=backup")
	}
	{
		tag, err := ParseTag("!role")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectTrue(tag.Negate)
		test.S(t).ExpectFalse(tag.HasValue)
	}
	{
		tag, err := ParseTag("~role=backup")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "backup")
		test.S(t).ExpectTrue(tag.Negate)
		test.S(t).ExpectTrue(tag.HasValue)
	}
}

func TestParseIntersectTags(t *testing.T) {
	{
		_, err := ParseIntersectTags("")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseIntersectTags(",")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseIntersectTags(",,,")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseIntersectTags("role,")
		test.S(t).ExpectNotNil(err)
	}
	{
		tags, err := ParseIntersectTags("role")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(tags), 1)

		test.S(t).ExpectEquals(tags[0].TagName, "role")
		test.S(t).ExpectEquals(tags[0].TagValue, "")
		test.S(t).ExpectFalse(tags[0].Negate)
		test.S(t).ExpectFalse(tags[0].HasValue)
	}
	{
		tags, err := ParseIntersectTags("role,dc")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(tags), 2)

		test.S(t).ExpectEquals(tags[0].TagName, "role")
		test.S(t).ExpectEquals(tags[0].TagValue, "")
		test.S(t).ExpectFalse(tags[0].Negate)
		test.S(t).ExpectFalse(tags[0].HasValue)

		test.S(t).ExpectEquals(tags[1].TagName, "dc")
		test.S(t).ExpectEquals(tags[1].TagValue, "")
		test.S(t).ExpectFalse(tags[1].Negate)
		test.S(t).ExpectFalse(tags[1].HasValue)
	}
	{
		tags, err := ParseIntersectTags("role=backup, !dc=ny")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(tags), 2)

		test.S(t).ExpectEquals(tags[0].TagName, "role")
		test.S(t).ExpectEquals(tags[0].TagValue, "backup")
		test.S(t).ExpectFalse(tags[0].Negate)
		test.S(t).ExpectTrue(tags[0].HasValue)

		test.S(t).ExpectEquals(tags[1].TagName, "dc")
		test.S(t).ExpectEquals(tags[1].TagValue, "ny")
		test.S(t).ExpectTrue(tags[1].Negate)
		test.S(t).ExpectTrue(tags[1].HasValue)
	}
}

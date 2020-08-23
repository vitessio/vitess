/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package inst

import (
	"fmt"
	"regexp"
	"strings"
)

type Tag struct {
	TagName  string
	TagValue string
	HasValue bool
	Negate   bool
}

var (
	negateTagEqualsRegexp = regexp.MustCompile("^~([^=]+)=(.*)$")
	TagEqualsRegexp       = regexp.MustCompile("^([^=]+)=(.*)$")
	negateTagExistsRegexp = regexp.MustCompile("^~([^=]+)$")
	tagExistsRegexp       = regexp.MustCompile("^([^=]+)$")
)

func NewTag(tagName string, tagValue string) (*Tag, error) {
	tagName = strings.TrimSpace(tagName)
	if tagName == "" {
		return nil, fmt.Errorf("NewTag: empty tag name")
	}
	return &Tag{TagName: tagName, TagValue: tagValue}, nil
}

func ParseTag(tagString string) (*Tag, error) {
	tagString = strings.Replace(tagString, "!", "~", -1)
	tagString = strings.TrimSpace(tagString)

	if submatch := negateTagEqualsRegexp.FindStringSubmatch(tagString); len(submatch) > 0 {
		return &Tag{
			TagName:  submatch[1],
			TagValue: submatch[2],
			HasValue: true,
			Negate:   true,
		}, nil
	} else if submatch := TagEqualsRegexp.FindStringSubmatch(tagString); len(submatch) > 0 {
		return &Tag{
			TagName:  submatch[1],
			TagValue: submatch[2],
			HasValue: true,
		}, nil
	} else if submatch := negateTagExistsRegexp.FindStringSubmatch(tagString); len(submatch) > 0 {
		return &Tag{
			TagName: submatch[1],
			Negate:  true,
		}, nil
	} else if submatch := tagExistsRegexp.FindStringSubmatch(tagString); len(submatch) > 0 {
		return &Tag{
			TagName: submatch[1],
		}, nil
	}
	return nil, fmt.Errorf("Unable to parse tag: %s", tagString)
}

func (tag *Tag) String() string {
	return fmt.Sprintf("%s=%s", tag.TagName, tag.TagValue)
}

func (tag *Tag) Display() string {
	if tag.TagValue == "" {
		return tag.TagName
	} else {
		return fmt.Sprintf("%s=%s", tag.TagName, tag.TagValue)
	}
}

func ParseIntersectTags(tagsString string) (tags [](*Tag), err error) {
	for _, tagString := range strings.Split(tagsString, ",") {
		tag, err := ParseTag(tagString)
		if err != nil {
			return tags, err
		}
		tags = append(tags, tag)
	}
	return tags, nil
}

type InstanceTag struct {
	Key InstanceKey
	T   Tag
}

func GetInstanceKeysByTags(tagsString string) (tagged *InstanceKeyMap, err error) {
	tags, err := ParseIntersectTags(tagsString)
	if err != nil {
		return tagged, err
	}
	for i, tag := range tags {
		taggedByTag, err := GetInstanceKeysByTag(tag)
		if err != nil {
			return tagged, err
		}
		if i == 0 {
			tagged = taggedByTag
		} else {
			tagged = tagged.Intersect(taggedByTag)
		}
	}
	return tagged, nil
}

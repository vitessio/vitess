/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.util;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTest {


  @Test
  public void indexOfQuoteDoubleAwareTest() {
    Assert.assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("this is a test", "'", 0));
    Assert.assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("this is a test", "", 0));
    Assert.assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("this is a test", "", 30));
    Assert.assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("this is a test", null, 30));
    Assert.assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(null, "'", 30));
    Assert.assertEquals(8, StringUtils.indexOfQuoteDoubleAware("this is 'a' test", "'", 0));
    Assert.assertEquals(11, StringUtils.indexOfQuoteDoubleAware("this is ''a' test", "'", 0));
    Assert.assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("this is ''a'' test", "'", 0));
  }

  @Test
  public void quoteIdentifierTest() {
    Assert.assertEquals("'this is a test'", StringUtils.quoteIdentifier("this is a test", "'"));
    Assert.assertEquals("'this is a test'", StringUtils.quoteIdentifier("'this is a test'", "'"));
    Assert.assertEquals("'this is'' a test'", StringUtils.quoteIdentifier("this is' a test", "'"));
    Assert.assertEquals("'this is'''' a test'", StringUtils.quoteIdentifier("this is'' a test", "'"));
    Assert.assertEquals("'this is'' a test'", StringUtils.quoteIdentifier("'this is'' a test'", "'"));
    Assert.assertEquals("'this is a ''test'''", StringUtils.quoteIdentifier("this is a 'test'", "'"));
    Assert.assertEquals("'this is a ''test'''", StringUtils.quoteIdentifier("'this is a ''test'''", "'"));
    Assert.assertEquals(null, StringUtils.quoteIdentifier(null, "'"));
    Assert.assertEquals("this is a test", StringUtils.quoteIdentifier("this is a test", ""));
    Assert.assertEquals("this is a test", StringUtils.quoteIdentifier("this is a test", " "));
  }

  @Test
  public void unQuoteIdentifierTest() {
    Assert.assertEquals(null, StringUtils.unQuoteIdentifier(null, "'"));
    Assert.assertEquals("'this is a test'", StringUtils.unQuoteIdentifier("'this is a test'", ""));
    Assert.assertEquals("'this is a test'", StringUtils.unQuoteIdentifier("'this is a test'", " "));
    Assert.assertEquals("'this is a test'", StringUtils.unQuoteIdentifier("'this is a test'", "\""));
    Assert.assertEquals("this is a test'", StringUtils.unQuoteIdentifier("this is a test'", "'"));
    Assert.assertEquals("this is a test", StringUtils.unQuoteIdentifier("'this is a test'", "'"));
    Assert.assertEquals("this is 'a' test", StringUtils.unQuoteIdentifier("'this is ''a'' test'", "'"));
    Assert.assertEquals("this is 'a'' test", StringUtils.unQuoteIdentifier("'this is ''a'''' test'", "'"));
    Assert.assertEquals("'this is a test'", StringUtils.unQuoteIdentifier("'''this is a test'''", "'"));
    Assert.assertEquals("this is a test", StringUtils.unQuoteIdentifier("``this is a test``", "``"));
    Assert.assertEquals("``this is ``a test``", StringUtils.unQuoteIdentifier("``this is ``a test``", "``"));
    Assert.assertEquals("this is ``a test", StringUtils.unQuoteIdentifier("``this is ````a test``", "``"));
    Assert.assertEquals("'this is ''a' test'", StringUtils.unQuoteIdentifier("'this is ''a' test'", "'"));
  }

  @Test
  public void indexOfIgnoreCaseTestDefault() {
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, "a", "`", "`"));
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "this is a test", "", "`", "`"));
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "this is a test", null, "`", "`"));
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(8, "this is a test", "this", "`", "`"));
    Assert.assertEquals(7, StringUtils.indexOfIgnoreCase(0, "this is a test", " a ", "`", "`"));
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "this is `a` test", "a", "`", "`"));
    Assert.assertEquals(10, StringUtils.indexOfIgnoreCase(0, "this is \\`a\\` test", "a", "`", "`"));
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "this `is \\`a` test", "a", "`", "`"));
    Assert.assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "th'is' `is` a test", "is", "`'", "`'"));
    Assert.assertEquals(10, StringUtils.indexOfIgnoreCase(0, "this is a test", "TEST", "`", "`"));
    Assert.assertEquals(10, StringUtils.indexOfIgnoreCase(0, "this is ``a` test", "a", "`", "`"));

    try {
      StringUtils.indexOfIgnoreCase(0, "test is a test", "a", "'", "''");
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
    try {
      StringUtils.indexOfIgnoreCase(0, "this is a test", "a", null, null);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
  }

  @Test
  public void splitTest() {
    Assert.assertEquals(Lists.newArrayList(), StringUtils.split(null, ",", "`", "`"));
    try {
      StringUtils.split("one,two", null, "`", "`");
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
    Assert.assertEquals(Lists.newArrayList("one", "two", "three"), StringUtils.split("one,two,three", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "`two,three`"), StringUtils.split("one,`two,three`", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "{tw{o,t}hree}"), StringUtils.split("one,{tw{o,t}hree}", ",", "{", "}"));
    Assert.assertEquals(Lists.newArrayList("one", "/*two,three*/"), StringUtils.split("one,/*two,three*/", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "-- two,three\n", "four"), StringUtils.split("one,-- two,three\n,four", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "-- two,three\r\n", "four"), StringUtils.split("one,-- two,three\r\n,four", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "#two,three\n", "four"), StringUtils.split("one,#two,three\n,four", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "--;two", "three", "four"), StringUtils.split("one,--;two,three,four", ",", "`", "`"));
    // split doesn't use ALLOW_BACKSLASH_ESCAPE, so escaping delimiter doesn't work
    Assert.assertEquals(Lists.newArrayList("one", "two\\", "three", "four"), StringUtils.split("one,two\\,three,four", ",", "`", "`"));
    // the following comment is a special non-comment block, and it should be split
    Assert.assertEquals(Lists.newArrayList("one", "/*!50110 one", " two */two", "three", "four"), StringUtils.split("one,/*!50110 one, two */two,three,four", ",", "`", "`"));
    Assert.assertEquals(Lists.newArrayList("one", "/*!5011 one", " two */two", "three", "four"), StringUtils.split("one,/*!5011 one, two */two,three,four", ",", "`", "`"));
  }
}

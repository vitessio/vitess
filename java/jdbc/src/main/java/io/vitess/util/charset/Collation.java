/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.util.charset;

/**
 * These classes were pulled from mysql-connector-java and simplified to just the parts supporting
 * the statically available charsets
 */
class Collation {

  public final int index;
  public final String collationName;
  public final int priority;
  public final MysqlCharset mysqlCharset;

  public Collation(int index, String collationName, int priority, String charsetName) {
    this.index = index;
    this.collationName = collationName;
    this.priority = priority;
    this.mysqlCharset = CharsetMapping.CHARSET_NAME_TO_CHARSET.get(charsetName);
  }

  @Override
  public String toString() {
    return "[" + "index=" + this.index + ",collationName=" + this.collationName + ",charsetName="
        + this.mysqlCharset.charsetName + ",javaCharsetName=" + this.mysqlCharset
        .getMatchingJavaEncoding(null) + "]";
  }
}

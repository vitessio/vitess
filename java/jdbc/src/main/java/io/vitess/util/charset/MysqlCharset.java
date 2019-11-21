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

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

/**
 * These classes were pulled from mysql-connector-java and simplified to just the parts supporting
 * the statically available charsets
 */
class MysqlCharset {

  public final String charsetName;
  public final int mblen;
  public final int priority;
  public final Set<String> javaEncodingsUc = new HashSet<>();
  private String defaultEncoding = null;

  public int major = 4;
  public int minor = 1;
  public int subminor = 0;

  /**
   * Constructs MysqlCharset object
   *
   * @param charsetName MySQL charset name
   * @param mblen Max number of bytes per character
   * @param priority MysqlCharset with highest lever of this param will be used for Java
   *     encoding --> Mysql charsets conversion.
   * @param javaEncodings List of Java encodings corresponding to this MySQL charset; the first
   *     name in list is the default for mysql --> java data conversion
   */
  public MysqlCharset(String charsetName, int mblen, int priority, String[] javaEncodings) {
    this.charsetName = charsetName;
    this.mblen = mblen;
    this.priority = priority;

    for (int i = 0; i < javaEncodings.length; i++) {
      String encoding = javaEncodings[i];
      try {
        Charset cs = Charset.forName(encoding);
        addEncodingMapping(cs.name());

        Set<String> als = cs.aliases();
        Iterator<String> ali = als.iterator();
        while (ali.hasNext()) {
          addEncodingMapping(ali.next());
        }
      } catch (Exception exc) {
        // if there is no support of this charset in JVM it's still possible to use our converter
        // for 1-byte charsets
        if (mblen == 1) {
          addEncodingMapping(encoding);
        }
      }
    }

    if (this.javaEncodingsUc.size() == 0) {
      if (mblen > 1) {
        addEncodingMapping("UTF-8");
      } else {
        addEncodingMapping("Cp1252");
      }
    }
  }

  private void addEncodingMapping(String encoding) {
    String encodingUc = encoding.toUpperCase(Locale.ENGLISH);

    if (this.defaultEncoding == null) {
      this.defaultEncoding = encodingUc;
    }

    if (!this.javaEncodingsUc.contains(encodingUc)) {
      this.javaEncodingsUc.add(encodingUc);
    }
  }

  public MysqlCharset(String charsetName, int mblen, int priority, String[] javaEncodings,
      int major, int minor) {
    this(charsetName, mblen, priority, javaEncodings);
    this.major = major;
    this.minor = minor;
  }

  public MysqlCharset(String charsetName, int mblen, int priority, String[] javaEncodings,
      int major, int minor, int subminor) {
    this(charsetName, mblen, priority, javaEncodings);
    this.major = major;
    this.minor = minor;
    this.subminor = subminor;
  }

  @Override
  public String toString() {
    StringBuilder asString = new StringBuilder();
    asString.append("[");
    asString.append("charsetName=");
    asString.append(this.charsetName);
    asString.append(",mblen=");
    asString.append(this.mblen);
    // asString.append(",javaEncoding=");
    // asString.append(this.javaEncodings.toString());
    asString.append("]");
    return asString.toString();
  }

  /**
   * If javaEncoding parameter value is one of available java encodings for this charset then
   * returns javaEncoding value as is. Otherwise returns first available java encoding name.
   */
  String getMatchingJavaEncoding(String javaEncoding) {
    if (javaEncoding != null && this.javaEncodingsUc
        .contains(javaEncoding.toUpperCase(Locale.ENGLISH))) {
      return javaEncoding;
    }
    return this.defaultEncoding;
  }
}

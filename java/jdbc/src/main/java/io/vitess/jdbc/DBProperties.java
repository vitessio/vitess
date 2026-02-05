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

package io.vitess.jdbc;

/**
 * Created by ashudeep.sharma on 10/03/16.
 */
public class DBProperties {

  private final String productVersion;
  private final String majorVersion;
  private final String minorVersion;
  private final int isolationLevel;
  private final boolean caseInsensitiveComparison;
  private final boolean storesLowerCaseTableName;

  public DBProperties(String productVersion, String majorVersion, String minorVersion,
      int isolationLevel, String lowerCaseTableNames) {

    this.productVersion = productVersion;
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.isolationLevel = isolationLevel;
    this.caseInsensitiveComparison =
        "1".equalsIgnoreCase(lowerCaseTableNames) || "2".equalsIgnoreCase(lowerCaseTableNames);
    this.storesLowerCaseTableName = "1".equalsIgnoreCase(lowerCaseTableNames);
  }

  public String getProductVersion() {
    return this.productVersion;
  }

  public String getMajorVersion() {
    return this.majorVersion;
  }

  public String getMinorVersion() {
    return this.minorVersion;
  }

  public int getIsolationLevel() {
    return this.isolationLevel;
  }

  public boolean getUseCaseInsensitiveComparisons() {
    return caseInsensitiveComparison;
  }

  public boolean getStoresLowerCaseTableName() {
    return storesLowerCaseTableName;
  }
}

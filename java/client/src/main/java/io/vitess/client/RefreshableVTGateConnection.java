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
 
package io.vitess.client;

import java.io.File;

public class RefreshableVTGateConnection extends VTGateConnection {

  private final File keystoreFile;
  private final File truststoreFile;
  private volatile long keystoreMtime;
  private volatile long truststoreMtime;

  public RefreshableVTGateConnection(RpcClient client,
      String keystorePath,
      String truststorePath) {
    super(client);
    this.keystoreFile = new File(keystorePath);
    this.truststoreFile = new File(truststorePath);
    this.keystoreMtime = this.keystoreFile.exists() ? this.keystoreFile.lastModified() : 0;
    this.truststoreMtime = this.truststoreFile.exists() ? this.truststoreFile.lastModified() : 0;
  }

  public boolean checkKeystoreUpdates() {
    long keystoreMtime = keystoreFile.exists() ? keystoreFile.lastModified() : 0;
    long truststoreMtime = truststoreFile.exists() ? truststoreFile.lastModified() : 0;
    boolean modified = false;
    if (keystoreMtime > this.keystoreMtime) {
      modified = true;
      this.keystoreMtime = keystoreMtime;
    }
    if (truststoreMtime > this.truststoreMtime) {
      modified = true;
      this.truststoreMtime = truststoreMtime;
    }
    return modified;
  }
}

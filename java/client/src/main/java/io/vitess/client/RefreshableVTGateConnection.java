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

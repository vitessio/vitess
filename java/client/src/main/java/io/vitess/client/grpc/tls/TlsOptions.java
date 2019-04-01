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

package io.vitess.client.grpc.tls;

import java.io.File;
import java.net.InetSocketAddress;

/**
 * <p>A wrapper type holding TLS-related fields for the
 * {@link io.vitess.client.RpcClientFactory#createTls(InetSocketAddress, TlsOptions)} method, so
 * that this method won't have an unwieldy number of direct parameters.</p>
 *
 * <p>This path uses a builder pattern style:</p>
 *
 * <blockquote>
 * <pre>{@code
 *    final TlsOptions tlsOptions = new TlsOptions()
 *       .keyStorePath(keyStore)
 *       .keyStorePassword("passwd")
 *       .keyAlias("cert")
 *       .trustStorePath(trustStore)
 *       .trustStorePassword("passwd")
 *       .trustAlias("cacert");
 * }</pre>
 * </blockquote>
 */
public class TlsOptions {

  private File keyStore;
  private String keyStorePassword;
  private String keyAlias;
  private String keyPassword;
  private File trustStore;
  private String trustStorePassword;
  private String trustAlias;

  public TlsOptions keyStorePath(String keyStorePath) {
    if (keyStorePath != null) {
      this.keyStore = new File(keyStorePath);
    }
    return this;
  }

  public TlsOptions keyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
    return this;
  }

  public TlsOptions keyAlias(String keyAlias) {
    this.keyAlias = keyAlias;
    return this;
  }

  public TlsOptions keyPassword(String keyPassword) {
    this.keyPassword = keyPassword;
    return this;
  }

  public TlsOptions trustStorePath(String trustStorePath) {
    if (trustStorePath != null) {
      this.trustStore = new File(trustStorePath);
    }
    return this;
  }

  public TlsOptions trustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
    return this;
  }

  public TlsOptions trustAlias(String trustAlias) {
    this.trustAlias = trustAlias;
    return this;
  }

  public File getKeyStore() {
    return keyStore;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getKeyAlias() {
    return keyAlias;
  }

  public String getKeyPassword() {
    return keyPassword;
  }

  public File getTrustStore() {
    return trustStore;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public String getTrustAlias() {
    return trustAlias;
  }
}

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

package io.vitess.client.grpc;

import io.grpc.CallCredentials;
import io.grpc.LoadBalancer;
import io.grpc.NameResolver;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;

import javax.net.ssl.SSLException;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.RpcClientFactory;
import io.vitess.client.grpc.tls.TlsOptions;

/**
 * GrpcClientFactory creates RpcClients with the gRPC implementation.
 */
public class GrpcClientFactory implements RpcClientFactory {

  private RetryingInterceptorConfig config;
  private CallCredentials callCredentials;
  private LoadBalancer.Factory loadBalancerFactory;
  private NameResolver.Factory nameResolverFactory;

  public GrpcClientFactory() {
    this(RetryingInterceptorConfig.noOpConfig());
  }

  public GrpcClientFactory(RetryingInterceptorConfig config) {
    this.config = config;
  }

  public GrpcClientFactory setCallCredentials(CallCredentials value) {
    callCredentials = value;
    return this;
  }

  public GrpcClientFactory setLoadBalancerFactory(LoadBalancer.Factory value) {
    loadBalancerFactory = value;
    return this;
  }

  public GrpcClientFactory setNameResolverFactory(NameResolver.Factory value) {
    nameResolverFactory = value;
    return this;
  }

  /**
   * Factory method to construct a gRPC client connection with no transport-layer security.
   *
   * @param ctx TODO: This parameter is not actually used, but probably SHOULD be so that timeout duration and caller ID settings aren't discarded
   * @param target
   *    target is passed to NettyChannelBuilder which will resolve based on scheme, by default dns.
   * @return
   */
  @Override
  public RpcClient create(Context ctx, String target) {
    NettyChannelBuilder channel = channelBuilder(target)
            .negotiationType(NegotiationType.PLAINTEXT)
            .intercept(new RetryingInterceptor(config));
    if (loadBalancerFactory != null) {
      channel.loadBalancerFactory(loadBalancerFactory);
    }
    if (nameResolverFactory != null) {
      channel.nameResolverFactory(nameResolverFactory);
    }
    return callCredentials != null ?
            new GrpcClient(channel.build(), callCredentials) : new GrpcClient(channel.build());
  }

  /**
   * <p>This method constructs NettyChannelBuilder object that will be used to create RpcClient.<p/>
   * <p>Subclasses may override this method to make adjustments to the builder<p/>
   * for example:
   * <code>
   *     @Override
   *     protected NettyChannelBuilder channelBuilder(String target) {
   *       return super.channelBuilder(target)
   *               .eventLoopGroup(new EpollEventLoopGroup())
   *               .withOption(EpollChannelOption.TCP_USER_TIMEOUT,30);
   *     }
   *
   * </code>
   *
   * @param target
   *    target is passed to NettyChannelBuilder which will resolve based on scheme, by default dns.
   * @return
   */
  protected NettyChannelBuilder channelBuilder(String target){
    return NettyChannelBuilder.forTarget(target);
  }

  /**
   * <p>Factory method to construct a gRPC client connection with transport-layer security.</p>
   *
   * <p>Within the <code>tlsOptions</code> parameter value, the <code>trustStore</code> field should
   * always be populated.  All other fields are optional.</p>
   *
   * @param ctx TODO: This parameter is not actually used, but probably SHOULD be so that timeout duration and caller ID settings aren't discarded
   * @param target
   *    target is passed to NettyChannelBuilder which will resolve based on scheme, by default dns.
   * @param tlsOptions
   * @return
   */
  @Override
  public RpcClient createTls(Context ctx, String target, TlsOptions tlsOptions) {
    final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();

    // trustManager should always be set
    final KeyStore trustStore = loadKeyStore(tlsOptions.getTrustStore(), tlsOptions.getTrustStorePassword());
    if (trustStore == null) {
      throw new RuntimeException("Could not load trustStore");
    }
    final X509Certificate[] trustCertCollection = tlsOptions.getTrustAlias() == null
            ? loadCertCollection(trustStore)
            : loadCertCollectionForAlias(trustStore, tlsOptions.getTrustAlias());
    sslContextBuilder.trustManager(trustCertCollection);

    // keyManager should only be set if a keyStore is specified (meaning that client authentication is enabled)
    final KeyStore keyStore = loadKeyStore(tlsOptions.getKeyStore(), tlsOptions.getKeyStorePassword());
    if (keyStore != null) {
      final PrivateKeyWrapper privateKeyWrapper = tlsOptions.getKeyAlias() == null
              ? loadPrivateKeyEntry(keyStore, tlsOptions.getKeyStorePassword(), tlsOptions.getKeyPassword())
              : loadPrivateKeyEntryForAlias(keyStore, tlsOptions.getKeyAlias(), tlsOptions.getKeyStorePassword(), tlsOptions.getKeyPassword());
      if (privateKeyWrapper == null) {
        throw new RuntimeException("Could not retrieve private key and certificate chain from keyStore");
      }

      sslContextBuilder.keyManager(
              privateKeyWrapper.getPrivateKey(),
              privateKeyWrapper.getPassword(),
              privateKeyWrapper.getCertificateChain()
      );
    }

    final SslContext sslContext;
    try {
      sslContext = sslContextBuilder.build();
    } catch (SSLException e) {
      throw new RuntimeException(e);
    }

    return new GrpcClient(
            channelBuilder(target).negotiationType(NegotiationType.TLS).sslContext(sslContext).intercept(new RetryingInterceptor(config)).build());
  }

  /**
   * <p>Opens a JKS keystore file from the filesystem.</p>
   *
   * <p>Returns <code>null</code> if the file is inaccessible for any reason, or if the password fails to unlock it.</p>
   *
   * @param keyStoreFile
   * @param keyStorePassword
   * @return
   * @throws KeyStoreException
   * @throws IOException
   * @throws CertificateException
   * @throws NoSuchAlgorithmException
   */
  private KeyStore loadKeyStore(final File keyStoreFile, String keyStorePassword) {
    if (keyStoreFile == null) {
      return null;
    }

    try {
      final KeyStore keyStore = KeyStore.getInstance(Constants.KEYSTORE_TYPE);
      final char[] password = keyStorePassword == null ? null : keyStorePassword.toCharArray();
      try (final FileInputStream fis = new FileInputStream(keyStoreFile)) {
        keyStore.load(fis, password);
      }
      return keyStore;
    } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
      return null;
    }
  }

  /**
   * <p>Loads an X509 certificate from a keystore using a given alias, and returns it as a one-element
   * array so that it can be passed to {@link SslContextBuilder#trustManager(File)}.</p>
   *
   * <p>Returns <code>null</code> if there is any problem accessing the keystore, or if the alias does
   * not match an X509 certificate.</p>
   *
   * @param keyStore
   * @param alias
   * @return
   */
  private X509Certificate[] loadCertCollectionForAlias(final KeyStore keyStore, final String alias) {
    if (keyStore == null) {
      return null;
    }

    try {
      return new X509Certificate[] {
              (X509Certificate) keyStore.getCertificate(alias)
      };
    } catch (KeyStoreException | ClassCastException e) {
      return null;
    }
  }

  /**
   * <p>Loads the first valid X509 certificate found in the keystore, and returns it as a one-element
   * array so that it can be passed to {@link SslContextBuilder#trustManager(File)}.</p>
   *
   * <p>Returns <code>null</code> if there is any problem accessing the keystore, or if no X509 certificates
   * are found at all.</p>
   *
   * @param keyStore
   * @return
   */
  private X509Certificate[] loadCertCollection(final KeyStore keyStore) {
    if (keyStore == null) {
      return null;
    }

    final Enumeration<String> aliases;
    try {
      aliases = keyStore.aliases();
    } catch (KeyStoreException e) {
      return null;
    }

    while (aliases.hasMoreElements()) {
      final String alias = aliases.nextElement();
      final X509Certificate[] certCollection = loadCertCollectionForAlias(keyStore, alias);
      if (certCollection != null) {
        return certCollection;
      }
    }

    return null;
  }

  /**
   * <p>Loads from a keystore the private key entry matching a given alias, and returns it parsed and
   * ready to be passed to {@link SslContextBuilder#keyManager(PrivateKey, String, X509Certificate...)}.</p>
   *
   * <p>To access the private key, this method will first try using the <code>keyPassword</code> parameter
   * value (this parameter can be set to <code>null</code> to explicitly indicate that there is no password).
   * If that fails, then this method will then try using the <code>keyStorePassword</code> parameter value
   * as the key value too.</p>
   *
   * <p>Returns null if there is any problem accessing the keystore, if neither <code>keyPassword</code>
   * or <code>keyStorePassword</code> can unlock the private key, or if no private key entry matches
   * <code>alias</code>.</p>
   *
   * @param keyStore
   * @param alias
   * @param keyStorePassword
   * @param keyPassword
   * @return
   * @throws KeyStoreException
   * @throws NoSuchAlgorithmException
   * @throws UnrecoverableEntryException
   */
  private PrivateKeyWrapper loadPrivateKeyEntryForAlias(final KeyStore keyStore, final String alias,
                                                  final String keyStorePassword, final String keyPassword) {
    if (keyStore == null || alias == null) {
      return null;
    }

    try {
      if (!keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class)) {
        // There is no private key matching this alias
        return null;
      }
    } catch (KeyStoreException e) {
      return null;
    }

    // Try loading the private key with the key password (which can be null)
    try {
      final char[] pass = keyPassword == null ? null : keyPassword.toCharArray();
      final KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias, new KeyStore.PasswordProtection(pass));
      return new PrivateKeyWrapper(entry.getPrivateKey(), keyPassword, entry.getCertificateChain());
    } catch (KeyStoreException | NoSuchAlgorithmException e) {
        return null;
    } catch (UnrecoverableEntryException e) {
      // The key password didn't work (or just wasn't set).  Try using the keystore password.
      final char[] pass = keyStorePassword == null ? null : keyStorePassword.toCharArray();
      try {
        final KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias, new KeyStore.PasswordProtection(pass));
        return new PrivateKeyWrapper(entry.getPrivateKey(), keyPassword, entry.getCertificateChain());
      } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableEntryException e1) {
          // Neither password worked.
          return null;
      }
    }
  }

  /**
   * <p>Loads from a keystore the first valid private key entry found, and returns it parsed and ready to be
   * passed to {@link SslContextBuilder#keyManager(PrivateKey, String, X509Certificate...)}.</p>
   *
   * <p>To access the private key, this method will first try using the <code>keyPassword</code> parameter
   * value (this parameter can be set to <code>null</code> to explicitly indicate that there is no password).
   * If that fails, then this method will then try using the <code>keyStorePassword</code> parameter value
   * as the key value too.</p>
   *
   * <p>Returns null if there is any problem accessing the keystore, if neither <code>keyPassword</code>
   * or <code>keyStorePassword</code> can unlock the private key, or if no private key entry matches
   * <code>alias</code>.</p>
   *
   * @param keyStore
   * @param keyStorePassword
   * @param keyPassword
   * @return
   * @throws KeyStoreException
   * @throws NoSuchAlgorithmException
   */
  private PrivateKeyWrapper loadPrivateKeyEntry(final KeyStore keyStore, final String keyStorePassword,
                                                  final String keyPassword) {
    if (keyStore == null) {
      return null;
    }

    final Enumeration<String> aliases;
    try {
      aliases = keyStore.aliases();
    } catch (KeyStoreException e) {
      return null;
    }

    while (aliases.hasMoreElements()) {
      final String alias = aliases.nextElement();
      final PrivateKeyWrapper privateKeyWrapper = loadPrivateKeyEntryForAlias(keyStore, alias, keyStorePassword, keyPassword);
      if (privateKeyWrapper != null) {
        return privateKeyWrapper;
      }
    }
    return null;
  }

  /**
   * A container for the values returned by {@link this#loadPrivateKeyEntry(KeyStore, String, String)} and
   * {@link this#loadPrivateKeyEntryForAlias(KeyStore, String, String, String)}, and passed in turn
   * to {@link SslContextBuilder#keyManager(PrivateKey, String, X509Certificate...)}.  Helpful since Java
   * methods cannot return multiple values.
   */
  private class PrivateKeyWrapper {
    private PrivateKey privateKey;
    private String password;
    private X509Certificate[] certificateChain;

    public PrivateKeyWrapper(final PrivateKey privateKey, final String password, final Certificate[] certificateChain) {
      this.privateKey = privateKey;
      this.password = password;
      this.certificateChain = Arrays.copyOf(certificateChain, certificateChain.length, X509Certificate[].class);
    }

    public PrivateKey getPrivateKey() {
      return privateKey;
    }

    public String getPassword() {
      return password;
    }

    public X509Certificate[] getCertificateChain() {
      return certificateChain;
    }
  }

}

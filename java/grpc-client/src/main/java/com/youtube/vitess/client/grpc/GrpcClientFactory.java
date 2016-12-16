package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.RpcClientFactory;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
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

/**
 * GrpcClientFactory creates RpcClients with the gRPC implementation.
 */
public class GrpcClientFactory implements RpcClientFactory {
  /**
   * TODO: Document
   *
   * @param ctx
   * @param address
   * @return
   */
  @Override
  public RpcClient create(Context ctx, InetSocketAddress address) {
    return new GrpcClient(
            NettyChannelBuilder.forAddress(address).negotiationType(NegotiationType.PLAINTEXT).build());
  }

  /**
   * TODO: Document
   *
   * @param ctx
   * @param address
   * @return
   */
  public RpcClient createTls(Context ctx, InetSocketAddress address, TlsOptions tlsOptions) {
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
        NettyChannelBuilder.forAddress(address).negotiationType(NegotiationType.TLS).sslContext(sslContext).build());
  }

  /**
   * TODO: Document
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
   * TODO: Document
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
      final X509Certificate[] certCollection = {
              (X509Certificate) keyStore.getCertificate(alias)
      };
      return certCollection;
    } catch (KeyStoreException | ClassCastException e) {
      return null;
    }
  }

  /**
   * TODO: Document
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
   * TODO: Document
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
   * TODO: Document
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
   * TODO: Document
   */
  public static class TlsOptions {
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

  /**
   * TODO: Document
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

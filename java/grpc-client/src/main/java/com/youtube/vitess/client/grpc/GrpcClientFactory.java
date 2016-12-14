package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.RpcClientFactory;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

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
    SslContext sslContext;
    try {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();

      // trustManager should always be set
      final KeyStore trustStore = loadKeyStore(tlsOptions.getTrustStore(), tlsOptions.getTrustStorePassword());
      final X509Certificate[] trustCertCollection = {
              (X509Certificate) trustStore.getCertificate(tlsOptions.getTrustAlias())
      };
      sslContextBuilder.trustManager(trustCertCollection);

      // keyManager should only be set if a keyStore is specified (meaning that client authentication is enabled
      final KeyStore keyStore = loadKeyStore(tlsOptions.getKeyStore(), tlsOptions.getKeyStorePassword());
      if (keyStore != null) {
        final PrivateKeyWrapper privateKeyWrapper = tlsOptions.getKeyAlias() == null
                ? loadPrivateKeyEntry(keyStore, tlsOptions.getKeyStorePassword(), tlsOptions.getKeyPassword())
                : loadPrivateKeyEntryForAlias(keyStore, tlsOptions.getKeyAlias(), tlsOptions.getKeyStorePassword(), tlsOptions.getKeyPassword());
        sslContextBuilder.keyManager(
                privateKeyWrapper.getPrivateKey(),
                privateKeyWrapper.getPassword(),
                privateKeyWrapper.getCertificateChain()
        );
      }

      sslContext = sslContextBuilder.build();
    } catch (NullPointerException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableEntryException
            | IOException ex) {
      throw new RuntimeException(ex);
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
    try {
      final KeyStore keyStore = KeyStore.getInstance(Constants.KEYSTORE_TYPE);
      final char[] password = keyStorePassword == null ? null : keyStorePassword.toCharArray();
      try (final FileInputStream fis = new FileInputStream(keyStoreFile)) {
        keyStore.load(fis, password);
      }
      return keyStore;
    } catch (NullPointerException | KeyStoreException | CertificateException | NoSuchAlgorithmException
            | IOException e) {
      return null;
    }
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
                                                  final String keyStorePassword, final String keyPassword)
          throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableEntryException {
    if (!keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class)) {
      // There is no private key matching this alias
      return null;
    }

    // Try loading the private key with the key password (which can be null)
    try {
      final char[] pass = keyPassword == null ? null : keyPassword.toCharArray();
      final KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias, new KeyStore.PasswordProtection(pass));
      return new PrivateKeyWrapper(entry.getPrivateKey(), keyPassword, entry.getCertificateChain());
    } catch (UnrecoverableEntryException e) {
        // Password invalid.  Try using the keystore password, and let the UnrecoverableEntryException bubble up
        // this time if that password doesn't work either.
        final char[] pass = keyStorePassword == null ? null : keyStorePassword.toCharArray();
        final KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias, new KeyStore.PasswordProtection(pass));
        return new PrivateKeyWrapper(entry.getPrivateKey(), keyPassword, entry.getCertificateChain());
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
                                                  final String keyPassword)
          throws KeyStoreException, NoSuchAlgorithmException {
    final Enumeration<String> aliases = keyStore.aliases();
    while (aliases.hasMoreElements()) {
      final String alias = aliases.nextElement();
      try {
        return loadPrivateKeyEntryForAlias(keyStore, alias, keyStorePassword, keyPassword);
      } catch (UnrecoverableEntryException e) {
          // Neither the key password nor keystore password could unlock this particular alias.  Not unexpected... just
          // continue trying the other aliases.
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

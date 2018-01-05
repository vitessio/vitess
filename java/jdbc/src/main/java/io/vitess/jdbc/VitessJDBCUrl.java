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

package io.vitess.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.vitess.util.Constants;
import io.vitess.util.StringUtils;

/**
 * VitessJDBCUrl is responsible for parsing a driver URL and Properties object,
 * returning a new Properties object with configuration from the URL and passed in Properties
 * merged.
 * <p>
 * Parameters passed in through the Properties object take precedence over the parameters
 * in the URL, where there are conflicts.
 * <p>
 * The Passed in URL is expected to conform to the following basic format:
 * <p>
 * jdbc:vitess://username:password@ip1:port1,ip2:port2/keyspace/catalog?property1=value1..
 */
public class VitessJDBCUrl {

    private final String url;
    private final List<HostInfo> hostInfos;
    private final Properties info;

    /*
     Assuming List of vtGate ips could be given in url, separated by ","
    */
    public static class HostInfo {
        private String hostname;
        private int port;

        public HostInfo(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public String getHostname() {
            return hostname;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return hostname + ":" + port;
        }
    }

    /**
     * <p>Create VitessJDBC url object for given urls and properties.</p>
     * <p>
     * <p>To indicate that SSL should be used, URL's follow the MySQL and MariaDB convention of including
     * the property <code>useSSL=true</code>.  To use a keyStore and trustStore other than the JRE
     * default, you can add the following URL properties:</p>
     * <p>
     * <p>
     * <ul>
     * <li><code>keyStore</code>=path_to_keystore_file</li>
     * <li><code>keyStorePassword</code>=password (if set)</li>
     * <li><code>keyPassword</code>=password (only needed if the private key password differs from
     * the keyStore password)</li>
     * <li><code>keyAlias</code>=alias_under_which_private_key_is_stored (if not set, then the
     * first valid <code>PrivateKeyEntry</code> found in the keyStore will be used)</li>
     * <li><code>trustStore</code>=path_to_truststore_file</li>
     * <li><code>trustStorePassword</code>=password (if set)</li>
     * <li><code>trustAlias</code>=alias_under_which_certificate_chain_is_stored (if not set,
     * then the first valid <code>X509Certificate</code> found in the trustStore will be used)</li>
     * </ul>
     * </p>
     * <p>
     * <p>If <code>useSSL=true</code>, and any of these additional properties are not set on the JDBC URL,
     * then the driver will look to see if these corresponding property was set at JVM startup time:</p>
     * <p>
     * <p>
     * <ul>
     * <li><code>-Djavax.net.ssl.keyStore</code></li>
     * <li><code>-Djavax.net.ssl.keyStorePassword</code></li>
     * <li><code>-Djavax.net.ssl.keyPassword</code></li>
     * <li><code>-Djavax.net.ssl.keyAlias</code></li>
     * <li><code>-Djavax.net.ssl.trustStore</code></li>
     * <li><code>-Djavax.net.ssl.trustStorePassword</code></li>
     * <li><code>-Djavax.net.ssl.trustStoreAlias</code></li>
     * </ul>
     * </p>
     * <p>
     * <p>See:</p>
     * <p>https://mariadb.com/kb/en/mariadb/about-mariadb-connector-j/#tls-ssl</p>
     * <p>https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-using-ssl.html</p>
     *
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    public VitessJDBCUrl(String url, Properties info) throws SQLException {

        /* URL pattern e.g. jdbc:vitess://username:password@ip1:port1,ip2:port2/keyspace/catalog?
        property1=value1..
        m.group(1) = "vitess"
        m.group(2) = "username:password@"
        m.group(3) = "username"
        m.group(4) = ":password"
        m.group(5) = "password"
        m.group(6) = "ip1:port1,ip2:port2"
        m.group(7) = "/keyspace"
        m.group(8) = "keyspace"
        m.group(9) = "/catalog"
        m.group(10) = "catalog"
        m.group(11) = "?property1=value1.."
        m.group(12) = "property1=value1.."
        */

        final Pattern p = Pattern.compile(Constants.URL_PATTERN);
        final Matcher m = p.matcher(url);
        if (!m.find()) {
            throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
        }

        info = getURLParamProperties(m.group(12), info);

        // Will add password property once its supported from vitess
        // this.password = (m.group(5) == null ? info.getProperty("password") : m.group(5));
        String postUrl = m.group(6);
        if (null == postUrl) {
            throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
        }

        this.hostInfos = getURLHostInfos(postUrl);
        this.url = url;

        // For Backward Compatibility of username in connection url.
        String userName = m.group(3);
        if (!StringUtils.isNullOrEmptyWithoutWS(userName)) {
            info.setProperty(Constants.Property.USERNAME, userName);
        }

        // For Backward Compatibility of keyspace & catalog/database/schema name in connection url.
        String keyspace = m.group(8);
        if (!StringUtils.isNullOrEmptyWithoutWS(keyspace)) {
            info.setProperty(Constants.Property.KEYSPACE, keyspace);
            String catalog = m.group(10);
            if (!StringUtils.isNullOrEmptyWithoutWS(catalog)) {
                info.setProperty(Constants.Property.DBNAME, catalog);
            } else {
                info.setProperty(Constants.Property.DBNAME, keyspace);
            }
        }

        // For Backward Compatibility of old tablet type.
        String oldTabletType = info.getProperty(Constants.Property.OLD_TABLET_TYPE);
        String tabletType = info.getProperty(Constants.Property.TABLET_TYPE);
        if (null != tabletType) {
            info.setProperty(Constants.Property.OLD_TABLET_TYPE, tabletType);
        } else if (null != oldTabletType) {
            info.setProperty(Constants.Property.TABLET_TYPE, oldTabletType);
        }

        this.info = info;
    }

    public String getUrl() {
        return url;
    }

    public List<HostInfo> getHostInfos() {
        return hostInfos;
    }

    /**
     * Get Properties object for params after ? in url.
     *
     * @param paramString Parameter String in the url
     * @param info        passed in the connection
     * @return Properties updated with parameters
     */

    private static Properties getURLParamProperties(String paramString, Properties info)
            throws SQLException {
        // If passed in, don't use info properties directly so we don't act upon it accidentally.
        // instead use as defaults for a new properties object
        info = (info == null) ? new Properties() : new Properties(info);

        if (!StringUtils.isNullOrEmptyWithoutWS(paramString)) {
            StringTokenizer queryParams = new StringTokenizer(paramString, "&"); //$NON-NLS-1$
            while (queryParams.hasMoreTokens()) {
                String parameterValuePair = queryParams.nextToken();
                int indexOfEquals = parameterValuePair.indexOf('=');

                String parameter = null;
                String value = null;

                if (indexOfEquals != -1) {
                    parameter = parameterValuePair.substring(0, indexOfEquals);

                    if (indexOfEquals + 1 < parameterValuePair.length()) {
                        value = parameterValuePair.substring(indexOfEquals + 1);
                    }
                }

                // Per the mysql-connector-java docs, passed in Properties values should take precedence over
                // those in the URL. See javadoc for NonRegisteringDriver#connect
                if ((null != value && value.length() > 0) && (parameter.length() > 0) && null == info.getProperty(parameter)) {
                    try {
                        info.put(parameter, URLDecoder.decode(value, "UTF-8"));
                    } catch (UnsupportedEncodingException | NoSuchMethodError badEncoding) {
                        throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
                    }
                }
            }
        }

        return info;
    }

    /**
     * Get List of Hosts for url separated by commas
     *
     * @param hostURLs
     * @return
     * @throws SQLException
     */
    private static List<HostInfo> getURLHostInfos(String hostURLs) throws SQLException {
        List<HostInfo> hostInfos = new ArrayList<>();

        StringTokenizer stringTokenizer = new StringTokenizer(hostURLs, ",");
        while (stringTokenizer.hasMoreTokens()) {
            String hostString = stringTokenizer.nextToken();
            /*
                pattern = ip:port
             */
            final Pattern p = Pattern.compile("([^/:]+):(\\d+)?");
            final Matcher m = p.matcher(hostString);
            if (!m.find()) {
                throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
            }
            String hostname = m.group(1);
            int port;
            if (m.group(2) != null) {
                port = Integer.parseInt(m.group(2));
            } else {
                port = Integer.parseInt(Constants.DEFAULT_PORT);
            }
            HostInfo hostInfo = new HostInfo(hostname, port);
            hostInfos.add(hostInfo);
        }
        if (hostInfos.size() == 0) {
            throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
        }
        return hostInfos;
    }

    public Properties getProperties() {
        return info;
    }
}

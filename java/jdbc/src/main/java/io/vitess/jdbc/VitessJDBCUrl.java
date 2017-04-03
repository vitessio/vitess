package io.vitess.jdbc;

import io.vitess.proto.Topodata;
import io.vitess.util.Constants;
import io.vitess.util.StringUtils;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * VitessJDBCUrl is responsible for parsing a driver URL and Properties object,
 * returning a new Properties object with configuration from the URL and passed in Properties
 * merged.
 *
 * Parameters passed in through the Properties object take precedence over the parameters
 * in the URL, where there are conflicts.
 *
 * The Passed in URL is expected to conform to the following basic format:
 *
 * jdbc:vitess://username:password@ip1:port1,ip2:port2/keyspace/catalog?property1=value1..
 *
 */
public class VitessJDBCUrl {

    private final String username;
    private final String url;
    private final List<HostInfo> hostInfos;
    private final String keyspace;
    private final Properties info;
    private String catalog;

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
    }

    /**
     * <p>Create VitessJDBC url object for given urls and properties.</p>
     *
     * <p>To indicate that SSL should be used, URL's follow the MySQL and MariaDB convention of including
     * the property <code>useSSL=true</code>.  To use a keyStore and trustStore other than the JRE
     * default, you can add the following URL properties:</p>
     *
     * <p>
     *     <ul>
     *         <li><code>keyStore</code>=path_to_keystore_file</li>
     *         <li><code>keyStorePassword</code>=password (if set)</li>
     *         <li><code>keyPassword</code>=password (only needed if the private key password differs from
     *                  the keyStore password)</li>
     *         <li><code>keyAlias</code>=alias_under_which_private_key_is_stored (if not set, then the
     *                  first valid <code>PrivateKeyEntry</code> found in the keyStore will be used)</li>
     *         <li><code>trustStore</code>=path_to_truststore_file</li>
     *         <li><code>trustStorePassword</code>=password (if set)</li>
     *         <li><code>trustAlias</code>=alias_under_which_certificate_chain_is_stored (if not set,
     *                  then the first valid <code>X509Certificate</code> found in the trustStore will be used)</li>
     *     </ul>
     * </p>
     *
     * <p>If <code>useSSL=true</code>, and any of these additional properties are not set on the JDBC URL,
     * then the driver will look to see if these corresponding property was set at JVM startup time:</p>
     *
     * <p>
     *     <ul>
     *         <li><code>-Djavax.net.ssl.keyStore</code></li>
     *         <li><code>-Djavax.net.ssl.keyStorePassword</code></li>
     *         <li><code>-Djavax.net.ssl.keyPassword</code></li>
     *         <li><code>-Djavax.net.ssl.keyAlias</code></li>
     *         <li><code>-Djavax.net.ssl.trustStore</code></li>
     *         <li><code>-Djavax.net.ssl.trustStorePassword</code></li>
     *         <li><code>-Djavax.net.ssl.trustStoreAlias</code></li>
     *     </ul>
     * </p>
     *
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

        this.username =
            (m.group(3) == null ? info.getProperty(Constants.Property.USERNAME) : m.group(3));
        // Will add password property once its supported from vitess
        // this.password = (m.group(5) == null ? info.getProperty("password") : m.group(5));
        String postUrl = m.group(6);
        if (null == postUrl) {
            throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
        }

        this.keyspace = StringUtils.isNullOrEmptyWithoutWS(m.group(8)) ? null : m.group(8);
        this.catalog =
            StringUtils.isNullOrEmptyWithoutWS(m.group(10)) ? this.keyspace : m.group(10);
        this.hostInfos = getURLHostInfos(postUrl);
        this.url = url;
        this.info = info;
    }

    public String getUsername() {
        return username;
    }

    public String getUrl() {
        return url;
    }

    public List<HostInfo> getHostInfos() {
        return hostInfos;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
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

    /**
     * Retrieves the value (if any) for a given key from a <code>Properties</code> object, regardless of the
     * capitalization used in the actual key.  Used by the constructor for parsing SSL-related optional parameters,
     * so that both the names and values of those parameters can be case-insensitive.
     *
     * @param properties
     * @param key
     * @return The first value found with a key that is a case-insensitive match for <code>key</code>, or <code>null</code> if there is no value found.
     */
    private static String caseInsensitiveKeyLookup(final Properties properties, final String key) {
        if (properties == null || key == null) return null;
        for (final Object uncastKeyBuffer : properties.keySet()) {
            if (uncastKeyBuffer instanceof String) {
                final String keyBuffer = (String) uncastKeyBuffer;
                if (key.equalsIgnoreCase(keyBuffer)) {
                    return properties.getProperty(keyBuffer);
                }
            }
        }
        return null;
    }

}

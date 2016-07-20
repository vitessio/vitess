package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.flipkart.vitess.util.StringUtils;
import com.youtube.vitess.proto.Topodata;

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
 * Created by naveen.nahata on 17/02/16.
 */
public class VitessJDBCUrl {

    private final String username;
    private final Topodata.TabletType tabletType;
    private final String url;
    private final List<HostInfo> hostInfos;
    private final String keyspace;
    private String catalog;
    private final String executeType;


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
     * Create VitessJDBC url object for given urls and properties.
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

        String tabletType = info.getProperty(Constants.Property.TABLET_TYPE);
        if (null == tabletType) {
            tabletType = Constants.DEFAULT_TABLET_TYPE;
        }

        this.tabletType = getTabletType(tabletType);

        this.executeType = info.getProperty(Constants.Property.EXECUTE_TYPE);
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public Topodata.TabletType getTabletType() {
        return tabletType;
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

    public Constants.QueryExecuteType getExecuteType() {
        if (this.executeType != null) {
            switch (this.executeType) {
                case "simple":
                    return Constants.QueryExecuteType.SIMPLE;
                case "stream":
                    return Constants.QueryExecuteType.STREAM;
            }
        }
        return Constants.DEFAULT_EXECUTE_TYPE;
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
        if (null == info) {
            info = new Properties();
        }

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

                if ((null != value && value.length() > 0) && (parameter.length() > 0)) {
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

    /**
     * Converting string tabletType to Topodata.TabletType
     *
     * @param tabletType
     * @return
     */
    public static Topodata.TabletType getTabletType(String tabletType) {
        switch (tabletType.toLowerCase()) {
            case "master":
                return Topodata.TabletType.MASTER;
            case "replica":
                return Topodata.TabletType.REPLICA;
            case "rdonly":
                return Topodata.TabletType.RDONLY;
            default:
                return Topodata.TabletType.UNRECOGNIZED;
        }
    }

}

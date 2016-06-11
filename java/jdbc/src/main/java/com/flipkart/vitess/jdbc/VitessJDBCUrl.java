package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
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

        info = getURLParamProperties(url, info);

        /* URL pattern e.g. jdbc:vitess://username:password@ip1:port1,ip2:port2/keyspace/catalog?
        property1=value1.. */

        final Pattern p = Pattern
            .compile("^jdbc:(vitess)://((\\w+)(:(\\w*))?@)?([^/]*)/([^/?]*)/?(\\w+)?(\\?(\\S+))?");
        final Matcher m = p.matcher(url);
        if (!m.find()) {
            throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
        }
        this.username =
            (m.group(3) == null ? info.getProperty(Constants.Property.USERNAME) : m.group(3));
        // Will add password property once its supported from vitess
        // this.password = (m.group(5) == null ? info.getProperty("password") : m.group(5));
        String postUrl = m.group(6);
        if (null == postUrl) {
            throw new SQLException(Constants.SQLExceptionMessages.MALFORMED_URL);
        }
        this.keyspace = m.group(7);
        this.catalog = (m.group(8) == null ? m.group(7) : m.group(8));
        this.hostInfos = getURLHostInfos(postUrl);

        String tabletType = info.getProperty(Constants.Property.TABLET_TYPE);
        if (null == tabletType) {
            tabletType = Constants.DEFAULT_TABLET_TYPE;
        }

        this.tabletType = getTabletType(tabletType);

        String executeType = info.getProperty(Constants.Property.EXECUTE_TYPE);

        this.executeType = executeType;
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
     * @param url
     * @param info
     * @return
     */

    private static Properties getURLParamProperties(String url, Properties info) {

     /*
      * Parse parameters after the ? in the URL
		 */
        if (null == info) {
            info = new Properties();
        }
        int index = url.indexOf("?"); //$NON-NLS-1$

        if (index != -1) {
            String paramString = url.substring(index + 1, url.length());

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

                if ((value != null && value.length() > 0) && (parameter != null
                    && parameter.length() > 0)) {
                    try {
                        info.put(parameter, URLDecoder.decode(value, "UTF-8"));
                    } catch (UnsupportedEncodingException | NoSuchMethodError badEncoding) {
                        info.put(parameter, URLDecoder.decode(value));
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
        int index = hostURLs.indexOf("?");
        String hostsString;
        List<HostInfo> hostInfos = new ArrayList<>();
        if (index != -1) {
            hostsString = hostURLs.substring(0, index - 1);
        } else {
            hostsString = hostURLs;
        }
        StringTokenizer stringTokenizer = new StringTokenizer(hostsString, ",");
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

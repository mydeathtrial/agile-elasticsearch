package cloud.agileframework.elasticsearch;


import cloud.agileframework.common.util.http.HttpUtil;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;

import java.net.InetSocketAddress;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Driver implements java.sql.Driver {
    private static Logger log = LoggerFactory.getLogger(Driver.class);
    public static final String URL_PREFIX = "jdbc:elastic://";

    //
    // Register with the DriverManager
    //
    static {
        try {
            java.sql.DriverManager.registerDriver(new Driver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        List<URI> urls = Arrays.stream(url.split(",")).map((s) -> s.startsWith("http") ? s : "http://" + s)
                .map(URI::create)
                .collect(Collectors.toList());
        InetSocketAddress[] inetSocketAddresses = urls.stream().map(a -> InetSocketAddress.createUnresolved(a.getHost(), a.getPort())).toArray(InetSocketAddress[]::new);

        ClientConfiguration.MaybeSecureClientConfigurationBuilder builder = ClientConfiguration.builder()
                .connectedTo(inetSocketAddresses);

        if ("https".equals(urls.get(0).getScheme())) {
            try {
                builder.usingSsl(HttpUtil.createIgnoreVerifySSL(SSLConnectionSocketFactory.SSL), NoopHostnameVerifier.INSTANCE)
                        .withBasicAuth(info.getProperty("user"), info.getProperty("password")).build();
            } catch (KeyManagementException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        return new ConnectionEnhanceImpl(RestClients.create(builder.build()).lowLevelRest(),
                url,
                info);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // TODO - implement this?
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}

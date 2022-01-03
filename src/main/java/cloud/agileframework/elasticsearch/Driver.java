package cloud.agileframework.elasticsearch;

import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.internal.util.UrlParser;
import com.amazon.opendistroforelasticsearch.jdbc.logging.Logger;
import com.amazon.opendistroforelasticsearch.jdbc.logging.NoOpLogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Driver extends com.amazon.opendistroforelasticsearch.jdbc.Driver implements java.sql.Driver {
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
        url = url.replace(URL_PREFIX, UrlParser.URL_PREFIX);
        ConnectionConfig connectionConfig = ConnectionConfig.builder()
                .setUrl(url)
                .setProperties(info)
                .build();
        Logger log = initLog(connectionConfig);
        log.debug(String.format("connect (%s, %s)", url, info == null ? "null" : info.toString()));
        log.debug(String.format("Opening connection using config: %s", connectionConfig));
        return new ConnectionEnhanceImpl(connectionConfig, log);
    }

    static Logger initLog(ConnectionConfig connectionConfig) {
        // precedence:
        // 1. explicitly supplied logWriter
        // 2. logOutput property
        // 3. DriverManager logWriter
        if (connectionConfig.getLogWriter() != null) {

            return com.amazon.opendistroforelasticsearch.jdbc.logging.LoggerFactory.getLogger(connectionConfig.getLogWriter(), connectionConfig.getLogLevel());

        } else if (connectionConfig.getLogOutput() != null) {

            return com.amazon.opendistroforelasticsearch.jdbc.logging.LoggerFactory.getLogger(connectionConfig.getLogOutput(), connectionConfig.getLogLevel());

        } else if (DriverManager.getLogWriter() != null) {

            return com.amazon.opendistroforelasticsearch.jdbc.logging.LoggerFactory.getLogger(DriverManager.getLogWriter(), connectionConfig.getLogLevel());

        } else {

            return NoOpLogger.INSTANCE;
        }
    }
}

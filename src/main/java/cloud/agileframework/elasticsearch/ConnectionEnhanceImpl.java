package cloud.agileframework.elasticsearch;

import cloud.agileframework.elasticsearch.protocol.AgileJsonHttpProtocolFactory;
import cloud.agileframework.elasticsearch.transport.AgileTransportFactory;
import com.amazon.opendistroforelasticsearch.jdbc.ConnectionImpl;
import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.logging.Logger;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.Protocol;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 连接
 */
public class ConnectionEnhanceImpl extends ConnectionImpl {

    public ConnectionEnhanceImpl(ConnectionConfig connectionConfig, Logger log) throws SQLException {
        super(connectionConfig, AgileTransportFactory.INSTANCE, AgileJsonHttpProtocolFactory.INSTANCE, log);
    }

    @Override
    public Statement createStatementX() {
        return new AgileStatement(this, getLog());
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {

        getLog().debug(() -> logEntry("prepareStatment (%s)", sql));
        if (isClosedX()) {
            logAndThrowSQLException(getLog(), new SQLException("Connection is closed."));
        }
        PreparedStatement pst = new AgilePreparedStatement(this, sql, getLog());
        getLog().debug(() -> logExit("prepareStatement", pst));
        return pst;
    }
}

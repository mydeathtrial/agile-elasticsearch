package cloud.agileframework.elasticsearch;

import cloud.agileframework.elasticsearch.protocol.EnhanceProtocol;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.amazon.opendistroforelasticsearch.jdbc.PreparedStatementImpl;
import com.amazon.opendistroforelasticsearch.jdbc.logging.Logger;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.exceptions.ResponseException;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class AgilePreparedStatement extends PreparedStatementImpl {
    private final ConnectionEnhanceImpl connection;
    private final List<SQLStatement> batch = Lists.newArrayList();
    
    public AgilePreparedStatement(ConnectionEnhanceImpl connection, String sql, Logger log) throws SQLException {
        super(connection, sql, log);
        this.connection = connection;
    }

    @Override
    public int executeUpdate() throws SQLException {
        // JDBC Spec: A ResultSet object is automatically closed when the Statement
        // object that generated it is closed, re-executed, or used to retrieve the
        // next result from a sequence of multiple results.
        closeResultSet(false);

        try {
            JdbcResponse response = ((EnhanceProtocol) (connection.getProtocol())).executeUpdate(JdbcRequest.of(sql));
            return response.count();

        } catch (ResponseException | IOException ex) {
            logAndThrowSQLException(log, new SQLException("Error executing query", ex));
        }
        return 0;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // JDBC Spec: A ResultSet object is automatically closed when the Statement
        // object that generated it is closed, re-executed, or used to retrieve the
        // next result from a sequence of multiple results.
        closeResultSet(false);

        try {
            JdbcResponse response = ((EnhanceProtocol) (connection.getProtocol())).executeUpdate(JdbcRequest.of(batch));
            return response.counts();

        } catch (ResponseException | IOException ex) {
            logAndThrowSQLException(log, new SQLException("Error executing query", ex));
        }
        clearBatch();
        return new int[0];
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        SQLStatement statement = JdbcRequest.to(sql);
        if (statement instanceof SQLInsertStatement) {
            batch.add(statement);
        }
        super.addBatch(sql);
    }

    @Override
    public void clearBatch() {
        batch.clear();
    }
}

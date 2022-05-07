package cloud.agileframework.elasticsearch;

import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class AgileStatement extends BaseStatement {
    private final List<SQLStatement> batch = Lists.newArrayList();

    public AgileStatement(ConnectionEnhanceImpl connection) {
        super(connection);
    }

    public List<SQLStatement> getBatch() {
        return batch;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        JdbcResponse result;
        try {
            result = JdbcRequest.send(sql, this);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return result.count();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        JdbcResponse result;
        try {
            result = JdbcRequest.send(sql, this);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return result.success();
    }


    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        JdbcResponse result;
        try {
            result = JdbcRequest.send(sql, this);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return result.resultSet();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        JdbcResponse result;
        try {
            result = JdbcRequest.of(batch, this).send();
        } catch (IOException e) {
            throw new SQLException(e);
        }
        clearBatch();
        return result.counts();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        SQLStatement statement = JdbcRequest.to(sql);
        if (statement instanceof SQLInsertStatement) {
            batch.add(statement);
        }
    }

    @Override
    public void clearBatch() {
        batch.clear();
    }

    protected void checkOpen() throws SQLException {
        if (!getConnection().getRestClient().isRunning()) {
            throw new SQLException("statement is closed");
        }
    }
}

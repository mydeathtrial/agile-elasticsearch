package cloud.agileframework.elasticsearch;

import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * 连接
 */
public class ConnectionEnhanceImpl implements Connection {
    private final RestClient restClient;
    private final AgileDatabaseMetaData databaseMetaData;


    private boolean autoCommit = true;
    private String catalog;
    private int transactionIsolation;
    private int holdability;
    private Map<String, Class<?>> typeMap = new HashMap<>();
    private SQLWarning warnings;
    private boolean readOnly;

    private String url;
    private Properties info;

    public ConnectionEnhanceImpl(RestClient restClient, String url, Properties info) {
        this.restClient = restClient;
        this.url = url;
        this.info = info;
        databaseMetaData = new AgileDatabaseMetaData(info, url);
    }

    public String getUrl() {
        return url;
    }

    public Properties getConnectProperties() {
        return info;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkState();

        return new AgileStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkState();

        return new AgilePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkState();

        return new AgileCallableStatement(this, sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkState();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        try {
            restClient.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !restClient.isRunning();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return databaseMetaData;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkState();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }

    public void checkState() throws SQLException {
        isClosed();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkState();
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.warnings = null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        AgileStatement statement = new AgileStatement(this);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        AgilePreparedStatement statement = new AgilePreparedStatement(this, sql);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        return statement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        AgileCallableStatement statement = new AgileCallableStatement(this, sql);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        return statement;
    }

    public void setWarnings(SQLWarning warnings) {
        this.warnings = warnings;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return typeMap;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.typeMap = map;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() {
        return holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        AgileStatement statement = new AgileStatement(this);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        AgilePreparedStatement statement = new AgilePreparedStatement(this, sql);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        statement.setResultSetHoldability(resultSetHoldability);
        return statement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        AgileCallableStatement statement = new AgileCallableStatement(this, sql);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        statement.setResultSetHoldability(resultSetHoldability);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return databaseMetaData.getProperties().getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return databaseMetaData.getProperties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return databaseMetaData.getSchemaTerm();
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }

        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public RestClient getRestClient() {
        return restClient;
    }
}

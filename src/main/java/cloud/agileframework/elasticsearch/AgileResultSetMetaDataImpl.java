package cloud.agileframework.elasticsearch;


import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class AgileResultSetMetaDataImpl implements ResultSetMetaData {
    private AgileResultSet resultSet;
    private Cursor.Schema schema;

    public AgileResultSetMetaDataImpl(AgileResultSet resultSet, Cursor.Schema schema) {
        this.resultSet = resultSet;
        this.schema = schema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema.getNumberOfColumns();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkAccessible(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumnIndex(column);
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkAccessible(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkAccessible(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkAccessible(column);
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getEsType().isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getEsType().getDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getTableSchemaName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getTableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkAccessible(column);
        return schema.getColumnMetaData(column - 1).getCatalogName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkAccessible(column);
        return schema.getElasticsearchType(column - 1).getJdbcType().getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkAccessible(column);
        return schema.getElasticsearchType(column - 1).getJdbcType().getName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkAccessible(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkAccessible(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkAccessible(column);
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        checkAccessible(column);
        return schema.getElasticsearchType(column - 1).getJavaClassName();
    }

    private void checkAccessible(int columnIndex) throws SQLException {
        checkOpen();
        checkColumnIndex(columnIndex);
    }

    private void checkOpen() throws SQLException {
        resultSet.checkOpen();
    }

    private void checkColumnIndex(int columnIndex) throws SQLException {
        resultSet.checkColumnIndex(columnIndex);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        } else {
            throw new SQLException("ResultSetMetaData of type [" + this.getClass().getName() + "] cannot be unwrapped as [" + iface.getName() + "]");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}

package cloud.agileframework.elasticsearch;


import cloud.agileframework.elasticsearch.types.TypeConverter;
import cloud.agileframework.elasticsearch.types.TypeConverters;
import lombok.Data;
import org.slf4j.Logger;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AgileResultSet implements ResultSet {
    private BaseStatement statement;
    protected Cursor cursor;
    private boolean open = false;
    private boolean wasNull = false;
    private boolean afterLast = false;
    private boolean beforeFirst = true;
    private Logger log;

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        } else {
            throw new SQLException("ResultSet of type [" + this.getClass().getName() + "] cannot be unwrapped as [" + iface.getName() + "]");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Data
    public static class Column {
        private String name;
        private String type;
        private String label;
    }

    public AgileResultSet(BaseStatement statement, List<Column> columnDescriptors,
                          List<List<Object>> dataRows, Logger log) {
        this.statement = statement;
        this.log = log;

        final Cursor.Schema schema;
        try {
            schema = new Cursor.Schema(columnDescriptors
                    .stream()
                    .map(a -> {
                        try {
                            return new ColumnMetaData(a);
                        } catch (SQLFeatureNotSupportedException e) {
                            return null;
                        }
                    }).filter(Objects::nonNull)
                    .collect(Collectors.toList()));

            List<Cursor.Row> rows = getRowsFromDataRows(dataRows);

            this.cursor = new Cursor(schema, rows);
            this.open = true;

        } catch (Exception ex) {
            log.error("AgileResultSet Error", new SQLException("Exception creating a ResultSet.", ex));
        }

    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        boolean next = cursor.next();

        if (next) {
            beforeFirst = false;
        } else {
            afterLast = true;
        }
        return next;
    }

    private List<Cursor.Row> getRowsFromDataRows(List<List<Object>> dataRows) {
        return dataRows
                .stream()
                .map(Cursor.Row::new)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws SQLException {
        log.debug("close()");
        closeX(true);
        log.debug("close");
    }

    protected void closeX(boolean closeStatement) throws SQLException {
        cursor = null;
        open = false;
        if (statement != null) {
            statement.close();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        log.debug("getString {}", columnIndex);
        checkCursorOperationPossible();
        String value = getStringX(columnIndex);
        log.debug("getString {}", value);
        return value;
    }

    private String getStringX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        log.debug("getBoolean {}", columnIndex);
        checkCursorOperationPossible();
        boolean value = getBooleanX(columnIndex);
        log.debug("getBoolean {}", value);
        return value;
    }

    private boolean getBooleanX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, Boolean.class);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        log.debug("getByte {}", columnIndex);
        checkCursorOperationPossible();
        byte value = getByteX(columnIndex);
        log.debug("getByte {}", value);
        return value;
    }

    private byte getByteX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, Byte.class);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        log.debug("getShort {}", columnIndex);
        checkCursorOperationPossible();
        short value = getShortX(columnIndex);
        log.debug("getShort {}", value);
        return value;
    }

    private short getShortX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, Short.class);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        log.debug("getInt {}", columnIndex);
        checkCursorOperationPossible();
        int value = getIntX(columnIndex);
        log.debug("getInt {}", value);
        return value;
    }

    private int getIntX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, Integer.class);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        log.debug("getLong {}", columnIndex);
        checkCursorOperationPossible();
        long value = getLongX(columnIndex);
        log.debug("getLong {}", value);
        return value;
    }

    private long getLongX(int columnIndex) throws SQLException {
        checkCursorOperationPossible();
        return getObjectX(columnIndex, Long.class);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        log.debug("getFloat {}", columnIndex);
        checkCursorOperationPossible();
        float value = getFloatX(columnIndex);
        log.debug("getFloat {}", value);
        return value;
    }

    private float getFloatX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, Float.class);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        log.debug("getDouble({})", columnIndex);
        checkCursorOperationPossible();
        double value = getDoubleX(columnIndex);
        log.debug("getDouble {}", value);
        return value;
    }

    private double getDoubleX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, Double.class);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        log.debug("getBigDecimal({} {})", columnIndex, scale);
        checkCursorOperationPossible();
        BigDecimal value = getBigDecimalX(columnIndex, scale);
        log.debug("getBigDecimal {}", value);
        return value;
    }

    private BigDecimal getBigDecimalX(int columnIndex, int scale) throws SQLException {
        checkOpen();
        // TODO - add support?
        throw new SQLFeatureNotSupportedException("BigDecimal is not supported");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        log.debug("getBytes({})", columnIndex);
        checkCursorOperationPossible();
        byte[] value = getBytesX(columnIndex);
        log.debug("getBytes" + String.format("%s, length(%s)", value, value != null ? value.length : 0));
        return value;
    }

    private byte[] getBytesX(int columnIndex) throws SQLException {
        // TODO - add ByteArrayType support
        return getObjectX(columnIndex, byte[].class);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        log.debug("getDate({})", columnIndex);
        checkCursorOperationPossible();
        Date value = getDateX(columnIndex, null);
        log.debug("getDate({})", value);
        return value;
    }

    private Date getDateX(int columnIndex, Calendar calendar) throws SQLException {
        Map<String, Object> conversionParams = null;
        if (calendar != null) {
            conversionParams = new HashMap<>();
            conversionParams.put("calendar", calendar);
        }
        return getObjectX(columnIndex, Date.class, conversionParams);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        log.debug("getTime({})", columnIndex);
        checkCursorOperationPossible();
        Time value = getTimeX(columnIndex);
        log.debug("getTime({})", value);
        return value;
    }

    private Time getTimeX(int columnIndex) throws SQLException {
        // TODO - add/check support
        return getObjectX(columnIndex, Time.class);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        log.debug("getTimestamp({})", columnIndex);
        checkCursorOperationPossible();
        Timestamp value = getTimestampX(columnIndex, null);
        log.debug("getTimestamp({})", value);
        return value;
    }

    private Timestamp getTimestampX(int columnIndex, Calendar calendar) throws SQLException {
        Map<String, Object> conversionParams = null;
        if (calendar != null) {
            conversionParams = new HashMap<>();
            conversionParams.put("calendar", calendar);
        }
        return getObjectX(columnIndex, Timestamp.class, conversionParams);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        log.debug("getString({})", columnLabel);
        checkCursorOperationPossible();
        String value = getStringX(getColumnIndex(columnLabel));
        log.debug("getString({})", value);
        return value;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        log.debug("getBoolean({})", columnLabel);
        checkCursorOperationPossible();
        boolean value = getBooleanX(getColumnIndex(columnLabel));
        log.debug("getBoolean({})", value);
        return value;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        log.debug("getByte({})", columnLabel);
        checkCursorOperationPossible();
        byte value = getByteX(getColumnIndex(columnLabel));
        log.debug("getByte({})", value);
        return value;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        log.debug("getShort({})", columnLabel);
        checkCursorOperationPossible();
        short value = getShortX(getColumnIndex(columnLabel));
        log.debug("getShort({})", value);
        return value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        log.debug("getInt({})", columnLabel);
        checkCursorOperationPossible();
        int value = getIntX(getColumnIndex(columnLabel));
        log.debug("getInt({})", value);
        return value;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        log.debug("getLong({})", columnLabel);
        checkCursorOperationPossible();
        long value = getLongX(getColumnIndex(columnLabel));
        log.debug("getLong({})", value);
        return value;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        log.debug("getFloat({})", columnLabel);
        checkCursorOperationPossible();
        float value = getFloatX(getColumnIndex(columnLabel));
        log.debug("getFloat({})", value);
        return value;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        log.debug("getDouble({})", columnLabel);
        checkCursorOperationPossible();
        double value = getDoubleX(getColumnIndex(columnLabel));
        log.debug("getDouble({})", value);
        return value;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        log.debug("getBigDecimal({})", columnLabel);
        checkCursorOperationPossible();
        BigDecimal value = getBigDecimalX(getColumnIndex(columnLabel), scale);
        log.debug("getBigDecimal({})", value);
        return value;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        log.debug("getBytes({})", columnLabel);
        checkCursorOperationPossible();
        byte[] value = getBytesX(getColumnIndex(columnLabel));
        log.debug("getBytes " + String.format("%s, length(%s)", value, value != null ? value.length : 0));

        return value;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        log.debug("getDate({})", columnLabel);
        checkCursorOperationPossible();
        Date value = getDateX(getColumnIndex(columnLabel), null);
        log.debug("getDate({})", value);
        return value;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        log.debug("getTime({})", columnLabel);
        checkCursorOperationPossible();
        Time value = getTimeX(getColumnIndex(columnLabel));
        log.debug("getTime({})", value);
        return value;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        log.debug("getTimestamp({})", columnLabel);
        checkCursorOperationPossible();
        Timestamp value = getTimestampX(getColumnIndex(columnLabel), null);
        log.debug("getTimestamp({})", value);
        return value;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor name is not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return new AgileResultSetMetaDataImpl(this, cursor.getSchema());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        log.debug("getObject({})", columnIndex);
        checkCursorOperationPossible();
        Object value = getObjectX(columnIndex);
        log.debug("getObject " + (value != null ? "(" + value.getClass().getName() + ") " + value : "null"));
        return value;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        log.debug("getObject({})", columnLabel);
        checkCursorOperationPossible();
        Object value = getObjectX(getColumnIndex(columnLabel));
        log.debug("getObject " + (value != null ? "(" + value.getClass().getName() + ") " + value : "null"));
        return value;
    }

    private Object getObjectX(int columnIndex) throws SQLException {
        return getObjectX(columnIndex, (Class<Object>) null);
    }

    protected <T> T getObjectX(int columnIndex, Class<T> javaClass) throws SQLException {
        return getObjectX(columnIndex, javaClass, null);
    }

    protected <T> T getObjectX(int columnIndex, Class<T> javaClass, Map<String, Object> conversionParams) throws SQLException {
        Object value = getColumn(columnIndex);
        TypeConverter tc = TypeConverters.getInstance(getColumnMetaData(columnIndex).getEsType().getJdbcType());
        return tc.convert(value, javaClass, conversionParams);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkOpen();
        return getColumnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        // TODO - add support?
        checkOpen();
        throw new SQLFeatureNotSupportedException("BigDecimal is not supported");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        // TODO - add support?
        checkOpen();
        throw new SQLFeatureNotSupportedException("BigDecimal is not supported");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkOpen();
        return beforeFirst;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkOpen();
        return afterLast;
    }

    private boolean isBeforeFirstX() throws SQLException {
        return beforeFirst;
    }

    private boolean isAfterLastX() throws SQLException {
        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public void afterLast() throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean first() throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean last() throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public int getRow() throws SQLException {
        // not supported yet
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean previous() throws SQLException {
        checkOpen();
        throw new SQLDataException("Illegal operation on ResultSet of type TYPE_FORWARD_ONLY");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLDataException("The ResultSet only supports FETCH_FORWARD direction");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        // no-op
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        log.debug("getObject({}, {})", columnIndex, map);
        Object value = getObjectX(columnIndex, map);

        log.debug("getObject({})", value);
        return value;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref is not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob is not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob is not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array is not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        log.debug("getObject({}, {})", columnLabel, map);
        Object value = getObjectX(getColumnIndex(columnLabel), map);
        log.debug("getObject({})", value);
        return value;
    }

    private Object getObjectX(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        String columnSQLTypeName = null;
        Class targetClass = null;
        if (map != null) {
            columnSQLTypeName = getColumnMetaData(columnIndex).getEsType().getJdbcType().getName();
            targetClass = map.get(columnSQLTypeName);
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Column SQL Type is: %s. Target class retrieved from custom mapping: %s",
                    columnSQLTypeName, targetClass));
        }
        return getObjectX(columnIndex, targetClass);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref is not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob is not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob is not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array is not supported");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        log.debug("getDate({}, {})", columnIndex, (cal == null ? "null" : "Calendar TZ= " + cal.getTimeZone()));
        checkCursorOperationPossible();
        Date value = getDateX(columnIndex, cal);
        log.debug("getDate({})", value);
        return value;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        log.debug("getDate({}, {})", columnLabel, (cal == null ? "null" : "Calendar TZ= " + cal.getTimeZone()));
        checkCursorOperationPossible();
        Date value = getDateX(getColumnIndex(columnLabel), cal);
        log.debug("getDate({})", value);
        return value;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO - implement?
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        // TODO - implement?
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        log.debug("getTimestamp({}, {})", columnIndex, (cal == null ? "null" : "Calendar TZ= " + cal.getTimeZone()));

        checkCursorOperationPossible();
        Timestamp value = getTimestampX(columnIndex, cal);
        log.debug("getTimestamp({})", value);
        return value;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        log.debug("getTimestamp({}, {})", columnLabel, (cal == null ? "null" : "Calendar TZ= " + cal.getTimeZone()));

        checkCursorOperationPossible();
        Timestamp value = getTimestampX(getColumnIndex(columnLabel), cal);
        log.debug("getTimestamp({})", value);
        return value;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        // TODO - implement
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        // TODO - implement
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !open;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        log.debug("getNString({})", columnIndex);
        String value = getStringX(columnIndex);
        log.debug("getNString({})", value);
        return value;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        log.debug("getNString({})", columnLabel);
        String value = getStringX(getColumnIndex(columnLabel));
        log.debug("getNString({})", value);
        return value;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw updatesNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        log.debug("getObject({}, {})", columnIndex, type);
        T value = getObjectX(columnIndex, type);
        log.debug("getObject({})", value);
        return value;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        log.debug("getObject({}, {})", columnLabel, type);
        T value = getObjectX(getColumnIndex(columnLabel), type);
        log.debug("getObject({})", value);
        return value;
    }

    private int getColumnIndex(String columnLabel) throws SQLException {
        Integer index = cursor.findColumn(columnLabel);

        if (index == null)
            log.error("AgileResultSet Error", new SQLDataException("Column '" + columnLabel + "' not found."));

        // +1 to adjust for JDBC indices that start from 1
        return index + 1;
    }

    protected Object getColumn(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        Object columnData = getColumnFromCursor(columnIndex);

        wasNull = (columnData == null);
        return columnData;
    }

    protected Object getColumnFromCursor(int columnIndex) {
        return cursor.getColumn(columnIndex - 1);
    }

    private ColumnMetaData getColumnMetaData(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        return cursor.getSchema().getColumnMetaData(columnIndex - 1);
    }

    protected void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > cursor.getColumnCount())
            log.error("AgileResultSet Error", new SQLDataException("Column index out of range."));
    }

    protected void checkCursorOperationPossible() throws SQLException {
        checkOpen();
        checkValidCursorPosition();
    }

    protected void checkOpen() throws SQLException {
        if (isClosed()) {
            log.error("AgileResultSet Error", new SQLException("ResultSet closed."));
        }
    }

    private void checkValidCursorPosition() throws SQLException {
        if (isBeforeFirstX())
            log.error("AgileResultSet Error", new SQLNonTransientException("Illegal operation before start of ResultSet."));
        else if (isAfterLastX())
            log.error("AgileResultSet Error", new SQLNonTransientException("Illegal operation before start of ResultSet."));
    }

    private SQLException updatesNotSupportedException() {
        return new SQLFeatureNotSupportedException("Updates are not supported");
    }

}

package cn.edu.ruc.kafka.connection;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * ConsumerConnection and ProducerConnection should extends this class.
 * This class implements the unused methods so that other sub-classes can be shorter and more clear.
 * @author Bian Haoqiong
 * @version 0.0.1
 * @see java.sql.Connection
 */
public abstract class Connection implements java.sql.Connection
{
    /*
    @Override
    public Statement createStatement() throws SQLException
    {
        return null;
    }
    */

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException
    {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        if (autoCommit)
        {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException
    {
        return false;
    }

    @Override
    public void commit() throws SQLException
    {

    }

    @Override
    public void rollback() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public void close() throws SQLException
    {

    }
    */

    /*
    @Override
    public boolean isClosed() throws SQLException
    {
        return false;
    }
    */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {

    }

    @Override
    public boolean isReadOnly() throws SQLException
    {
        return false;
    }
    */
    @Override
    public void setCatalog(String catalog) throws SQLException
    {

    }

    @Override
    public String getCatalog() throws SQLException
    {
        return "kafka";
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException
    {

    }

    @Override
    public cn.edu.ruc.kafka.statement.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public cn.edu.ruc.kafka.statement.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public boolean isValid(int timeout) throws SQLException
    {
        return false;
    }
    */


    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {

    }

    @Override
    public String getClientInfo(String name) throws SQLException
    {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException
    {
        return null;
    }


    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getSchema() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void abort(Executor executor) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {

    }

    @Override
    public int getNetworkTimeout() throws SQLException
    {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }
}

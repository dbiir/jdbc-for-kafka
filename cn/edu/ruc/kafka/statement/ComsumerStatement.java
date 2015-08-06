package cn.edu.ruc.kafka.statement;

import cn.edu.ruc.kafka.connection.ConsumerConnection;
import cn.edu.ruc.kafka.query.QueryExecutor;
import cn.edu.ruc.kafka.query.QueryExecutorFactory;
import cn.edu.ruc.kafka.resultset.ResultSet;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * @author Hank Bian
 * @version 0.0.1
 * @see cn.edu.ruc.kafka.statement.Statement
 */
public class ComsumerStatement extends Statement
{
    private ConsumerConnection connection = null;
    private ResultSet resultSet = null;


    public ComsumerStatement (ConsumerConnection connection)
    {
        this.connection = connection;

    }

    /**
     *
     * @param sql the query, contains the parameters.
     * @return
     * @throws SQLException
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException
    {
        String[] tokens = sql.split("\\s+");
        String executorName = tokens[0];
        QueryExecutor executor = QueryExecutorFactory.Instance().getExecutor(executorName);
        this.resultSet = executor.execute(this.connection, tokens);
        return this.resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * this method will close the corresponding consumerConnection.
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException
    {
        this.connection.close();
    }

    @Override
    public ResultSet getResultSet() throws SQLException
    {
        return this.resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException
    {
        return 0;
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return this.connection;
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return this.connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException
    {

    }

    @Override
    public boolean isPoolable() throws SQLException
    {
        return true;
    }
}

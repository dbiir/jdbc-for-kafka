package cn.edu.ruc.kafka.resultset;

import kafka.javaapi.FetchResponse;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public class MessageResultSet extends ResultSet
{
    private String topic = null;
    private int partition = -1;
    private FetchResponse response = null;
    private ResultSetMetaData metaData = null;
    private Iterator<MessageAndOffset> iterator = null;
    private MessageAndOffset message = null;

    public MessageResultSet (String topic, int partition, FetchResponse response, ResultSetMetaData metaData)
    {
        this.topic = topic;
        this.partition = partition;
        this.response = response;
        this.metaData = metaData;
        this.iterator = this.response.messageSet(this.topic, this.partition).iterator();
    }

    @Override
    public boolean next() throws SQLException
    {
        if (this.iterator.hasNext())
        {
            this.message = this.iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException
    {
        this.response = null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return this.metaData;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException
    {
        if (this.message == null)
        {
            throw new SQLException("invalid row index: -1");
        }
        if (this.metaData.getColumnName(columnIndex).equalsIgnoreCase("offset"))
        {

            return this.message.offset();
        }
        else if (this.metaData.getColumnName(columnIndex).equalsIgnoreCase("nextOffset"))
        {

            return this.message.nextOffset();
        }
        throw new SQLException("invalid column index: " + columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException
    {
        if (this.message == null)
        {
            throw new SQLException("invalid row index: -1");
        }
        if (columnLabel.equalsIgnoreCase("offset"))
        {

            return this.message.offset();
        }
        else if (columnLabel.equalsIgnoreCase("nextOffset"))
        {

            return this.message.nextOffset();
        }
        throw new SQLException("invalid column label: " + columnLabel);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException
    {
        if (this.message == null)
        {
            throw new SQLException("invalid row index: -1");
        }
        if (this.metaData.getColumnName(columnIndex).equalsIgnoreCase("message"))
        {
            ByteBuffer payload = this.message.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            return bytes;
        }
        throw new SQLException("invalid column index: " + columnIndex);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException
    {
        if (this.message == null)
        {
            throw new SQLException("invalid row index: -1");
        }
        if (columnLabel.equalsIgnoreCase("message"))
        {
            ByteBuffer payload = this.message.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            return bytes;
        }
        throw new SQLException("invalid column label: " + columnLabel);
    }
}

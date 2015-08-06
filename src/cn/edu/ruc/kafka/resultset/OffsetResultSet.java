package cn.edu.ruc.kafka.resultset;

import java.sql.SQLException;

/**
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public class OffsetResultSet extends ResultSet
{
    private long[] offsets = null;
    private ResultSetMetaData resultSetMetaData = null;
    private int index = -1;

    public OffsetResultSet (long[] offsets, ResultSetMetaData metaData)
    {
        this.offsets = offsets;
        this.resultSetMetaData = metaData;
        this.index = -1;
    }

    @Override
    public boolean next() throws SQLException
    {
        index++;
        if (index < this.offsets.length)
        {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException
    {
        this.offsets = null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return this.resultSetMetaData;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException
    {
        if (this.index < 0 || this.index >= this.offsets.length)
        {
            throw new SQLException("invalid row index: " + this.index);
        }
        if (this.resultSetMetaData.getColumnName(columnIndex).equalsIgnoreCase("offset"))
        {

            return this.offsets[this.index];
        }
        throw new SQLException("invalid column index: " + columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException
    {
        if (this.index < 0 || this.index >= this.offsets.length)
        {
            throw new SQLException("invalid row index: " + this.index);
        }
        if (columnLabel.equalsIgnoreCase("offset"))
        {

            return this.offsets[this.index];
        }
        throw new SQLException("invalid column label: " + columnLabel);
    }
}

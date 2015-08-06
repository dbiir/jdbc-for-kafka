package cn.edu.ruc.kafka.resultset;

import kafka.javaapi.PartitionMetadata;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public class PartitionMetadataResultSet extends ResultSet
{
    private List<PartitionMetadata> partsMetadata = null;
    private ResultSetMetaData resultSetMetaData = null;
    private int index = -1;

    public PartitionMetadataResultSet (List<PartitionMetadata> partsMetadata, ResultSetMetaData resultSetMetaData)
    {
        this.partsMetadata = partsMetadata;
        this.resultSetMetaData = resultSetMetaData;
        this.index = -1;
    }

    @Override
    public boolean next() throws SQLException
    {
        index++;
        if (index < this.partsMetadata.size())
        {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException
    {
        this.partsMetadata.clear();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return this.resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException
    {
        if (this.index < 0 || this.index >= this.partsMetadata.size())
        {
            throw new SQLException("invalid row index: " + this.index);
        }
        if (this.resultSetMetaData.getColumnName(columnIndex).equalsIgnoreCase("leader"))
        {
            return this.partsMetadata.get(this.index).leader();
        }
        throw new SQLException("invalid column index: " + columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException
    {
        if (this.index < 0 || this.index >= this.partsMetadata.size())
        {
            throw new SQLException("invalid row index: " + this.index);
        }
        if (columnLabel.equalsIgnoreCase("leader"))
        {
            return this.partsMetadata.get(this.index).leader();
        }
        throw new SQLException("invalid column label: " + columnLabel);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException
    {
        if (this.index < 0 || this.index >= this.partsMetadata.size())
        {
            throw new SQLException("invalid row index: " + this.index);
        }
        if (this.resultSetMetaData.getColumnName(columnIndex).equalsIgnoreCase("partitionId"))
        {

            return this.partsMetadata.get(this.index).partitionId();
        }
        else if (this.resultSetMetaData.getColumnName(columnIndex).equalsIgnoreCase("sizeInBytes"))
        {
            return this.partsMetadata.get(this.index).sizeInBytes();
        }
        throw new SQLException("invalid column index: " + columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException
    {
        if (this.index < 0 || this.index >= this.partsMetadata.size())
        {
            throw new SQLException("invalid row index: " + this.index);
        }
        if (columnLabel.equalsIgnoreCase("partitionId"))
        {

            return this.partsMetadata.get(this.index).partitionId();
        }
        else if (columnLabel.equalsIgnoreCase("sizeInBytes"))
        {
            return this.partsMetadata.get(this.index).sizeInBytes();
        }
        throw new SQLException("invalid column label: " + columnLabel);
    }
}

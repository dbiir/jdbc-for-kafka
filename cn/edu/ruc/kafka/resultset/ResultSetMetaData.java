package cn.edu.ruc.kafka.resultset;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Hank Bian
 * @version 0.0.1
 * @see java.sql.ResultSetMetaData
 */
public class ResultSetMetaData implements java.sql.ResultSetMetaData
{
    private List<String> columns = null;
    private List<Integer> types = null;
    private List<String> typeClassNames = null;

    public ResultSetMetaData(List<String> columns, List<Integer> types)
    {
        this.columns = columns;
        this.types = types;
    }

    public ResultSetMetaData(List<String> columns, List<Integer> types, List<String> typeClassNames)
    {
        this(columns, types);
        this.typeClassNames = typeClassNames;
    }

    @Override
    public int getColumnCount() throws SQLException
    {
        return this.columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException
    {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException
    {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException
    {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException
    {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException
    {
        return this.columns.get(column-1);
    }

    @Override
    public String getColumnName(int column) throws SQLException
    {
        return this.columns.get(column-1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException
    {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException
    {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException
    {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException
    {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException
    {
        return "kafka";
    }

    @Override
    public int getColumnType(int column) throws SQLException
    {
        return types.get(column-1);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException
    {
        if (this.typeClassNames != null)
        {
            this.typeClassNames.get(column-1);
        }
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException
    {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException
    {
        if (this.typeClassNames != null)
        {
            this.typeClassNames.get(column-1);
        }
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
    }
}

package cn.edu.ruc.kafka.query;

import cn.edu.ruc.kafka.connection.ConsumerConnection;
import cn.edu.ruc.kafka.resultset.OffsetResultSet;
import cn.edu.ruc.kafka.resultset.ResultSet;
import cn.edu.ruc.kafka.resultset.ResultSetMetaData;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Bian Haoqiong
 */
public class GetLastOffSet implements QueryExecutor
{
    @Override
    public ResultSet execute(ConsumerConnection connection, String... params) throws SQLException
    {
        int partition = Integer.parseInt(params[1]);
        long time = Long.parseLong(params[2]);

        List<String> columns = new ArrayList<String>();
        List<Integer> types = new ArrayList<Integer>();
        List<String> typeClassNames = new ArrayList<String>();
        columns.add("offset");
        types.add(Types.BIGINT);
        typeClassNames.add("java.lang.Long");
        return new OffsetResultSet(connection.getOffsets(partition, time), new ResultSetMetaData(columns, types, typeClassNames));
    }
}

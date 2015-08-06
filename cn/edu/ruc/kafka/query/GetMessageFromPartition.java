package cn.edu.ruc.kafka.query;

import cn.edu.ruc.kafka.connection.ConsumerConnection;
import cn.edu.ruc.kafka.resultset.MessageResultSet;
import cn.edu.ruc.kafka.resultset.ResultSet;
import cn.edu.ruc.kafka.resultset.ResultSetMetaData;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public class GetMessageFromPartition implements QueryExecutor
{
    @Override
    public ResultSet execute(ConsumerConnection connection, String... params) throws SQLException
    {
        int partition = Integer.parseInt(params[1]);
        long offset = Long.parseLong(params[2]);

        List<String> columns = new ArrayList<String>();
        List<Integer> types = new ArrayList<Integer>();
        List<String> typeClassNames = new ArrayList<String>();
        columns.add("offset");
        types.add(Types.BIGINT);
        typeClassNames.add("java.lang.Long");
        columns.add("nextOffset");
        types.add(Types.BIGINT);
        typeClassNames.add("java.lang.Long");
        columns.add("message");
        types.add(Types.BLOB);
        typeClassNames.add("java.lang.byte[]");
        return new MessageResultSet(connection.getProperties().getProperty("topic"), partition,
                connection.fetch(partition, offset), new ResultSetMetaData(columns, types, typeClassNames));
    }
}

package cn.edu.ruc.kafka.query;

import cn.edu.ruc.kafka.connection.ConsumerConnection;
import cn.edu.ruc.kafka.exception.NoPartitionsException;
import cn.edu.ruc.kafka.exception.TopicNotFoundException;
import cn.edu.ruc.kafka.resultset.PartitionMetadataResultSet;
import cn.edu.ruc.kafka.resultset.ResultSet;
import cn.edu.ruc.kafka.resultset.ResultSetMetaData;
import kafka.javaapi.PartitionMetadata;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Bian Haoqiong
 */
public class GetLeaderPartitionsMetadata implements QueryExecutor
{
    @Override
    public ResultSet execute(ConsumerConnection connection, String... params) throws TopicNotFoundException, NoPartitionsException
    {
            List<PartitionMetadata> metadata = null;
            List<String> columns = new ArrayList<String>();
            List<Integer> types = new ArrayList<Integer>();
            List<String> typeClassNames = new ArrayList<String>();
            columns.add("leader");
            types.add(Types.BINARY);
            typeClassNames.add("kafka.cluster.Broker");
            columns.add("partitionId");
            types.add(Types.INTEGER);
            typeClassNames.add("java.lang.Integer");
            columns.add("sizeInBytes");
            types.add(Types.INTEGER);
            typeClassNames.add("java.lang.Integer");

            if (params.length == 1)
            {
                metadata = connection.getLeaderPartsMetadata();
            }
            else if (params.length == 2)
            {
                metadata = connection.getLeaderPartsMetadata(params[1]);
            }
            return new PartitionMetadataResultSet(metadata, new ResultSetMetaData(columns, types, typeClassNames));
    }
}

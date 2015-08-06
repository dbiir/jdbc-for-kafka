package cn.edu.ruc.kafka.query;

/**
 * here is some template of the supported queries for kafka consumer
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public class Query
{
    private Query()
    {
    }

    public static final String GET_LEADER_PARTITIONS_METADATA = "GetLeaderPartitionsMetadata";

    public static final String GET_LEADER_PARTITIONS_METADATA (String host)
    {
        return "GetLeaderPartitionsMetadata " + host;
    }
    public static final String GET_ALL_PARTITIONS_METADATA = "GetAllPartitionsMetadata";
    public static final String GET_MESSAGE_FROM_PARTITION (int partition, long offset)
    {
        return "GetMessageFromPartition " + partition + " " + offset;
    }
    public static final String GET_LAST_OFFSET (int partition, long time)
    {
        return "GetLastOffSet " + partition + " " + time;
    }

}

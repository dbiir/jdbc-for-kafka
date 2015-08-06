package cn.edu.ruc.kafka.query;

import cn.edu.ruc.kafka.connection.ConsumerConnection;
import cn.edu.ruc.kafka.resultset.ResultSet;

import java.sql.SQLException;

/**
 * this is the interface of query executors of kafka consumer.
 * if you want to add a supported query into this jdbc,
 * you need to implement your own query executor and the corresponding resultset
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public interface QueryExecutor
{
    public ResultSet execute (ConsumerConnection connection, String... params) throws SQLException;
}

package cn.edu.ruc.kafka.test;

import cn.edu.ruc.kafka.query.Query;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.Properties;

/**
 * @author Hank Bian
 * @version 0.0.1
 */
public class KafkaJDBCTest
{
    public static void main(String[] args) throws ClassNotFoundException, SQLException, UnsupportedEncodingException
    {
        Class.forName("cn.edu.ruc.kafka.Driver");
        Properties info = new Properties();
        info.setProperty("role","consumer");
        info.setProperty("client.id", "12345");
        Connection conn = DriverManager.getConnection("kafka://log04:9092/test", info);

        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(Query.GET_LEADER_PARTITIONS_METADATA);
        while (rs.next())
        {
            System.out.println(((Broker)rs.getObject("leader")).host() + ", " + rs.getInt("partitionId") + ", " + rs.getInt("sizeInBytes"));

            ResultSet rs1 = st.executeQuery(Query.GET_LAST_OFFSET(rs.getInt("partitionId"), OffsetRequest.LatestTime()));
            while (rs1.next())
            {
                System.out.println("\t" + rs1.getLong("offset"));
                long offset = rs1.getLong("offset");
                while (true)
                {
                    ResultSet rs2 = st.executeQuery(Query.GET_MESSAGE_FROM_PARTITION(rs.getInt("partitionId"), offset));
                    while (rs2.next())
                    {
                        offset = rs2.getLong("nextOffset");
                        System.out.println("\t\t" + rs2.getLong("offset") + ", " + rs2.getLong("nextOffset") + ", " + new String(rs2.getBytes("message"), "UTF-8"));
                    }
                }
            }
        }
        st.close();
        conn.close();
    }
}

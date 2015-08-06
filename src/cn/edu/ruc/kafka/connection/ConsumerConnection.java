package cn.edu.ruc.kafka.connection;

import cn.edu.ruc.kafka.exception.BrokerConnectException;
import cn.edu.ruc.kafka.exception.FetchingOffsetException;
import cn.edu.ruc.kafka.exception.NoPartitionsException;
import cn.edu.ruc.kafka.exception.TopicNotFoundException;
import cn.edu.ruc.kafka.statement.ComsumerStatement;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.*;

/**
 * This is the java.sql.Connection implementation for kafka consumer.
 * The basic methods for interacting with kafka are contained here.
 * date Aug/05/2015
 * @author Bian Haoqiong
 * @version 0.0.1
 * @see cn.edu.ruc.kafka.connection.Connection
 */
public class ConsumerConnection extends Connection
{
    /**
     * @see kafka.javaapi.consumer.SimpleConsumer
     */
    private SimpleConsumer consumer = null;

    private Properties properties = null;

    private boolean closed = true;

    private String clientName = null;

    /**
     * The param <code>param</code> will be cloned and saved in the attribute <code>properties</code> of this class.
     * A <code>kafka.javaapi.consumer.ConsumerConnector</code> instance will be created in this construction method.
     * @param pros the properties used by kafka consumer to connect to the kafka cluster.
     * @throws BrokerConnectException
     */
    public ConsumerConnection (Properties pros) throws BrokerConnectException
    {
        this.properties = (Properties) pros.clone();

        /**
         * to get this.consumer
         */
        String broker = this.properties.getProperty("broker.host");
        int port = Integer.parseInt(this.properties.getProperty("broker.port", "9092"));
        String topic = this.properties.getProperty("topic");
        int soTimeout = Integer.parseInt(this.properties.getProperty("socket.timeout", "10000"));
        int bufferSize = Integer.parseInt(this.properties.getProperty("buffer.size", "65536"));
        this.clientName = broker + "_" + port + "_" + topic + "_" + this.properties.getProperty("client.id");
        SimpleConsumer tryConsumer = new SimpleConsumer(broker, port, soTimeout, bufferSize, this.clientName);
        if (tryConsumer == null)
        {
            throw new BrokerConnectException("can not connect to broker " + broker + " on port " + port);
        }
        this.consumer = tryConsumer;
        this.closed = false;
    }

    public SimpleConsumer getConsumer()
    {
        return consumer;
    }

    public Properties getProperties()
    {
        return properties;
    }

    /**
     * get the the metadata of all the partitions of the topic.
     * @return
     * @throws TopicNotFoundException
     * @throws NoPartitionsException
     */
    public List<PartitionMetadata> getAllPartsMetadata () throws TopicNotFoundException, NoPartitionsException
    {
        String topic = this.properties.getProperty("topic");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest request = new TopicMetadataRequest(topics);
        TopicMetadataResponse response = consumer.send(request);
        List<TopicMetadata> topicsMetadata = response.topicsMetadata();
        if (topicsMetadata == null || topicsMetadata.size() == 0)
        {
            throw new TopicNotFoundException("topic " + topic + " not found.");
        }
        List<PartitionMetadata> allPartsMetadata = topicsMetadata.get(0).partitionsMetadata();
        if (allPartsMetadata == null || allPartsMetadata.size() == 0)
        {
            throw new NoPartitionsException("no partitions in topic " + topic);
        }
        return allPartsMetadata;
    }

    /**
     * get the metadata of the partitions, of which the leaders are located on the specified kafka broker.
     * Notice that the leaders of the partitions may change during the interval between
     * calling this method and using the returned metadata. So you must use the returned metadata asap.
     * @param host the hostname or ip of the kafka broker.
     * @return the metadata of the partitions, of which the leaders are located on the specified kafka broker.
     */
    public List<PartitionMetadata> getLeaderPartsMetadata (String host) throws TopicNotFoundException, NoPartitionsException
    {
        List<PartitionMetadata> leaderPartsMetadata = new ArrayList<PartitionMetadata>();
        int port = Integer.parseInt(this.properties.getProperty("broker.port", "9092"));
        for (PartitionMetadata part : this.getAllPartsMetadata())
        {
            if (part.leader().host().equals(host) && part.leader().port() == port)
            {
                leaderPartsMetadata.add(part);
            }
        }
        return leaderPartsMetadata;
    }

    /**
     * get the metadata of the partitions, of which the leader is the broker.host of this connection.
     * Notice that the leaders of the partitions may change during the interval between
     * calling this method and using the returned metadata. So you must use the returned metadata asap.
     * @return
     * @throws TopicNotFoundException
     * @throws NoPartitionsException
     */
    public List<PartitionMetadata> getLeaderPartsMetadata () throws TopicNotFoundException, NoPartitionsException
    {
        String broker = this.properties.getProperty("broker.host");
        return this.getLeaderPartsMetadata(broker);
    }

    public long[] getOffsets (int partition, long time) throws FetchingOffsetException
    {
        String topic = this.properties.getProperty("topic");
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), this.clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            throw new FetchingOffsetException("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
        }
        return response.offsets(topic, partition);
    }

    /**
     * fetch the message from the offset of the partition on broker.host of this connection.
     * @param partition the partition id
     * @param offset the offset
     * @return the fetch response
     */
    public FetchResponse fetch (int partition, long offset)
    {
        int soTimeout = Integer.parseInt(this.properties.getProperty("socket.timeout", "10000"));
        String topic = this.properties.getProperty("topic");
        kafka.api.FetchRequest req = new FetchRequestBuilder().clientId(this.clientName).addFetch(topic, partition, offset, soTimeout).build();
        return this.consumer.fetch(req);
    }

    /**
     * create the statement for executing queries.
     * @return
     * @throws SQLException
     */
    @Override
    public Statement createStatement() throws SQLException
    {
        return new ComsumerStatement(this);
    }

    /**
     * close the consumer
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException
    {
        if (this.closed == false)
        {
            if (this.consumer != null)
                this.consumer.close();
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return this.closed;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isReadOnly() throws SQLException
    {
        return true;
    }

    /**
     * if the consumer is null or has been closed, return false;
     * else if the consumer can request the partition metadata, even if the metadata is empty (but not null), return true;
     * else return false
     * @param timeout do no used
     * @return whether this connection is valid.
     * @throws SQLException
     */
    @Override
    public boolean isValid(int timeout) throws SQLException
    {
        if (this.consumer == null || this.closed)
        {
            return false;
        }
        List<PartitionMetadata> partsMetadata = null;
        try
        {
            partsMetadata = this.getAllPartsMetadata();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (partsMetadata != null)
            {
                partsMetadata.clear();
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}

# jdbc-for-kafka
The java consumer API of kafka is tedious. This is an easy-to-use JDBC for kafka. 

## dependencies
This jdbc relies on the SimpleConsumer in kafka.
So you have to use it with jafka_2.xx-0.x.x.x.jar, see https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz
for downloading kafka, extract kafka, the jar-balls can be found in the libs sub directory.

## usage
You can clone this repo, there is a jdbc-for-kafka-x.x.x.jar in the root directory. add this jar file and the kafka jar file into your java project.

Here is the example test code:
>       Class.forName("cn.edu.ruc.kafka.Driver");
        Properties info = new Properties();
        info.setProperty("role","consumer");
        info.setProperty("client.id", "12345");
        Connection conn = DriverManager.getConnection("kafka://log04:9092/test", info);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(Query.GET_LEADER_PARTITIONS_METADATA);
        while (rs.next())
        {
                System.out.println(((Broker)rs.getObject("leader")).host() + ", " + rs.getInt("partitionId") + ", " + rs.getInt("sizeInBytes"));
        }
        st.close();
        conn.close();
        
A full example of using jdbc-for-kafka can be found in src/cn/edu/ruc/kafka/test/KafkaJDBCTest


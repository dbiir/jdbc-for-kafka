package cn.edu.ruc.kafka;

import cn.edu.ruc.kafka.connection.ConsumerConnection;
import cn.edu.ruc.kafka.exception.BrokerConnectException;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This <code>Driver</code> implements the java.sql.Driver interface.
 * It is invoked by java.sql.DriverManager.
 * date Aug/05/2015
 *
 * @author Hank Bian
 * @version 0.0.1
 * @see java.sql.Driver
 */
public class Driver implements java.sql.Driver
{
    private static boolean registered = false;

    static
    {
        if (!registered)
        {
            try
            {
                java.sql.DriverManager.registerDriver(new Driver());
                registered = true;
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * parse the url and build the kafka connection.
     * the client.id is used as the postfix of the clientName.
     * currently the producer connection is not implemented.
     * @param url should be: kafka://host:port/topic
     * @param info should contain at lease role and client.id
     * @return
     * @throws SQLException
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException
    {
        if (registered == false)
        {
            throw new SQLException("driver is not registered.");
        }

        if (!url.startsWith("kafka://"))
        {
            return null;
        }
        if (!info.containsKey("role") || !info.containsKey("client.id"))
        {
            throw new BrokerConnectException("user and/or client.id not specified.");
        }

        if (!this.acceptsURL(url))
        {
            throw new BrokerConnectException("url is not accepted. should in the form: kafka://host:port/topic.");
        }
        String broker = url.substring(8, url.indexOf(':', 8));
        String port = url.substring(url.indexOf(':', 8)+1, url.indexOf('/', 8));
        String topic = url.substring(url.lastIndexOf('/')+1);
        info.setProperty("broker.host", broker);
        info.setProperty("broker.port", port);
        info.setProperty("topic", topic);
        if (info.getProperty("role").equalsIgnoreCase("consumer"))
        {
            return new ConsumerConnection(info);
        }
        else if (info.getProperty("role").equalsIgnoreCase("producer"))
        {
            //producer connection not implemented.
            throw new SQLFeatureNotSupportedException("producer connection not implemented.");
        }
        else
        {
            throw new BrokerConnectException("role should be consumer or producer");
        }

    }

    @Override
    public boolean acceptsURL(String url) throws SQLException
    {
        if (url.matches("kafka://[a-z|A-Z|0-9|_|.|-]+:[0-9]+/[a-z|A-Z|0-9|_|.|-]+"))
        {
            return true;
        }
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion()
    {
        return 0;
    }

    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    @Override
    public boolean jdbcCompliant()
    {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        return null;
    }
}

package cn.edu.ruc.kafka.query;

import cn.edu.ruc.kafka.exception.QueryNotSupportedException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bian Haoqiong
 * @version 0.0.1
 */
public class QueryExecutorFactory
{
    private QueryExecutorFactory ()
    {
    }

    private static QueryExecutorFactory intance = null;

    /**
     * calling this method to get the singleton instance of the QueryExecutorFactory.
     * @return
     */
    public static QueryExecutorFactory Instance ()
    {
        if (intance == null)
        {
            intance = new QueryExecutorFactory();
        }
        return intance;
    }

    private Map<String, QueryExecutor> executorMap = new HashMap<String, QueryExecutor>();

    /**
     * @param query the class name of the query executor
     * @return the instance of query executor
     * @throws QueryNotSupportedException
     */
    public QueryExecutor getExecutor (String query) throws QueryNotSupportedException
    {
        if (this.executorMap.containsKey(query))
        {
            return this.executorMap.get(query);
        }

        try
        {
            QueryExecutor executor = (QueryExecutor) Class.forName("cn.edu.ruc.kafka.query." + query).newInstance();
            this.executorMap.put(query, executor);
            return executor;
        } catch (Exception e)
        {
            throw new QueryNotSupportedException("Query " + query + " is not supported: " + e.getMessage());
        }
    }
}

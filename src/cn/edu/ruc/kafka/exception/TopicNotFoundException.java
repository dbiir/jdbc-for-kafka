package cn.edu.ruc.kafka.exception;

import java.sql.SQLException;

/**
 * Created by Bian Haoqiong on 8/5/15.
 */
public class TopicNotFoundException extends SQLException
{
    public TopicNotFoundException(String reason)
    {
        super(reason);
    }
}

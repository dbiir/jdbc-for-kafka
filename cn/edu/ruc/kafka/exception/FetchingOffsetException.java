package cn.edu.ruc.kafka.exception;

import java.sql.SQLException;

/**
 * Created by Bian Haoqiong on 8/5/15.
 */
public class FetchingOffsetException extends SQLException
{
    public FetchingOffsetException (String reason)
    {
        super(reason);
    }
}

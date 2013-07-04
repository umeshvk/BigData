package com.mvdb.etl;

public class ConsumerException extends RuntimeException
{
    public ConsumerException(String message)
    {
        super(message);  
    }
    
    public ConsumerException(String message, Throwable t)
    {
        super(message, t);
    }
}

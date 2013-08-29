package com.mvdb.etl.consumer;

public class ConsumerException extends RuntimeException
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ConsumerException(String message)
    {
        super(message);  
    }
    
    public ConsumerException(String message, Throwable t)
    {
        super(message, t);
    }
}

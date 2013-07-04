package com.mvdb.etl;




public interface GenericConsumer
{
    boolean consume(DataRecord dataRecord) ;

    boolean flushAndClose();
    
}
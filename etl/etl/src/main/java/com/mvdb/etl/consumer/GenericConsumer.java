package com.mvdb.etl.consumer;

import com.mvdb.etl.data.DataRecord;




public interface GenericConsumer
{
    boolean consume(DataRecord dataRecord) ;

    boolean flushAndClose();
    
}
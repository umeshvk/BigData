package com.mvdb.etl.consumer;

import com.mvdb.etl.data.DataRecord;
import com.mvdb.etl.data.IdRecord;




public interface GenericConsumer
{
    boolean consume(DataRecord dataRecord) ;

    boolean flushAndClose();

    boolean consume(IdRecord idRecord);
    
}
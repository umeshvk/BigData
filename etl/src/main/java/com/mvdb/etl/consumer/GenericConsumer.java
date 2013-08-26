package com.mvdb.etl.consumer;

import com.mvdb.data.DataRecord;
import com.mvdb.data.IdRecord;
import com.mvdb.data.Metadata;






public interface GenericConsumer
{
    boolean consume(DataRecord dataRecord) ;
    
    boolean consume(Metadata metadata) ;

    boolean flushAndClose();

    boolean consume(IdRecord idRecord);
    
}
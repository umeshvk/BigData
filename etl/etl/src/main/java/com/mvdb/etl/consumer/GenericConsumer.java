package com.mvdb.etl.consumer;

import com.mvdb.etl.data.DataRecord;
import com.mvdb.etl.data.IdRecord;
import com.mvdb.etl.data.Metadata;




public interface GenericConsumer
{
    boolean consume(DataRecord dataRecord) ;
    
    boolean consume(Metadata metadata) ;

    boolean flushAndClose();

    boolean consume(IdRecord idRecord);
    
}
package com.mvdb.etl.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.model.OrderLineItem;

public interface OrderLineItemDAO
{
    public Map<String, ColumnMetadata> findMetadata(); 
    
    public void insert(OrderLineItem orderLineItem);

    public void insertBatch(List<OrderLineItem> customer);

    public OrderLineItem findById(long orderLineItemId);

    public List<OrderLineItem> findAll();
    
    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalRecords();
    
    public long findMaxId();

    public long getNextSequenceValue();

    public void executeSQl(String[] sqlList);

    public void update(OrderLineItem orderLineItem);

    public List<Long> findAllIds();

    public void deleteById(long orderLineItemId);

}

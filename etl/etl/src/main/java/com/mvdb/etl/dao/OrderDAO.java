package com.mvdb.etl.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.model.Order;

public interface OrderDAO
{
    public Map<String, ColumnMetadata> findMetadata(); 
    
    public void insert(Order order);

    public void insertBatch(List<Order> customer);

    public Order findByOrderId(long orderId);

    public List<Order> findAll();
    
    //public List<Order> findAll(Timestamp modifiedAfter);

    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalOrders();
    
    //public int findTotalOrders(String customer);

    public long findMaxId();

    public long getNextSequenceValue();

    public void executeSQl(String[] sqlList);

    public void update(Order order);

    public List<Long> findAllIds();

    public void deleteById(long orderId);

}

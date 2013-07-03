package com.mvdb.etl.dao;

import java.sql.Timestamp;
import java.util.List;

import com.mvdb.etl.Consumer;
import com.mvdb.etl.model.Order;

public interface OrderDAO
{
    public void insert(Order order);

    public void insertBatch(List<Order> customer);

    public Order findByOrderId(long orderId);

    public List<Order> findAll();
    
    //public List<Order> findAll(Timestamp modifiedAfter);

    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalOrders();

    public long findMaxId();

    public long getNextSequenceValue();

    public void executeSQl(String[] sqlList);

    public void update(Order order);

}

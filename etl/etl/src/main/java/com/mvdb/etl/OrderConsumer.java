package com.mvdb.etl;

import java.util.ArrayList;
import java.util.List;

import com.mvdb.etl.model.Order;

public class OrderConsumer implements Consumer
{

   List<Order> orders = new ArrayList<Order>();

    @Override
    public void consume(Object object)
    {
        Order order = (Order)object;
        orders.add(order);        
    }

    public List<Order> getOrders()
    {
        return orders;
    }
    
    

}

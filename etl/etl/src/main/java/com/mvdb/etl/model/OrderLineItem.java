package com.mvdb.etl.model;

import java.io.Serializable;
import java.util.Date;

public class OrderLineItem implements Serializable
{
    long   orderLineItemId;
    long   orderId;
    String description;
    int    quantity;
    double price; 
    Date   createTime;
    Date   updateTime;

    public OrderLineItem()
    {
    }

    public OrderLineItem(long   orderLineItemId, long orderId, String description, int quantity, double price, Date createTime, Date updateTime)
    {
        this.orderLineItemId = orderLineItemId;
        this.orderId = orderId;
        this.description = description; 
        this.quantity = quantity;
        this.price = price; 
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public long getOrderLineItemId()
    {
        return orderLineItemId;
    }

    public void setOrderLineItemId(long orderLineItemId)
    {
        this.orderLineItemId = orderLineItemId;
    }

    public long getOrderId()
    {
        return orderId;
    }

    public void setOrderId(long orderId)
    {
        this.orderId = orderId;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public int getQuantity()
    {
        return quantity;
    }

    public void setQuantity(int quantity)
    {
        this.quantity = quantity;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    public Date getCreateTime()
    {
        return createTime;
    }

    public void setCreateTime(Date createTime)
    {
        this.createTime = createTime;
    }

    public Date getUpdateTime()
    {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime)
    {
        this.updateTime = updateTime;
    }

    @Override
    public String toString()
    {
        return "OrderLineItem [orderLineItemId=" + orderLineItemId + ", orderId=" + orderId + 
                    ", description=" + description +  ", quantity=" + quantity +   
                    ", price=" + price + ", createTime=" + createTime + ", updateTime=" + updateTime + "]";
    }

}

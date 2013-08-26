package com.mvdb.etl.model;

import java.io.Serializable;
import java.util.Date;

public class Order implements Serializable
{
    long   orderId;
    String note;
    int    saleCode;
    Date   createTime;
    Date   updateTime;

    public Order()
    {
    }

    public Order(long orderId, String note, int saleCode, Date createTime, Date updateTime)
    {
        this.orderId = orderId;
        this.note = note;
        this.saleCode = saleCode;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public long getOrderId()
    {
        return orderId;
    }

    public void setOrderId(long orderId)
    {
        this.orderId = orderId;
    }

    public String getNote()
    {
        return note;
    }

    public void setNote(String note)
    {
        this.note = note;
    }

    public int getSaleCode()
    {
        return saleCode;
    }

    public void setSaleCode(int saleCode)
    {
        this.saleCode = saleCode;
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
        return "Order [orderId=" + orderId + ", note=" + note + ", " + ", saleCode=" + saleCode + ", createTime="
                + createTime + ", updateTime=" + updateTime + "]";
    }

}

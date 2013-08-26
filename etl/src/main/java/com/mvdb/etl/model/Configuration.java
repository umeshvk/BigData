package com.mvdb.etl.model;

import java.io.Serializable;

public class Configuration implements Serializable
{
    
    String customer;
    String name;
    String value;

    public Configuration()
    {
    }
    
    public Configuration(String customer, String name, String value)
    {
        this.customer = customer;
        this.name = name;
        this.value = value;
    }
    
    public String getCustomer()
    {
        return customer;
    }

    public void setCustomer(String customer)
    {
        this.customer = customer;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    @Override
    public String toString()
    {
        return "Configuration [customer=" + customer + ", name=" + name + ", " + ", value=" + value  +  "]";
    }

}

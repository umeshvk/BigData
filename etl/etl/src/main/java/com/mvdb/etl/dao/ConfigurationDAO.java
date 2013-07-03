package com.mvdb.etl.dao;

import java.util.List;

import com.mvdb.etl.model.Configuration;

public interface ConfigurationDAO
{
    public void insert(Configuration configuration);


    public Configuration find(String customer, String name);

    public List<Configuration> findAll();
    
    public int getCount(String customer);
    
    public int getCustomerCount();

    public void update(Configuration configuration);

}

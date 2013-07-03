package com.mvdb.etl.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.etl.Consumer;
import com.mvdb.etl.SequenceNames;
import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.model.Configuration;
import com.mvdb.etl.model.ConfigurationRowMapper;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.model.OrderRowMapper;

public class JdbcConfigurationDAO extends JdbcDaoSupport implements ConfigurationDAO
{

    @Override
    public void insert(Configuration configuration)
    {

        String sql = "INSERT INTO configuration "
                + "(customer, name, value) VALUES (?, ?, ?)";

        getJdbcTemplate().update(
                sql,
                new Object[] { configuration.getCustomer(), configuration.getName(), configuration.getValue() });

    }

   
    @Override
    public Configuration find(String customer, String name)
    {
        String sql = "SELECT * FROM configuration WHERE customer = ? AND name = ?";

        Configuration configuration = (Configuration) getJdbcTemplate().queryForObject(sql, new Object[] { customer, name }, new ConfigurationRowMapper());

        return configuration;
    }



    @Override
    public int getCount(String customer)
    {
        String sql = "SELECT COUNT(*) FROM configuration where customer = ?";
        int total = getJdbcTemplate().queryForInt(sql, new Object[] { customer});
        return total;
    }



    @Override
    public int getCustomerCount()
    {
        String sql = "select count(*) from (select count(*) from configuration group by customer) x";
        int total = getJdbcTemplate().queryForInt(sql);
        return total;
    }



    @Override
    public void update(Configuration configuration)
    {
        getJdbcTemplate().update(
                "update configuration set value = ? where customer = ? AND name = ?", new Object[] { 
                        configuration.getValue(), configuration.getCustomer(), configuration.getName()});
        
    }


    @Override
    public List<Configuration> findAll()
    {
        // TODO Auto-generated method stub
        return null;
    }
    

    

}

package com.mvdb.etl.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class ConfigurationRowMapper implements RowMapper
{
    public Object mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        Configuration configuration = new Configuration();
        configuration.setCustomer(rs.getString("customer"));
        configuration.setName(rs.getString("name"));
        configuration.setValue(rs.getString("value"));
        return configuration;
    }

}

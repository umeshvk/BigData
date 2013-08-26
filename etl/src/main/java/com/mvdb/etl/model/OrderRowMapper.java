package com.mvdb.etl.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class OrderRowMapper implements RowMapper
{
    public Object mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        Order order = createOrder(rs);
        return order;
    }
    
    
    public static Order createOrder(ResultSet rs) throws SQLException
    {
        Order order = new Order();
        order.setOrderId(rs.getLong("ORDER_ID"));
        order.setNote(rs.getString("NOTE"));
        order.setSaleCode(rs.getInt("SALE_CODE"));
        order.setCreateTime(new java.util.Date(rs.getDate("CREATE_TIME").getTime()));
        order.setUpdateTime(new java.util.Date(rs.getDate("UPDATE_TIME").getTime()));
        
        return order;
    }

}

package com.mvdb.etl.dao.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.model.OrderRowMapper;
import com.mvdb.etl.util.db.SequenceNames;

public class JdbcOrderDAO extends JdbcDaoSupport implements OrderDAO
{

    @Override
    public void insert(Order order)
    {

        String sql = "INSERT INTO ORDERS "
                + "(ORDER_ID, NOTE, SALE_CODE, CREATE_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?)";

        getJdbcTemplate().update(
                sql,
                new Object[] { order.getOrderId(), order.getNote(), order.getSaleCode(),
                        new java.sql.Timestamp(order.getCreateTime().getTime()),
                        new java.sql.Timestamp(order.getUpdateTime().getTime()) });

    }

    @Override
    public void insertBatch(final List<Order> orders)
    {

        String sql = "INSERT INTO ORDERS "
                + "(ORDER_ID, NOTE, SALE_CODE, CREATE_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?)";

        getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException
            {
                Order order = orders.get(i);
                ps.setLong(1, order.getOrderId());
                ps.setString(2, order.getNote());
                ps.setInt(3, order.getSaleCode());
                ps.setTimestamp(4, new java.sql.Timestamp(order.getCreateTime().getTime()));
                ps.setTimestamp(5, new java.sql.Timestamp(order.getUpdateTime().getTime()));
            }

            @Override
            public int getBatchSize()
            {
                return orders.size();
            }
        });
    }

    @Override
    public Order findByOrderId(final long orderId)
    {


        Order order = (Order)getJdbcTemplate().query(
                "SELECT * FROM ORDERS WHERE ORDER_ID = ?",
                 new PreparedStatementSetter() {
                   public void setValues(PreparedStatement preparedStatement) throws
                     SQLException {
                       preparedStatement.setLong(1, orderId);
                   }
                 }, 
                 new ResultSetExtractor() {
                   public Order extractData(ResultSet resultSet) throws SQLException,
                     DataAccessException {
                       if (resultSet.next()) {
                           return com.mvdb.etl.model.OrderRowMapper.createOrder(resultSet);
                       }
                       return null;
                   }
                 }
             );

        return order;
    }

    @Override
    public List<Order> findAll()
    {
        String sql = "SELECT * FROM ORDERS";

        List<Order> orders = findAll(sql);

        return orders;
    }
    
    @Override
    public List<Long> findAllIds()
    {
        String sql = "SELECT order_id FROM ORDERS";
        List<Long> orderIds = new ArrayList<Long>();
        List<Map> rows = getJdbcTemplate().queryForList(sql);
        for (Map row : rows)
        {
            orderIds.add((Long) (row.get("order_id")));
        }
        return orderIds;
    }

    /*
     * private void findAll(String sql, Consumer consumer) {
     * 
     * List<Map> rows = getJdbcTemplate().queryForList(sql); for (Map row :
     * rows) { Order order = new Order(); order.setOrderId((Long)
     * (row.get("order_id"))); order.setNote((String) row.get("note"));
     * order.setSaleCode((Integer) row.get("sale_code"));
     * order.setCreateTime(new java.util.Date(((java.sql.Timestamp)
     * row.get("create_time")).getTime())); order.setUpdateTime(new
     * java.util.Date(((java.sql.Timestamp) row.get("update_time")).getTime()));
     * consumer.consume(order); }
     * 
     * }
     */

    private List<Order> findAll(String sql)
    {
        List<Order> orders = new ArrayList<Order>();

        List<Map> rows = getJdbcTemplate().queryForList(sql);
        for (Map row : rows)
        {
            Order order = new Order();
            order.setOrderId((Long) (row.get("order_id")));
            order.setNote((String) row.get("note"));
            order.setSaleCode((Integer) row.get("sale_code"));
            order.setCreateTime(new java.util.Date(((java.sql.Timestamp) row.get("create_time")).getTime()));
            order.setUpdateTime(new java.util.Date(((java.sql.Timestamp) row.get("update_time")).getTime()));
            orders.add(order);
        }

        return orders;
    }

    // @Override
    // public List<Order> findAll(Timestamp modifiedAfter)
    // {
    // // TODO Auto-generated method stub
    // return null;
    // }


    
    @Override
    public Map<String, ColumnMetadata> findMetadata()
    {
        String sql = "SELECT * FROM ORDERS limit 1";
        final Map<String, ColumnMetadata> metaDataMap = new HashMap<String , ColumnMetadata>();
        
        
        getJdbcTemplate().query(sql, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for(int column=1;column<(columnCount+1);column++)
                {
                    ColumnMetadata metadata = new ColumnMetadata();
                    metadata.setColumnLabel(rsm.getColumnLabel(column));
                    metadata.setColumnName(rsm.getColumnName(column));
                    metadata.setColumnType(rsm.getColumnType(column));
                    metadata.setColumnTypeName(rsm.getColumnTypeName(column));
                    
                    metaDataMap.put(rsm.getColumnName(column), metadata);
                }
                
            }
        });
        
        return metaDataMap; 
    }
    
    @Override
    public void findAll(Timestamp modifiedAfter, final Consumer consumer)
    {
        String sql = "SELECT * FROM ORDERS where orders.update_time >= ?";

        
        getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                
                Order order = new Order();
                order.setOrderId(row.getLong("order_id"));
                order.setNote(row.getString("note"));
                order.setSaleCode(row.getInt("sale_code"));

                Date createTime = new java.util.Date(row.getTimestamp("create_time").getTime());
                order.setCreateTime(createTime);
                Date updateTime = new java.util.Date(row.getTimestamp("update_time").getTime());
                order.setUpdateTime(updateTime);

                consumer.consume(order);
            }
        });
    }

    @Override
    public int findTotalOrders()
    {

        String sql = "SELECT COUNT(*) FROM Orders";

        int total = getJdbcTemplate().queryForInt(sql);

        return total;
    }
        

    @Override
    public long findMaxId()
    {
        String sql = "SELECT MAX(Order_Id) FROM Orders";

        long max = getJdbcTemplate().queryForLong(sql);

        return max;
    }

    @Override
    public void executeSQl(String[] sqlList)
    {

        for (String sql : sqlList)
        {
            getJdbcTemplate().update(sql);
        }
    }

    @Override
    public long getNextSequenceValue()
    {
        String sql = "SELECT nextval('" + SequenceNames.ORDER_SEQUENCE_NAME + "');";

        long value = getJdbcTemplate().queryForLong(sql);

        return value;
    }

    /**
     * long orderId; String note; int saleCode; Date createTime; Date
     * updateTime;
     * 
     * @param order
     */
    public void update(Order order)
    {
        long tm = new Date().getTime();
        getJdbcTemplate().update("update orders set note = ?, sale_code = ?, update_time = ? where order_id = ?",
                new Object[] { order.getNote(), order.getSaleCode(), new java.sql.Timestamp(order.getUpdateTime().getTime()) , order.getOrderId() });
    }

    @Override
    public void deleteById(long orderId)
    {
        getJdbcTemplate().update("delete from orders where order_id = ?", new Object[] {orderId});        
    }





}

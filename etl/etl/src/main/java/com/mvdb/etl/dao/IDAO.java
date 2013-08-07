package com.mvdb.etl.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.util.db.SequenceNames;

public interface IDAO<T>
{
    public Map<String, ColumnMetadata> findMetadata(String tableName); 
    
    public void insert(String sql, Object[] parameters);

    public void insertBatch(String sql, BatchPreparedStatementSetter batchPreparedStatementSetter, final List<T> recordList);

    public T findById(final long id, String tableName);
    public T createRecord(ResultSet rs) throws SQLException;
    
    public List<T> findAll(String tableName);
    
    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalRecords(String tableName);
    
    public long findMaxId(String tableName);

    public long getNextSequenceValue(SequenceNames sequenceName);

    public void executeSQl(String[] sqlList);

    public void update(String updateSql, Object[] parameters);

    public List<Long> findAllIds(String tableName, String primaryKeyName);

    public void deleteById(String tableName, String primaryKeyName, long id);

}

package com.mvdb.etl.dao.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.etl.BinaryGenericConsumer;
import com.mvdb.etl.ColumnMetadata;
import com.mvdb.etl.ConsumerException;
import com.mvdb.etl.DataHeader;
import com.mvdb.etl.DataRecord;
import com.mvdb.etl.GenericConsumer;
import com.mvdb.etl.GenericDataRecord;
import com.mvdb.etl.Metadata;
import com.mvdb.etl.dao.GenericDAO;

public class JdbcGenericDAO extends JdbcDaoSupport implements GenericDAO
{

    private boolean writeMetadata(Metadata metadata, File snapshotDirectory)
    {
        try
        {
            String structFileName = "schema-" + metadata.getTableName() + ".dat";
            snapshotDirectory.mkdirs();
            File structFile = new File(snapshotDirectory, structFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            metadata.writeExternal(oos);
            FileUtils.writeByteArrayToFile(structFile, baos.toByteArray());
            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }
    
    private boolean writeDataHeader(DataHeader dataHeader, String objectName, File snapshotDirectory)
    {
        try
        {
            snapshotDirectory.mkdirs();
            String headerFileName = "header-" + objectName + ".dat";
            File headerFile = new File(snapshotDirectory, headerFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            dataHeader.writeExternal(oos);
            FileUtils.writeByteArrayToFile(headerFile, baos.toByteArray());
            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }
    

    @Override
    public void fetchMetadata(String objectName, File snapshotDirectory)
    {
        final Metadata metadata = new Metadata();
        metadata.setTableName(objectName);
        String sql = "SELECT * FROM " + objectName + " limit 1";
        final Map<String, ColumnMetadata> metaDataMap = new HashMap<String, ColumnMetadata>();
        metadata.setColumnMetadataMap(metaDataMap);
        metadata.setTableName(objectName);

        getJdbcTemplate().query(sql, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount+1); column++)
                {
                    ColumnMetadata columnMetadata = new ColumnMetadata();
                    columnMetadata.setColumnLabel(rsm.getColumnLabel(column));
                    columnMetadata.setColumnName(rsm.getColumnName(column));
                    columnMetadata.setColumnType(rsm.getColumnType(column));
                    columnMetadata.setColumnTypeName(rsm.getColumnTypeName(column));

                    metaDataMap.put(rsm.getColumnName(column), columnMetadata);
//                    if (column == 1)
//                    {
//                        metadata.setTableName(rsm.getTableName(column));
//                        metadata.setSchemaName(rsm.getSchemaName(column));
//                    }

                }

            }
        });

        writeMetadata(metadata, snapshotDirectory);
    }

    @Override
    public DataHeader fetchAll(File snapshotDirectory, Timestamp modifiedAfter, String objectName)
    {
        final GenericConsumer genericConsumer = new BinaryGenericConsumer(new File(snapshotDirectory, "data-" + objectName + ".dat"));
        final DataHeader dataHeader = new DataHeader();

        String sql = "SELECT * FROM " + objectName + " o where o.update_time >= ?";

        getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                final Map<String, Object> dataMap = new HashMap<String, Object>();
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount+1); column++)
                {
                    dataMap.put(rsm.getColumnName(column), row.getObject(rsm.getColumnLabel(column)));
                }

                DataRecord dataRecord = new GenericDataRecord(dataMap);

                genericConsumer.consume(dataRecord);
                dataHeader.incrementCount();

            }
        });

        genericConsumer.flushAndClose();

        
        writeDataHeader(dataHeader, objectName, snapshotDirectory);
        return dataHeader;

    }

    @Override
    public boolean scan(File file, int count)
    {

        FileInputStream fis = null;
        ObjectInputStream ois;

        try
        {
            fis = new FileInputStream(file);
            for (int i = 0; i < count; i++)
            {
                DataRecord dataRecord = new GenericDataRecord();
                ois = new ObjectInputStream(fis);
                dataRecord.readExternal(ois);
            }
        } catch (FileNotFoundException e)
        {
            throw new ConsumerException("", e);
        } catch (ClassNotFoundException e)
        {
            throw new ConsumerException("", e);
        } catch (IOException e)
        {
            throw new ConsumerException("", e);
        }

        return true;

    }

    @Override
    public Metadata getMetadata(String objectName)
    {
        // TODO Auto-generated method stub
        return null;
    }



}

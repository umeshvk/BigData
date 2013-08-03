package com.mvdb.etl.dao.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.etl.actions.ActionUtils;
import com.mvdb.etl.consumer.GenericConsumer;
import com.mvdb.etl.consumer.SequenceFileConsumer;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.data.DataHeader;
import com.mvdb.etl.data.DataRecord;
import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.etl.data.GenericIdRecord;
import com.mvdb.etl.data.IdRecord;
import com.mvdb.etl.data.Metadata;

public class JdbcGenericDAO extends JdbcDaoSupport implements GenericDAO
{
    private static Logger logger = LoggerFactory.getLogger(JdbcGenericDAO.class);
    private GlobalMvdbKeyMaker globalMvdbKeyMaker = new GlobalMvdbKeyMaker();
    private boolean writeDataHeader(DataHeader dataHeader, String objectName, File snapshotDirectory)
    {
        try
        {
            snapshotDirectory.mkdirs();
            String headerFileName = "header-" + objectName + ".dat";
            File headerFile = new File(snapshotDirectory, headerFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(dataHeader);
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
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    ColumnMetadata columnMetadata = new ColumnMetadata();
                    columnMetadata.setColumnLabel(rsm.getColumnLabel(column));
                    columnMetadata.setColumnName(rsm.getColumnName(column));
                    columnMetadata.setColumnType(rsm.getColumnType(column));
                    columnMetadata.setColumnTypeName(rsm.getColumnTypeName(column));

                    metaDataMap.put(rsm.getColumnName(column), columnMetadata);
                }

            }
        });

        writeMetadata(metadata, snapshotDirectory);
    }

    private boolean writeMetadata(Metadata metadata, File snapshotDirectory)
    {
        try
        {
            String structFileName = "schema-" + metadata.getTableName() + ".dat";
            snapshotDirectory.mkdirs();
            File structFile = new File(snapshotDirectory, structFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(metadata);
            oos.flush();
            FileUtils.writeByteArrayToFile(structFile, baos.toByteArray());
            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }


    @Override
    public DataHeader fetchAll2(File snapshotDirectory, Timestamp modifiedAfter, String objectName, String keyName, String updateTimeColumnName)
    {
        DataHeader dataHeader = new DataHeader();
        writeUpdates(snapshotDirectory, modifiedAfter, objectName, keyName, updateTimeColumnName, dataHeader);                
        writeDataHeader(dataHeader, objectName, snapshotDirectory);
        writeIds(snapshotDirectory, objectName, keyName, updateTimeColumnName/*, dataHeader*/);
        return dataHeader;
    }


    
    private void writeIds(File snapshotDirectory, String objectName, final String keyName,
            final String updateTimeColumnName/*, DataHeader dataHeader*/)
    {
        final String snapShotDirName = snapshotDirectory.getName();
        //final Date refreshTimeStamp = ActionUtils.getDate(snapShotDirName);
        File objectFile = new File(snapshotDirectory, "ids-" + objectName + ".dat");
        final GenericConsumer genericConsumer = new SequenceFileConsumer(objectFile);
        

        String sql = "SELECT " + keyName + " FROM " + objectName;

        getJdbcTemplate().query(sql, new Object[] {  }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                final Map<String, Object> dataMap = new HashMap<String, Object>();
                ResultSetMetaData rsm = row.getMetaData();               
                Object originalKeyValue = row.getObject(1);
                IdRecord idRecord = new GenericIdRecord(originalKeyValue , globalMvdbKeyMaker, snapShotDirName);
                genericConsumer.consume(idRecord);
                //dataHeader.incrementCount();

            }
        });

        genericConsumer.flushAndClose();
        
    }

    private void writeUpdates(File snapshotDirectory, Timestamp modifiedAfter, String objectName, final String keyName, final String updateTimeColumnName, final DataHeader dataHeader)
    {
        File objectFile = new File(snapshotDirectory, "data-" + objectName + ".dat");
        final GenericConsumer genericConsumer = new SequenceFileConsumer(objectFile);
        
        final String snapShotDirName = snapshotDirectory.getName();
        //final Date refreshTimeStamp = ActionUtils.getDate(snapShotDirName);

        String sql = "SELECT * FROM " + objectName + " o where o.update_time >= ?";

        getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                final Map<String, Object> dataMap = new HashMap<String, Object>();
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    dataMap.put(rsm.getColumnName(column), row.getObject(rsm.getColumnLabel(column)));
                }

                GenericDataRecord dataRecord = new GenericDataRecord(dataMap, keyName , globalMvdbKeyMaker, 
                                updateTimeColumnName, new GlobalMvdbUpdateTimeMaker());
                dataRecord.setRefreshTimeStamp(snapShotDirName);
                genericConsumer.consume(dataRecord);
                dataHeader.incrementCount();

            }
        });

        genericConsumer.flushAndClose();
    }

    @Override
    public boolean scan2(String objectName, File snapshotDirectory)
    {
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopLocalFS);
        String dataFileName = "data-" + objectName + ".dat";
        File dataFile = new File(snapshotDirectory, dataFileName);
        Path path = new Path(dataFile.getAbsolutePath());

        FileSystem fs;
        try
        {
            fs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

            Text key = new Text(); 
            BytesWritable value = new BytesWritable(); 
            while (reader.next(key, value))
            {
                byte[] bytes = value.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                GenericDataRecord dr = (GenericDataRecord) ois.readObject();
                System.out.println(dr.toString());
            }

            IOUtils.closeStream(reader);
        } catch (IOException e)
        {
            logger.error("scan2():", e);
            return false;
        } catch (ClassNotFoundException e)
        {
            logger.error("scan2():", e);
            return false;
        }

        return true;
    }



    private Metadata readMetadata(String objectName, File snapshotDirectory) throws IOException, ClassNotFoundException
    {
        Metadata metadata = new Metadata();
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try
        {
            String structFileName = "schema-" + objectName + ".dat";
            File structFile = new File(snapshotDirectory, structFileName);
            fis = new FileInputStream(structFile);
            ois = new ObjectInputStream(fis);
            metadata = (Metadata) ois.readObject();
            return metadata;
        } finally
        {
            if (fis != null)
            {
                fis.close();
            }
            if (ois != null)
            {
                ois.close();
            }
        }

    }

    @Override
    public Metadata getMetadata(String objectName, File snapshotDirectory)
    {

        try
        {
            return readMetadata(objectName, snapshotDirectory);
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }
    

}

package com.mvdb.etl.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata implements AnyRecord, Externalizable, Comparable<Metadata>
{
    private static final long  serialVersionUID  = 1L;
    public static final String COLUMNDATALISTKEY = "columnDataListKey";

    String                     refreshTimeStamp;
    int                        count;
    String                     schemaName;
    String                     tableName;
    // ColumnMetadata
    Map<String, Object>        columnMetadataMap = new HashMap<String, Object>();

    public static void main1(String[] args) throws IOException, ClassNotFoundException
    {
//        ActionUtils.setUpInitFileProperty();
//        ApplicationContext context = Top.getContext();
//        GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
//        
//        org.apache.hadoop.conf.Configuration conf1 = new org.apache.hadoop.conf.Configuration();
//        conf1.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
//        Metadata md = genericDAO.readMetadata("hdfs://localhost:9000/data/alpha/20030115050607/schema-orderlineitem.dat", conf1);
        Metadata m1 = new Metadata();
        m1.setCount(1);
        m1.setRefreshTimeStamp("20120101000000");
        m1.setSchemaName("schema");
        m1.setTableName("table");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //FileOutputStream fo = new FileOutputStream("tmp");
        ObjectOutputStream so = new ObjectOutputStream(baos);
        so.writeObject(m1);
        so.flush();
        
        byte[] theData = baos.toByteArray();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(theData);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Metadata m2 = (Metadata)ois.readObject();
        
        System.out.println(m1);
        System.out.println(m2);
        int ii =0; 
        int jj = 0; 
        

        
        
    }
    
    public Metadata()
    {
        count = 0;
    }

    public String getRefreshTimeStamp()
    {
        return refreshTimeStamp;
    }

    public void setRefreshTimeStamp(String refreshTimeStamp)
    {
        this.refreshTimeStamp = refreshTimeStamp;
    }

    public void incrementCount()
    {
        count++;
    }

    public int getCount()
    {
        return count;
    }

    public void setCount(int count)
    {
        this.count = count;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public Map<String, Object> getColumnMetadataMap()
    {
        return columnMetadataMap;
    }

    public void setColumnMetadataMap(Map<String, Object> columnMetadataMap)
    {
        this.columnMetadataMap = columnMetadataMap;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        count = input.readInt();
        schemaName = (String) input.readObject();
        tableName = (String) input.readObject();
        refreshTimeStamp = (String) input.readObject();

        columnMetadataMap = new HashMap<String, Object>();
        int keyCount = input.readInt();
        List<Object> columnDataObjectList = new ArrayList<Object>();
        columnMetadataMap.put(COLUMNDATALISTKEY, columnDataObjectList);
        if(keyCount > -1)
        {
            for (int i = 0; i < keyCount; i++)
            {
                // String key = (String)input.readObject();
                // ColumnMetadata columnMetadata = new ColumnMetadata();
                // columnMetadata = (ColumnMetadata) input.readObject();
                // columnMetadataMap.put(key, columnMetadata);
                ColumnMetadata columnMetadata = (ColumnMetadata) input.readObject();
                columnDataObjectList.add(columnMetadata);
            }
        }

    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeInt(count);
        output.writeObject(schemaName);
        output.writeObject(tableName);
        output.writeObject(refreshTimeStamp);

        List<Object> columnDataObjectList = (List<Object>) columnMetadataMap.get(COLUMNDATALISTKEY);
        if(columnDataObjectList == null || columnDataObjectList.size() == 0) 
        {
            output.writeInt(-1);
        } 
        else 
        { 
            output.writeInt(columnDataObjectList.size());
        }
        if(columnDataObjectList != null && columnDataObjectList.size() > 0)
        {
            for (Object obj : columnDataObjectList)
            {
                ColumnMetadata columnMetadata = (ColumnMetadata) obj;
                output.writeObject(columnMetadata);
            }
        }
        // output.writeInt(columnMetadataMap.size());
        //
        // Iterator<String> keysIter = columnMetadataMap.keySet().iterator();
        // while(keysIter.hasNext())
        // {
        // String key = keysIter.next();
        // Object value = columnMetadataMap.get(key);
        // output.writeObject(key);
        // output.writeObject(value);
        // }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException
    {
        /*
        ActionUtils.setUpInitFileProperty();
        ApplicationContext context = Top.getContext();
        GenericDAO genericDAO = (GenericDAO) context.getBean("genericDAO");

        org.apache.hadoop.conf.Configuration conf1 = new org.apache.hadoop.conf.Configuration();
        conf1.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        // Metadata md = genericDAO.readMetadata(new
        // File("/home/umesh/.mvdb/etl/data/alpha/20030115050607/schema-orderlineitem.dat").toURI().toString(),
        // conf1);
        Metadata md = genericDAO.readMetadata(
                "hdfs://localhost:9000/data/alpha/20030115050607/schema-orderlineitem.dat", conf1);
        */
        
        Metadata m1 = new Metadata();
        m1.setCount(1);
        m1.setRefreshTimeStamp("20120101000000");
        m1.setSchemaName("schema");
        m1.setTableName("table");
        Map<String, Object> theColumnDataMap = m1.getColumnMetadataMap();
        List<Object> columnDataObjectList = new ArrayList<Object>();
        ColumnMetadata cmd = new ColumnMetadata();
        cmd.setColumnLabel("columnLabel");
        cmd.setColumnName("columnName");
        cmd.setColumnType(11);
        cmd.setColumnTypeName("columnTypeName");
        columnDataObjectList.add(cmd);
        theColumnDataMap.put(COLUMNDATALISTKEY, columnDataObjectList);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream so = new ObjectOutputStream(baos);
        so.writeObject(m1);
        so.flush();
        System.out.println(m1);
        
        byte[] theData = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(theData);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Metadata m2 = (Metadata) ois.readObject();
        System.out.println(m2);
        
        
        baos = new ByteArrayOutputStream();
        so = new ObjectOutputStream(baos);
        so.writeObject(m2);
        so.flush();
        
        theData = baos.toByteArray();

        bais = new ByteArrayInputStream(theData);
        ois = new ObjectInputStream(bais);
        Metadata m3 = (Metadata) ois.readObject();
        System.out.println(m3);
        int ii = 0;
        int jj = 0;

    }

    @Override
    public String toString()
    {
        return "Metadata [refreshTimeStamp=" + refreshTimeStamp + ", count=" + count + ", schemaName=" + schemaName
                + ", tableName=" + tableName + ", columnMetadataMap=" + columnMetadataMap + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnMetadataMap == null) ? 0 : columnMetadataMap.hashCode());
        result = prime * result + count;
        result = prime * result + ((refreshTimeStamp == null) ? 0 : refreshTimeStamp.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Metadata other = (Metadata) obj;
        if (columnMetadataMap == null)
        {
            if (other.columnMetadataMap != null)
                return false;
        } else if (!columnMetadataMap.equals(other.columnMetadataMap))
            return false;
        if (count != other.count)
            return false;
        if (refreshTimeStamp == null)
        {
            if (other.refreshTimeStamp != null)
                return false;
        } else if (!refreshTimeStamp.equals(other.refreshTimeStamp))
            return false;
        if (schemaName == null)
        {
            if (other.schemaName != null)
                return false;
        } else if (!schemaName.equals(other.schemaName))
            return false;
        if (tableName == null)
        {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }

    @Override
    public int compareTo(Metadata other)
    {
        String local = getRefreshTimeStamp();
        String external = other.getRefreshTimeStamp();
        return local.compareTo(external);
    }

    @Override
    public Map<String, Object> getDataMap()
    {
        return columnMetadataMap;
    }

    @Override
    public void removeIdenticalColumn(String columnName, Object latestValue)
    {
        Object lastValue = columnMetadataMap.get(columnName);
        if (lastValue == null)
        {
            return;
        }
        if (lastValue.equals(latestValue))
        {
            columnMetadataMap.remove(columnName);
        }

    }

    /*
     * @Override public void readFields(DataInput dataInput) throws IOException
     * { count = dataInput.readInt(); schemaName = dataInput.readUTF();
     * tableName = dataInput.readUTF();
     * 
     * 
     * columnMetadataMap = new HashMap<String, ColumnMetadata>(); int keyCount =
     * dataInput.readInt();
     * 
     * for(int i=0;i<keyCount;i++) { String key = (String)dataInput.readUTF();
     * ColumnMetadata columnMetadata = new ColumnMetadata();
     * columnMetadata.readFields(dataInput); columnMetadataMap.put(key,
     * columnMetadata); }
     * 
     * }
     * 
     * @Override public void write(DataOutput dataOutput) throws IOException {
     * dataOutput.writeInt(count); dataOutput.writeUTF(schemaName);
     * dataOutput.writeUTF(tableName);
     * 
     * dataOutput.writeInt(columnMetadataMap.size());
     * 
     * Iterator<String> keysIter = columnMetadataMap.keySet().iterator();
     * while(keysIter.hasNext()) { String key = keysIter.next(); ColumnMetadata
     * value = columnMetadataMap.get(key); dataOutput.writeUTF(key);
     * value.write(dataOutput); } }
     */

}
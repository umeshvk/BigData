package com.mvdb.etl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Metadata implements Externalizable 
{
    int count;
    String schemaName; 
    String tableName;
    Map<String,ColumnMetadata> columnMetadataMap;


    
    public Metadata()
    {
        count = 0; 
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

    public Map<String,ColumnMetadata> getColumnMetadataMap()
    {
        return columnMetadataMap;
    }

    public void setColumnMetadataMap(Map<String,ColumnMetadata> columnMetadataMap)
    {
        this.columnMetadataMap = columnMetadataMap;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        count = input.readInt();
        schemaName = (String)input.readObject();
        tableName = (String)input.readObject();
        columnMetadataMap = new HashMap<String, ColumnMetadata>();
        int keyCount = input.readInt();
        for(int i=0;i<keyCount;i++)
        {
            String key = (String)input.readObject();
            ColumnMetadata columnMetadata = new ColumnMetadata();
            columnMetadata.readExternal(input); 
            columnMetadataMap.put(key, columnMetadata);
        }
        
    }
    
    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeInt(count);
        output.writeObject(schemaName);
        output.writeObject(tableName);
        output.writeInt(columnMetadataMap.size());
        Iterator<String> keysIter = columnMetadataMap.keySet().iterator();
        while(keysIter.hasNext())
        {
            String key = keysIter.next();
            ColumnMetadata value = columnMetadataMap.get(key);
            output.writeObject(key);
            value.writeExternal(output);
        }
    }




}
package com.mvdb.etl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GenericDataRecord implements DataRecord
{
    Map<String, Object> dataMap;

    public GenericDataRecord()
    {
        dataMap = new HashMap<String, Object>();
    }

    public GenericDataRecord(Map<String, Object> dataMap)
    {
        if (dataMap == null)
        {
            dataMap = new HashMap<String, Object>();
        }
        this.dataMap = dataMap;

    }

    public Map<String, Object> getDataMap()
    {
        return dataMap;
    }

    public void setDataMap(Map<String, Object> dataMap)
    {
        this.dataMap = dataMap;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        int size = input.readInt();
        dataMap = new HashMap<String, Object>();
        for (int i = 0; i < size; i++)
        {
            String key = input.readUTF();
            Object value = input.readObject();
            dataMap.put(key, value);
        }
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeInt(dataMap.size());
        Iterator<String> keysIter = dataMap.keySet().iterator();
        while (keysIter.hasNext())
        {
            String key = keysIter.next();
            Object value = dataMap.get(key);
            output.writeUTF(key);
            output.writeObject(value);
        }

    }

}

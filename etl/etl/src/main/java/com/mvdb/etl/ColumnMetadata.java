package com.mvdb.etl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ColumnMetadata implements Externalizable
{

    String  columnName;
    String  columnTypeName;
    String  columnLabel;
    int     columnType;

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        columnName = (String)input.readObject();
        columnTypeName = (String)input.readObject();
        columnLabel = (String)input.readObject();
        columnType = input.readInt();
        
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeObject(columnName);
        output.writeObject(columnTypeName);
        output.writeObject(columnLabel);
        output.writeInt(columnType);
    }
    
    public ColumnMetadata()
    {

    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public String getColumnTypeName()
    {
        return columnTypeName;
    }

    public void setColumnTypeName(String columnTypeName)
    {
        this.columnTypeName = columnTypeName;
    }

    public String getColumnLabel()
    {
        return columnLabel;
    }

    public void setColumnLabel(String columnLabel)
    {
        this.columnLabel = columnLabel;
    }

    public int getColumnType()
    {
        return columnType;
    }

    public void setColumnType(int columnType)
    {
        this.columnType = columnType;
    }



}

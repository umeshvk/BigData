package com.mvdb.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DataHeader implements Externalizable 
{
    private static final long serialVersionUID = 1L;

    int count;
    
    public DataHeader()
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

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        count = input.readInt();
        
        
    }
    
    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeInt(count);        
    }




}
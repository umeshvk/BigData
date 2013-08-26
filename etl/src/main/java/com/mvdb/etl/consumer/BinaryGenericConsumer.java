package com.mvdb.etl.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.mvdb.data.DataRecord;
import com.mvdb.data.IdRecord;
import com.mvdb.data.Metadata;



public class BinaryGenericConsumer implements GenericConsumer
{

    File                file;
    FileOutputStream    fos;
    ObjectOutputStream  oos;
    boolean             good;
    boolean             done;

    public BinaryGenericConsumer(File file)
    {
        this.done = false;
        this.file = file;
        try
        {
            file.getParentFile().mkdirs();
            this.fos = new FileOutputStream(file);
            this.oos = new ObjectOutputStream(fos);
            good = true;
        } catch (FileNotFoundException e)
        {
            good = false;
            e.printStackTrace();
            
        } catch (IOException e)
        {
            good = false;
            e.printStackTrace();
        }
    }

    @Override
    public boolean consume(DataRecord dataRecord)
    {
        if (done == true)
        {
            throw new ConsumerException("Consumer closed for output file:" + file.getAbsolutePath());
        }
        if (good == false)
        {
            throw new ConsumerException("Check log for prior error. Consumer unusable for output file:"
                    + file.getAbsolutePath());
        }

        try
        {            
            oos.writeObject(dataRecord);
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            throw new ConsumerException("Consumer failed to consume for output file:" + file.getAbsolutePath()
                    + ", and DataRecord:" + dataRecord.toString());
        }

    }
    
    @Override
    public boolean consume(IdRecord idRecord)
    {
        if (done == true)
        {
            throw new ConsumerException("Consumer closed for output file:" + file.getAbsolutePath());
        }
        if (good == false)
        {
            throw new ConsumerException("Check log for prior error. Consumer unusable for output file:"
                    + file.getAbsolutePath());
        }

        try
        {            
            oos.writeObject(idRecord);
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            throw new ConsumerException("Consumer failed to consume for output file:" + file.getAbsolutePath()
                    + ", and DataRecord:" + idRecord.toString());
        }
    }

    @Override
    public boolean flushAndClose()
    {
        try
        {
            if (oos != null)
            {
                oos.flush();
                oos.close();               
            }
            
            if (fos != null)
            {
                fos.flush();
                fos.close();               
            }
            
            return true;
        } catch (IOException ioe)
        {
            throw new ConsumerException("Unable to flush for output file:" + file.getAbsolutePath(), ioe);
        }
    }

    @Override
    public boolean consume(Metadata metadata)
    {
        if(true)
        {
            throw new RuntimeException("Not Implemeted Yet");
        }
        return false;
    }



}

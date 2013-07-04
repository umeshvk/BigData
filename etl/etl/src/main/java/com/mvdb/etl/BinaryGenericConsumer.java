package com.mvdb.etl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.springframework.dao.DataAccessException;

public class BinaryGenericConsumer implements GenericConsumer
{

    File             file;
    FileOutputStream fos;
    boolean          good;
    boolean          done;

    public BinaryGenericConsumer(File file)
    {
        this.done = false;
        this.file = file;
        try
        {
            file.getParentFile().mkdirs();
            this.fos = new FileOutputStream(file);
            good = true;
        } catch (FileNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            good = false;
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try
        {
            oos = new ObjectOutputStream(baos);
            dataRecord.writeExternal(oos);
            fos.write(baos.toByteArray());
            // FileUtils.writeByteArrayToFile(file, baos.toByteArray(), true);
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            throw new ConsumerException("Consumer failed to consume for output file:" + file.getAbsolutePath()
                    + ", and DataRecord:" + dataRecord.toString());
        }

    }

    @Override
    public boolean flushAndClose()
    {
        try
        {
            if (fos != null)
            {
                fos.flush();
                fos.close();
                return true;
            }
        } catch (IOException ioe)
        {
            throw new ConsumerException("Unable to flush for output file:" + file.getAbsolutePath(), ioe);
        }
        return true;
    }

}

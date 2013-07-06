package com.mvdb.etl.consumer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.dao.impl.JdbcGenericDAO;
import com.mvdb.etl.data.DataRecord;

public class SequenceFileConsumer implements GenericConsumer
{
    private static Logger logger = LoggerFactory.getLogger(SequenceFileConsumer.class);
    File                  file;
    FileOutputStream      fos;

    boolean               good;
    boolean               done;
    SequenceFile.Writer   writer;

    public SequenceFileConsumer(File dataFile)
    {
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", hadoopLocalFS);
        FileSystem fs;
        try
        {
            fs = FileSystem.get(conf);
            if (conf != null)
            {
                Path path = new Path(dataFile.getAbsolutePath());
                writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, BytesWritable.class);
            }
            good = true;
        } catch (IOException e)
        {
            logger.error("SequenceFileConsumer constructor:", e);
            good = false;
            return;
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
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(dataRecord);
            oos.flush();
            BytesWritable value = new BytesWritable(bos.toByteArray());
            IntWritable key = new IntWritable(1);
            writer.append(key, value);
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
        if (writer != null)
        {
            IOUtils.closeStream(writer);
        }
        return true;
    }

}

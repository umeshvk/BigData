package com.mvdb.platform.action;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.actions.ScanDBChanges;
import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.etl.data.GenericIdRecord;
import com.mvdb.etl.data.IdRecord;
import com.mvdb.platform.data.MultiVersionRecord;

public class ScanDBTable
{
    private static Logger logger = LoggerFactory.getLogger(ScanDBChanges.class);
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        //ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/20030115050607/ids-orders.dat"); 
        //ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/20030131050607/ids-orders.dat"); 
        ///home/umesh/.mvdb/etl/data/alpha/20030117050607
        //ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/db/tmp-49728/orders-r-00000"); 
        ScanDBTable.scan("/home/umesh/ordersmv.dat"); 
        
    }

    
    
    public static boolean scan(String dataFileName)
    {
        File dataFile = new File(dataFileName);
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopLocalFS);
//        String dataFileName = "data-" + objectName + ".dat";
//        File dataFile = new File(snapshotDirectory, dataFileName);
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
                Object object = ois.readObject();
                if(object instanceof GenericDataRecord)
                {
                    GenericDataRecord dr = (GenericDataRecord) object;
                    System.out.println(dr.toString());
                }
                else if(object instanceof MultiVersionRecord)
                {
                    MultiVersionRecord mvr = (MultiVersionRecord) object;
                    System.out.println(mvr.toString());
                }
                else if(object instanceof GenericIdRecord)
                {
                    IdRecord idr = (IdRecord) object;
                    System.out.println(idr.toString());
                }
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
}

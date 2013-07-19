package com.mvdb.platform.action;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;

import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.platform.data.MultiVersionRecord;

public class VersionMerge
{
    private static Logger logger = LoggerFactory.getLogger(VersionMerge.class);

    public static class MyIdentityMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>
    {

        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String customer = fileSplit.getPath().getParent().getName();
            System.out.println("File name "+filename);
            System.out.println("Directory and File name"+fileSplit.getPath().toString());
            context.write(key, value);
        }
    }

    public static class MyIdentityReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>
    {

        public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException,
                InterruptedException
        {

            Iterator<BytesWritable> itr = values.iterator();
            List<GenericDataRecord> gdrList = new ArrayList<GenericDataRecord>();
            MultiVersionRecord mvr = null;
            int mvrCount = 0; 
            while (itr.hasNext())
            {
                BytesWritable bw = itr.next();

                byte[] bytes = bw.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);

                try
                {
                    Object record = ois.readObject();
                    if(record instanceof MultiVersionRecord)
                    {
                        mvrCount++;
                        if(mvrCount > 1)
                        {
                            System.out.println("!!!ERROR!!!: Found two or more MultiVersionRecords in reducer for key:" + key);
                            System.out.println(mvr.toString());
                        }
                        mvr = (MultiVersionRecord)record;
                        System.out.println(mvr.toString());
                    }
                    if(record instanceof GenericDataRecord)
                    {
                        GenericDataRecord gdr = (GenericDataRecord)record;
                        gdrList.add(gdr);
                        System.out.println(gdr.toString());
                    }
                } catch (ClassNotFoundException e)
                {
                    e.printStackTrace();
                }

            }
            
            if(mvr == null) { 
                mvr = new MultiVersionRecord();
            }                

            
            Collections.sort(gdrList);
            for(GenericDataRecord gdr : gdrList)
            {
                Object keyValue = gdr.getKeyValue();                
                System.out.println("gdr keyValue:" + keyValue);
                long timestamp = gdr.getTimestampLongValue();
                System.out.println("gdr timestamp:" + timestamp);
                mvr.addLatestVersion(gdr);                   
            }
            
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(mvr);
            oos.flush();
            BytesWritable bwOut = new BytesWritable(bos.toByteArray());            
            context.write(key, bwOut);
        }
    }

    public static void main(String[] args) throws Exception
    {
        logger.error("error1");
        logger.warn("warning1");
        logger.info("info1");
        logger.debug("debug1");
        logger.trace("trace1");

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        StatusPrinter.print(lc);

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: scanner <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "scanner");
        job.setJarByClass(VersionMerge.class);
        job.setMapperClass(MyIdentityMapper.class);
        job.setReducerClass(MyIdentityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
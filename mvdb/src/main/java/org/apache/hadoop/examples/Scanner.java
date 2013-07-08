package org.apache.hadoop.examples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.data.GenericDataRecord;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;

public class Scanner
{
    private static Logger logger = LoggerFactory.getLogger(Scanner.class);

    public static class MyIdentityMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable>
    {

        public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException
        {

            context.write(key, value);
        }
    }

    public static class MyIdentityReducer extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable>
    {

        public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException,
                InterruptedException
        {

            Iterator<BytesWritable> itr = values.iterator();
            while (itr.hasNext())
            {
                BytesWritable bw = itr.next();

                byte[] bytes = bw.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                GenericDataRecord dr = null;
                try
                {
                    dr = (GenericDataRecord) ois.readObject();
                    System.out.println(dr.toString());
                } catch (ClassNotFoundException e)
                {
                    e.printStackTrace();
                }
                

                context.write(key, bw);
            }
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
        job.setJarByClass(Scanner.class);
        job.setMapperClass(MyIdentityMapper.class);
        job.setReducerClass(MyIdentityReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
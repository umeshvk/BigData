package com.mvdb.platform.action.merge;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.platform.action.MergeKey;

public  class VersionMergeMapper extends Mapper<Text, BytesWritable, MergeKey, BytesWritable>
{
    private static Logger logger = LoggerFactory.getLogger(VersionMergeMapper.class);
    
    MergeKey mergeKey= new MergeKey();
    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
    {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        if(filename.startsWith("data-") == false && filename.startsWith("ids-") == false)
        {
            return;
        }
        String timestamp = fileSplit.getPath().getParent().getName();
        String customer = fileSplit.getPath().getParent().getParent().getName();
        System.out.println("File name "+filename);
        System.out.println("Directory and File name"+fileSplit.getPath().toString());
        
        mergeKey.setCompany(customer);
        String fn = filename.substring(filename.indexOf('-') + 1, filename.lastIndexOf(".dat"));            
        mergeKey.setTable(fn);
        mergeKey.setId(key.toString());
        
        context.write(mergeKey, value);
    }
}
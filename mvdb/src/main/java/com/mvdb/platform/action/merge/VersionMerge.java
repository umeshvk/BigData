package com.mvdb.platform.action.merge;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.actions.ActionUtils;
import com.mvdb.etl.actions.ConfigurationKeys;
import com.mvdb.platform.action.MergeKey;

public class VersionMerge
{
    private static Logger logger = LoggerFactory.getLogger(VersionMerge.class);

    public static void main(String[] args) throws Exception
    {        

        ActionUtils.setUpInitFileProperty();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //Also add  lastMergedTimeStamp and  mergeUptoTimestamp and passive db name which would be mv1 or mv2
        if (otherArgs.length != 1)
        {
            System.err.println("Usage: versionmerge <customer-directory>");
            System.exit(1);
        }
        //Example: file:/home/umesh/.mvdb/etl/data/alpha
        //Example: hdfs://localhost:9000/data/alpha
        String customerDirectory = otherArgs[0];
        String customerName = new Path(customerDirectory).getName();
        

        
        String passiveDBName = ActionUtils.getPassiveDBName(customerName);
        
        if(passiveDBName == null)
        {
            //String activeDBName = ActionUtils.getConfigurationValue(customerName, ConfigurationKeys.ACTIVE_DB_DIR);
            System.err.println(String.format("Configuration error. Unable to find the passiveDBName for customer %s", customerName));
            System.exit(2);
        }
        
        
        String lastMergedDirName = ActionUtils.getConfigurationValue(customerName + "." + passiveDBName, ConfigurationKeys.LAST_MERGE_TO_MVDB_DIRNAME); //otherArgs[1];
        String lastCopiedDirName = ActionUtils.getConfigurationValue(customerName + "." + passiveDBName, ConfigurationKeys.LAST_COPY_TO_HDFS_DIRNAME); //otherArgs[2];
        
        if(lastMergedDirName.compareTo(lastCopiedDirName) >= 0)
        {
            System.err.println(String.format("Merge(%s) has already caught up with last copied snapshot(%s)", lastMergedDirName, lastCopiedDirName));
            System.exit(0);
        }
        
        org.apache.hadoop.conf.Configuration conf1 = new org.apache.hadoop.conf.Configuration();
        conf1.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf1);
        
        Path topPath = new Path(customerDirectory);
        
        
        //Clean scratch db
        Path passiveDbPath = new Path(topPath, "db/" + passiveDBName);
        Path tempDbPath = new Path(topPath, "db/" + "tmp-" + (int)(Math.random() * 100000));        
        Path passiveDBBackupPath = new Path(topPath, "db/" + passiveDBName + ".old");
        
        
        
        if(hdfsFileSystem.exists(tempDbPath))
        {
            boolean success = hdfsFileSystem.delete(tempDbPath, true);
            if(success == false)
            {
                System.err.println(String.format("Unable to delete temp directory %s", tempDbPath.toString()));
                System.exit(1);
            }
        }
        //last three parameters are hardcoded and  the nulls must be replaced later after changing inout parameters. 
        Path[] inputPaths = getInputPaths(hdfsFileSystem, topPath, lastMergedDirName, lastCopiedDirName, passiveDbPath);
        String lastDirName = getLastDirName(inputPaths);
        Set<String> tableNameSet = new HashSet<String>();
        Set<String> timeStampSet = new HashSet<String>();
        for(Path path: inputPaths)
        {
            timeStampSet.add(path.getParent().getName());
            String filename = path.getName();
            if(filename.endsWith(".dat") == false)
            {
                  continue;
            }
            String fn = filename.substring(filename.indexOf('-') + 1, filename.lastIndexOf(".dat"));
            tableNameSet.add(fn);
        }
        
        String timeStampCSV = getCSV(timeStampSet, "\\d{14}");
        conf.set("timeStampCSV", timeStampCSV);
        Job job = new Job(conf, "versionmerge");
        job.setJarByClass(VersionMerge.class);
        job.setMapperClass(VersionMergeMapper.class);
        job.setReducerClass(VersionMergeReducer.class);
        job.setMapOutputKeyClass(MergeKey.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        

        for(Path inputPath : inputPaths)
        {
            FileInputFormat.addInputPath(job, inputPath);
        }
        FileOutputFormat.setOutputPath(job, tempDbPath);
        
       
        for(String table: tableNameSet)
        {
            MultipleOutputs.addNamedOutput(job, table, SequenceFileOutputFormat.class , Text.class, BytesWritable.class);
        }
        boolean success = job.waitForCompletion(true);     
        System.out.println("Success:" + success);
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        if(success && lastDirName != null)
        {
            ActionUtils.setConfigurationValue(/*new Path(customerDirectory).getName()*/customerName + "." + passiveDBName , ConfigurationKeys.LAST_MERGE_TO_MVDB_DIRNAME, lastDirName);
        }
        if(success == false)
        {
            System.err.println("VersionMerge: VersionMerge Batch Job Failed. Human intervention required.");
            System.exit(3);
        }
        if(hdfsFileSystem.exists(passiveDBBackupPath))
        {
            success = hdfsFileSystem.delete(passiveDBBackupPath, true);
        }
        if(success == false) 
        {
            System.err.println("VersionMerge: Unable to delete passive db backup" + passiveDBBackupPath.getName() + ". Human intervention required.");
            System.exit(4);
        }
        
        success = hdfsFileSystem.rename(passiveDbPath, passiveDBBackupPath);
        if(success == false) 
        {
            System.err.println(String.format("VersionMerge: Unable to rename passive db path %s to %s. Human intervention required.", passiveDbPath.toString(), passiveDBBackupPath.toString() ));
            System.exit(5);
        }
        
        success = hdfsFileSystem.rename(tempDbPath, passiveDbPath);
        if(success == false) 
        {
            System.err.println(String.format("VersionMerge: Unable to rename passive db path %s to %s. Human intervention required.", tempDbPath.getName(), passiveDBBackupPath.getName() ));
            System.exit(6);
        }
        //Flip active and passive directory
        ActionUtils.setConfigurationValue(customerName, ConfigurationKeys.ACTIVE_DB_DIR, passiveDBName);

        System.exit(0);
    }
    
    private static String getLastDirName(Path[] inputPaths)
    {
        String lastTimestampDirName = "00000000000000";     
        for(Path path : inputPaths)
        {
            //
            String timestampDirName = path.getParent().getName();
            if(timestampDirName.matches("\\d{14}")   && timestampDirName.compareTo(lastTimestampDirName) > 0)
            {
                lastTimestampDirName = timestampDirName;
            }            
        }
        return lastTimestampDirName;
    }

    public static String getCSV(Collection collection, String regex)
    {
        StringBuffer sb = new StringBuffer();
        Iterator iter  = collection.iterator();
        while(iter.hasNext())
        {
            String str = iter.next().toString(); 
            if(str.matches(regex)) { 
                sb.append(str).append(",");
            }
        }
        if(sb.length() > 0) { 
            sb.setLength(sb.length()-1);
        }
        return sb.toString();
    }
    
    /**           
     * @param hdfsFileSystem
     * @param topPath
     * @return
     * @throws IOException
     */

    private static Path[] getInputPaths(FileSystem hdfsFileSystem, Path topPath, String lastMergedDirName, String lastcopiedDirName, Path passiveDbPath) throws IOException
    {
        if(hdfsFileSystem.exists(passiveDbPath) == false)
        {
            hdfsFileSystem.mkdirs(passiveDbPath);
        }
        List<Path> pathList = new ArrayList<Path>();        
        buildInputPathList(hdfsFileSystem, topPath, pathList, lastMergedDirName, lastcopiedDirName);
        pathList.add(passiveDbPath);
        Path[] inputPaths = pathList.toArray(new Path[0]);        
        return inputPaths;
    }

    private static void buildInputPathList(FileSystem fileSystem, Path topPath, List<Path> pathList, String lastMergedDirName, String lastcopiedDirName) throws IOException
    {
        FileStatus topPathStatus = fileSystem.getFileStatus(topPath);
        if(topPathStatus.isDir() == false)
        {
            String topPathFullName = topPath.toString(); 
            String[] tokens = topPathFullName.split("/");
            String fileName = tokens[tokens.length-1];
            if((fileName.startsWith("data-") && fileName.endsWith(".dat")) ||
                    (fileName.startsWith("ids-") && fileName.endsWith(".dat")) )
            {
                String timeStamp = tokens[tokens.length-2];
                if(timeStamp.compareTo(lastMergedDirName) > 0 && timeStamp.compareTo(lastcopiedDirName) <= 0) 
                {
                    pathList.add(topPath);
                }
            }
            return; //This is a leaf
        }
        
        FileStatus[] fsArray = fileSystem.listStatus(topPath);
        for(FileStatus fileStatus: fsArray)
        {
            Path path = fileStatus.getPath();            
            buildInputPathList(fileSystem, path, pathList, lastMergedDirName, lastcopiedDirName);          
        }
    }
    
}



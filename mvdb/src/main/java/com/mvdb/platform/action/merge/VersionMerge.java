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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.actions.ActionUtils;
import com.mvdb.etl.actions.ConfigurationKeys;
import com.mvdb.etl.actions.Top;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.data.Metadata;
import com.mvdb.platform.action.MergeKey;

public class VersionMerge
{
    private static Logger logger = LoggerFactory.getLogger(VersionMerge.class);

    public static void main(String[] args) throws Exception
    {        

        ActionUtils.setUpInitFileProperty();

        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
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
        

        FileSystem hdfsFileSystem = FileSystem.get(configuration);
        
//        testMD(conf1);
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
            System.out.println(">>>table:" + fn);
            tableNameSet.add(fn);
            tableNameSet.add("schema" + fn);
        }
        
        String timeStampCSV = getCSV(timeStampSet, "\\d{14}");
        configuration.set("timeStampCSV", timeStampCSV);
        Job job = new Job(configuration, "versionmerge");
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
            System.out.println("inputPath:" + inputPath);
            FileInputFormat.addInputPath(job, inputPath);
        }
        FileOutputFormat.setOutputPath(job, tempDbPath);
        
       
        for(String table: tableNameSet)
        {
            MultipleOutputs.addNamedOutput(job, table, SequenceFileOutputFormat.class , Text.class, BytesWritable.class);
            //MultipleOutputs.addNamedOutput(job, "schema" + table, SequenceFileOutputFormat.class , Text.class, BytesWritable.class);
        }
        boolean success = job.waitForCompletion(true); 
        
        Counters counters = job.getCounters();
        long errorCount = counters.findCounter(VersionMergeCounter.ERROR_COUNTER).getValue();
        System.out.println("errorCount:" + errorCount);
        int counterCount = counters.countCounters();
        
        System.out.println("Success:" + success);
        
        
        organizeForHive(hdfsFileSystem, tempDbPath, tableNameSet);
        
        
        
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
    
    private static void testMD(Configuration conf)
    {
        ApplicationContext context = Top.getContext();
        GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
                
        //Metadata md = genericDAO.readMetadata(new File("/home/umesh/.mvdb/etl/data/alpha/20030115050607/schema-orderlineitem.dat").toURI().toString(), conf1);
        Metadata md = genericDAO.readMetadata("hdfs://localhost:9000/data/alpha/20030115050607/schema-orderlineitem.dat", conf);
        int ii =0; 
        int jj = ii; 
        
    }

    private static void organizeForHive(FileSystem hdfsFileSystem, Path tempDbPath, Set<String> tableNameSet) throws IOException
    {
        for(String tableName : tableNameSet)
        {
            Path tableDir = new Path(tempDbPath, tableName);            
            boolean success = hdfsFileSystem.mkdirs(tableDir);
            if(success == false)
            {
                System.err.println(String.format("VersionMerge: Uanble to create table %s in directory %s. Human intervention required.", tableName, tableDir.getName()));
                System.exit(10);
            }
        }
                
        FileStatus[] fsArray = hdfsFileSystem.listStatus(tempDbPath);
        for(FileStatus fileStatus: fsArray)
        {
            Path path = fileStatus.getPath(); 
            String fileName = path.getName();
            String[] tokens = fileName.split("-r-");
            if(tokens.length == 2)
            {
                Path destDirPath = new Path(tempDbPath, tokens[0]);
                Path dest = new Path(destDirPath, fileName);
                hdfsFileSystem.rename(path, dest);
            }
        }
        
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
        buildDBInputPathList(hdfsFileSystem, passiveDbPath, pathList);
        //pathList.add(passiveDbPath);
        Path[] inputPaths = pathList.toArray(new Path[0]);        
        return inputPaths;
    }

    private static void buildInputPathList(FileSystem fileSystem, Path thePathToProcess, List<Path> pathList, String lastMergedDirName, String lastcopiedDirName) throws IOException
    {
        FileStatus topPathStatus = fileSystem.getFileStatus(thePathToProcess);
        if(topPathStatus.isDir() == false)
        {
            //This is a file
            String topPathFullName = thePathToProcess.toString(); 
            String[] tokens = topPathFullName.split("/");
            String fileName = tokens[tokens.length-1];
            if((fileName.startsWith("data-") && fileName.endsWith(".dat")) ||
                    (fileName.startsWith("ids-") && fileName.endsWith(".dat")) ||
                    (fileName.startsWith("schema-") && fileName.endsWith(".dat")))
            {
                String timeStamp = tokens[tokens.length-2];
                if(timeStamp.compareTo(lastMergedDirName) > 0 && timeStamp.compareTo(lastcopiedDirName) <= 0) 
                {
                    pathList.add(thePathToProcess);
                }
            }
            return; //This is a leaf
        }
        
        FileStatus[] fsArray = fileSystem.listStatus(thePathToProcess);
        for(FileStatus fileStatus: fsArray)
        {
            Path path = fileStatus.getPath();            
            buildInputPathList(fileSystem, path, pathList, lastMergedDirName, lastcopiedDirName);          
        }
    }
    
    private static void buildDBInputPathList(FileSystem fileSystem, Path thePathToProcess, List<Path> pathList) throws IOException
    {
        FileStatus topPathStatus = fileSystem.getFileStatus(thePathToProcess);
        if(topPathStatus.isDir() == false)
        {
            //This is a file
            String topPathFullName = thePathToProcess.toString(); 
            String[] tokens = topPathFullName.split("/");
            String fileName = tokens[tokens.length-1];
            if((fileName.contains("-r-") == true &&  fileName.contains("part-r-") == false))
            {
                pathList.add(thePathToProcess);
            }
            return; //This is a leaf
        }
        
        FileStatus[] fsArray = fileSystem.listStatus(thePathToProcess);
        for(FileStatus fileStatus: fsArray)
        {
            Path path = fileStatus.getPath();            
            buildDBInputPathList(fileSystem, path, pathList);          
        }
    }
    

    
}



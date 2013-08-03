package com.mvdb.platform.action;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.actions.ActionUtils;
import com.mvdb.etl.actions.ConfigurationKeys;
import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.etl.data.GenericIdRecord;
import com.mvdb.platform.data.MultiVersionRecord;

public class VersionMerge
{
    private static Logger logger = LoggerFactory.getLogger(VersionMerge.class);

    public static class VersionMergeMapper extends Mapper<Text, BytesWritable, MergeKey, BytesWritable>
    {
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
                    
            //fn = filename.replaceAll("-", "");             
            //fn = fn.replaceAll(".dat", "");
            
            mergeKey.setTable(fn);
            mergeKey.setId(key.toString());
            
            context.write(mergeKey, value);
        }
    }


    private static class TimestampData
    {
        String timestamp; 
        GenericDataRecord gdr; 
        GenericIdRecord gir;
    }
    
    public static class VersionMergeReducer extends Reducer<MergeKey, BytesWritable, Text, BytesWritable>
    {

        MultipleOutputs<Text, BytesWritable> mos;
        SortedMap<String, TimestampData> sortedMap; 
        List<String> sortedTimeStampList; 
        public void setup(Context context)
        {
             mos = new MultipleOutputs<Text, BytesWritable>(context);
             sortedMap = new TreeMap<String, TimestampData>();
             String timeStampCSV = context.getConfiguration().get("timeStampCSV");
             String[] timeStampArray = timeStampCSV.split(",");
             sortedTimeStampList = Arrays.asList(timeStampArray);
             Collections.sort(sortedTimeStampList);
             int i =0; 
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
            mos.close();
            sortedMap.clear();
        }
        
        public void reduce(MergeKey mergeKey, Iterable<BytesWritable> values, Context context) throws IOException,
                InterruptedException
        {
            sortedMap.clear();
            
            for(String ts : this.sortedTimeStampList) { 
                TimestampData timestampData = sortedMap.get(ts);
                if(timestampData == null)
                {
                    timestampData = new TimestampData();
                    sortedMap.put(ts, timestampData);
                    
                }
            }

            
            System.out.println(ManagementFactory.getRuntimeMXBean().getName());
            Iterator<BytesWritable> itr = values.iterator();
            //List<IdRecord> gdrList = new ArrayList<IdRecord>();
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
                            System.out.println("!!!ERROR!!!: Found two or more MultiVersionRecords in reducer for key:" + mergeKey.toString());
                            System.out.println(mvr.toString());
                        }
                        mvr = (MultiVersionRecord)record;
                        System.out.println(mvr.toString());
                    }
                    if(record instanceof GenericDataRecord)
                    {
                        GenericDataRecord gdr = (GenericDataRecord)record;
                        //gdrList.add(gdr);
                        TimestampData timestampData = sortedMap.get(gdr.getRefreshTimeStamp());
                        if(timestampData == null)
                        {
                            timestampData = new TimestampData();
                            sortedMap.put(gdr.getRefreshTimeStamp(), timestampData);
                            
                        }
                        timestampData.gdr = gdr;
                        System.out.println(gdr.toString());
                    }
                    if(record instanceof GenericIdRecord)
                    {
                        GenericIdRecord gir = (GenericIdRecord)record;
                        //gdrList.add(idr);
                        TimestampData timestampData = sortedMap.get(gir.getRefreshTimeStamp());
                        if(timestampData == null)
                        {
                            timestampData = new TimestampData();
                            sortedMap.put(gir.getRefreshTimeStamp(), timestampData);                            
                        }
                        timestampData.gir = gir;
                        System.out.println(gir.toString());
                    }
                } catch (ClassNotFoundException e)
                {
                    e.printStackTrace();
                }

            }
            
            if(mvr == null) { 
                mvr = new MultiVersionRecord();
            }                

            Iterator<String> timestampKeysSetIter = sortedMap.keySet().iterator();
            while(timestampKeysSetIter.hasNext())
            {
                String timestamp = timestampKeysSetIter.next(); 
                TimestampData timestampData = sortedMap.get(timestamp);
                GenericDataRecord gdr = timestampData.gdr; 
                GenericIdRecord gir = timestampData.gir;
                
                if(gdr == null && gir != null)
                {
                    //Majority of rows on a given day will not be updated. This is the most likely scenario.
                    if(mvr.getVersionCount() == 0)
                    {
                        System.out.println("!!!ERROR!!!: Impossible event unless it is the first time an entry is created for a specific record id. No existing record but an id was found for key:" + mergeKey.toString());
                        System.out.println(mvr.toString());
                    } else { 
                        //For an existing record if we find an id but no updates that is ok.                        
                    }
                }
                else if(gdr != null && gir != null)
                {
                    //This is the next most likely scenario.
                    //Ignore the id in this case. 
                    Object keyValue = gdr.getKeyValue();                
                    System.out.println("gdr keyValue:" + keyValue);
                    long ts = gdr.getTimestampLongValue();
                    System.out.println("gdr timestamp:" + ts);
                    mvr.addLatestVersion(gdr);  
                } 
                else if(gdr != null && gir == null)
                {
                    //This is an uncommon case. The id gets deleted from the database after we get the list of updates. 
                    //The window for this to happen is small but can happen.
                    
                    //First add the gdr to mvr to record a change. 
                    Object keyValue = gdr.getKeyValue();                
                    System.out.println("gdr keyValue:" + keyValue);
                    long ts = gdr.getTimestampLongValue();
                    System.out.println("gdr timestamp:" + ts);
                    mvr.addLatestVersion(gdr);  
                    
                    //Second add a row to delete it. The timestamp for delete will be the time 
                    //at which the data download was initiated.
                    //All updates are by definition before that time as the download query is constructed that way. 
                    GenericDataRecord deleteRecord = new GenericDataRecord();
                    deleteRecord.setRefreshTimeStamp(gdr.getRefreshTimeStamp());
                    deleteRecord.setMvdbKeyValue(gdr.getMvdbKeyValue());
                    deleteRecord.setDeleted(true);                   
                    deleteRecord.setMvdbUpdateTime(ActionUtils.getDate(gdr.getRefreshTimeStamp()));   
                    mvr.addLatestVersion(deleteRecord);
                }                
                else if(gdr == null && gir == null)
                {

            
                    //The record exists and it has no updates, but also the id has gone missing. 
                    //This could only mean that the record got deleted. 
                    
                    if(mvr.getVersionCount() == 0)
                    {
                        System.out.println("!!!ERROR!!!: Impossible event unless there is a bug. Found an empty MultiVersionRecord for key:" + mergeKey.toString());
                        System.out.println(mvr.toString());
                    }
                    GenericDataRecord lastGDR = mvr.getVersion(mvr.getVersionCount()-1);
                    
                    if(lastGDR.isDeleted() == false)
                    {
                        GenericDataRecord deleteRecord = new GenericDataRecord();
                        deleteRecord.setRefreshTimeStamp(timestamp);
                        deleteRecord.setMvdbKeyValue(lastGDR.getMvdbKeyValue());
                        deleteRecord.setDeleted(true);                   
                        deleteRecord.setMvdbUpdateTime(ActionUtils.getDate(timestamp));   
                        mvr.addLatestVersion(deleteRecord);
                    }
                    
                }
            }
            
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(mvr);
            oos.flush();
            BytesWritable bwOut = new BytesWritable(bos.toByteArray());            
            context.write(new Text(mergeKey.getId()), bwOut);
            mos.write(mergeKey.getTable(), new Text(mergeKey.getId()), bwOut);
        }
    }


    public static void main(String[] args) throws Exception
    {        
        logger.error("error1");
        logger.warn("warning1");
        logger.info("info1");
        logger.debug("debug1");
        logger.trace("trace1");
        ActionUtils.setUpInitFileProperty();
//        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
//        StatusPrinter.print(lc);

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //Also add  lastMergedTimeStamp and  mergeUptoTimestamp and passive db name which would be mv1 or mv2
        if (otherArgs.length != 3)
        {
            System.err.println("Usage: versionmerge <customer-directory>");
            System.exit(2);
        }
        //Example: file:/home/umesh/.mvdb/etl/data/alpha
        //Example: hdfs://localhost:9000/data/alpha
        String customerDirectory = otherArgs[0];
        String lastMergedDirName = otherArgs[1];
        String lastCopiedDirName = otherArgs[2];
        
        org.apache.hadoop.conf.Configuration conf1 = new org.apache.hadoop.conf.Configuration();
        //conf1.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf1);
        
        Path topPath = new Path(customerDirectory);
        
        //Clean scratch db
        Path passiveDbPath = new Path(topPath, "db/mv1");
        Path tempDbPath = new Path(topPath, "db/tmp-" + (int)(Math.random() * 100000));
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
        Path[] inputPaths = getInputPaths(hdfsFileSystem, topPath, lastMergedDirName, lastCopiedDirName, null);
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
        
        String lastDirName = null;
        if(inputPaths != null && inputPaths.length > 1)
        {
            lastDirName = inputPaths[(inputPaths.length)-2].getParent().getName();
        }
        for(Path inputPath : inputPaths)
        {
            FileInputFormat.addInputPath(job, inputPath);
        }
        FileOutputFormat.setOutputPath(job, tempDbPath);
        
       
        for(String table: tableNameSet)
        {
//            if(table.endsWith(".dat") == false)
//            {
//                continue;
//            }
//            table = table.replaceAll("-", "");
//            table = table.replaceAll(".dat", "");
            MultipleOutputs.addNamedOutput(job, table, SequenceFileOutputFormat.class , Text.class, BytesWritable.class);
        }
        boolean success = job.waitForCompletion(true);     
        System.out.println("Success:" + success);
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        if(success && lastDirName != null)
        {
            ActionUtils.setConfigurationValue(new Path(customerDirectory).getName(), ConfigurationKeys.LAST_MERGE_TO_MVDB_DIRNAME, lastDirName);
        }
        //hdfsFileSystem.delete(passiveDbPath, true);
        //hdfsFileSystem.rename(tempDbPath, passiveDbPath);
        System.exit(success ? 0 : 1);
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

    private static Path[] getInputPaths(FileSystem hdfsFileSystem, Path topPath, String lastMergedDirName, String lastcopiedDirName, Path passiveDbPathT) throws IOException
    {
        Path passiveDbPath = new Path(topPath, "db/mv1");  
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



package com.mvdb.platform.action.merge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.actions.ActionUtils;
import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.etl.data.GenericIdRecord;
import com.mvdb.platform.action.MergeKey;
import com.mvdb.platform.data.MultiVersionRecord;

public class VersionMergeReducer extends Reducer<MergeKey, BytesWritable, Text, BytesWritable>
{

    private static Logger logger = LoggerFactory.getLogger(VersionMergeReducer.class);
    
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
            System.out.println("ts:" + timestamp);
            System.out.println("MergeKey:" + mergeKey.toString());
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
        //context.write(new Text(mergeKey.getId()), bwOut);
        mos.write(mergeKey.getTable(), new Text(mergeKey.getId()), bwOut);
    }
}


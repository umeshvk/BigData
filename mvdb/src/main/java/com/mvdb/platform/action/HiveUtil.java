package com.mvdb.platform.action;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HiveUtil
{
    private static SimpleDateFormat hiveTimeStampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    
    public static String getHiveTimestamp(Date date)
    {
        return hiveTimeStampFormatter.format(date);
    }
    
    public static Date getDateFromHiveTimeStamp(String timestamp)
    {
        try
        {
            if(timestamp == null || timestamp.toLowerCase().startsWith("null"))
            {
                return null; 
            }
            return hiveTimeStampFormatter.parse(timestamp);
        }        
        catch (Throwable e)
        {            
            e.printStackTrace();
            return null;
        }
    }

}

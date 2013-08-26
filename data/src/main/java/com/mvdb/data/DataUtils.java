package com.mvdb.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataUtils
{

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    public static Date getDate(String yyyyMMddHHmmss)
    {
        try
        {
            return sdf.parse(yyyyMMddHHmmss);
        } catch (ParseException e)
        {            
            return null;
        }
        
    }

}

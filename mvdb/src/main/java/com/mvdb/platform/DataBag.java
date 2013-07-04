package com.mvdb.platform;

import java.util.HashMap;
import java.util.Map;

public class DataBag
{

    int version; 
    Map<Integer, String> dataMap; 
    
    public DataBag()
    {
        dataMap = new HashMap();
    }
    
    public void add(Integer key, String value)
    {
        
    }
    
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        // TODO Auto-generated method stub

    }

}

package com.mvdb.etl.dao.impl;

import java.text.DecimalFormat;

import com.mvdb.etl.data.MvdbKeyMaker;

public class GlobalMvdbKeyMaker implements MvdbKeyMaker
{
    DecimalFormat df = new DecimalFormat("0000000000000000");
    @Override
    public String makeKey(Object originalKeyValue)
    {
        if(originalKeyValue instanceof Long)
        {            
            return df.format(originalKeyValue);
        }
        //Default conversion
        return originalKeyValue.toString();
    }

}

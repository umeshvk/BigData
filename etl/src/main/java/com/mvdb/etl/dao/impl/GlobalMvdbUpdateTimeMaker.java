package com.mvdb.etl.dao.impl;

import java.sql.Timestamp;
import java.util.Date;

import com.mvdb.data.MvdbUpdateTimeMaker;


public class GlobalMvdbUpdateTimeMaker implements MvdbUpdateTimeMaker
{

    @Override
    public Date makeMvdbUpdateTime(Object originalUpdateTimeValue)
    {
        if(originalUpdateTimeValue instanceof Timestamp)
        {            
            return (Date)originalUpdateTimeValue;
        }
        //Must map to something meaningful. Otherwise deal with the failure
        return null;
    }

}

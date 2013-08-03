package com.mvdb.etl.data;


public interface DataRecord extends IdRecord
{
    Object getKeyValue();
    long getTimestampLongValue();
    String getMvdbKeyValue();
    void setMvdbKeyValue(String mvdbKeyValue);
}

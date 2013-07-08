package com.mvdb.etl.data;

import java.io.Externalizable;

public interface DataRecord extends Externalizable, Comparable<DataRecord>
{
    Object getKeyValue();
    long getTimestampLongValue();
    String getMvdbKeyValue();
    void setMvdbKeyValue(String mvdbKeyValue);
}

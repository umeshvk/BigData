package com.mvdb.etl.data;

import java.io.Externalizable;

public interface IdRecord extends Externalizable, Comparable<IdRecord>
{
    Object getKeyValue();
    String getMvdbKeyValue();
    long getTimestampLongValue();
}

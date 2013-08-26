package com.mvdb.data;

import java.io.Externalizable;

public interface IdRecord extends AnyRecord, Externalizable, Comparable<IdRecord>
{
    Object getKeyValue();
    String getMvdbKeyValue();
    long getTimestampLongValue();
}

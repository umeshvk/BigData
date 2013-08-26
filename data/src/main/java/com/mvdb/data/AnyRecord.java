package com.mvdb.data;

import java.util.Map;

public interface AnyRecord
{
    Map<String, Object> getDataMap();
    void removeIdenticalColumn(String columnName, Object latestValue);
}

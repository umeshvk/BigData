package com.mvdb.etl.dao;

import java.io.File;
import java.sql.Timestamp;

import com.mvdb.etl.ConsumerException;
import com.mvdb.etl.DataHeader;
import com.mvdb.etl.GenericConsumer;
import com.mvdb.etl.Metadata;

public interface GenericDAO
{
    void fetchMetadata(String objectName, File snapshotDirectory);
    DataHeader fetchAll(File snapshotDirectory, Timestamp modifiedAfter, String objectName);
    boolean scan(File file, int count);
    Metadata getMetadata(String objectName);
}
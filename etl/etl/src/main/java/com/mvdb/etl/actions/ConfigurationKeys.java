package com.mvdb.etl.actions;

public class ConfigurationKeys
{
    //Keys in configuration table
    public static final String GLOBAL_CUSTOMER = "global"; 
    public static final String GLOBAL_DB_URL = "db.url"; 
    public static final String GLOBAL_DB_USER = "db.user"; 
    public static final String GLOBAL_DB_PASSWORD = "db.password"; 
    
    public static final String GLOBAL_LOCAL_DATA_ROOT = "data.root"; 
    public static final String GLOBAL_HDFS_ROOT = "hdfs.root"; 
    public static final String GLOBAL_HADOOP_HOME = "hadoop.home"; 
    public static final String GLOBAL_ACTION_CHAIN_STATUS_FILE = "action.chain.status.file"; 

    //Use when implementing locking
    public static final String REFRESH_LOCK = "refresh-lock";     
    public static final String COPY_TO_HDFS_LOCK = "copy-to-hdfs-lock";
    public static final String MERGE_TO_HDFS_LOCK = "merge-to-mvdb-lock"; 
    
    //Process marker
    public static final String LAST_REFRES_HTIME = "last-refresh-time"; 
    public static final String LAST_COPY_TO_HDFS_TIME = "last-copy-to-hdfs-time"; 
    public static final String LAST_MERGE_TO_MVDB_TIME = "last-merge-to-mvdb-time"; 
    
    /*
    //Keys in  ~/.mvdb/etl.init.properties
    public static final String DataRootKey = "data.root"; 
    public static final String HdfsHomeKey = "hdfs.home"; 
    public static final String HdfsRootKey = "hdfs.root"; 
    */
    //public static final String ActionChainStatusFile = "hdfs.actionchain";


}

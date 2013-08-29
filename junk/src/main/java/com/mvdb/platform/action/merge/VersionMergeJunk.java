package com.mvdb.platform.action.merge;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.actions.Top;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.data.Metadata;

public class VersionMergeJunk
{

    
    private static void testMD(Configuration conf)
    {
        ApplicationContext context = Top.getContext();
        GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
                
        //Metadata md = genericDAO.readMetadata(new File("/home/umesh/.mvdb/etl/data/alpha/20030115050607/schema-orderlineitem.dat").toURI().toString(), conf1);
        Metadata md = genericDAO.readMetadata("hdfs://localhost:9000/data/alpha/20030115050607/schema-orderlineitem.dat", conf);

        
    }

    
    
}



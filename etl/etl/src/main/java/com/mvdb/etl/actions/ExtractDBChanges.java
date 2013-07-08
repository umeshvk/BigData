package com.mvdb.etl.actions;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.consumer.OrderJsonFileConsumer;
import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.model.Configuration;

public class ExtractDBChanges
{
    private static Logger logger = LoggerFactory.getLogger(ExtractDBChanges.class);


    
    
    
    public static void main(String[] args) throws JSONException
    {
        //String schemaDescription = "{ 'root' : [{'table' : 'orders', 'keyColumn' : 'order_id', 'updateTimeColumn' : 'update_time'}]}";
        logger.error("error");
        logger.warn("warning");
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");
        


        
        String customerName = null;
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        final Options posixOptions = constructPosixOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
            if (commandLine.hasOption("customer"))
            {
                customerName = commandLine.getOptionValue("customer");
            }
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }

        if (customerName == null)
        {
            System.err.println("Could not find customerName. Aborting...");
            System.exit(1);
        }

        ApplicationContext context = new ClassPathXmlApplicationContext("Spring-Module.xml");

        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");
        final GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
        File snapshotDirectory = getSnapshotDirectory(configurationDAO, customerName);
        try
        {
            FileUtils.writeStringToFile(new File("/tmp/etl.extractdbchanges.directory.txt"), snapshotDirectory.getName(), false);
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
            return;
        }
        long currentTime = new Date().getTime();
        Configuration lastRefreshTimeConf = configurationDAO.find(customerName, "last-refresh-time");
        Configuration schemaDescriptionConf = configurationDAO.find(customerName, "schema-description");
        long lastRefreshTime = Long.parseLong(lastRefreshTimeConf.getValue());
        OrderJsonFileConsumer orderJsonFileConsumer = new OrderJsonFileConsumer(snapshotDirectory);
        Map<String, ColumnMetadata> metadataMap = orderDAO.findMetadata();
        //write file schema-orders.dat in snapshotDirectory
        genericDAO.fetchMetadata("orders", snapshotDirectory);
        //writes files: header-orders.dat, data-orders.dat in snapshotDirectory
        JSONObject json = new JSONObject(schemaDescriptionConf.getValue());
        JSONArray rootArray = json.getJSONArray("root");
        int length = rootArray.length();
        for(int i=0;i<length;i++)
        {
            JSONObject jsonObject = rootArray.getJSONObject(i);
            String  table = jsonObject.getString("table");
            String  keyColumnName = jsonObject.getString("keyColumn");
            String  updateTimeColumnName = jsonObject.getString("updateTimeColumn");
            System.out.println(
                    "table:" + table + 
                    ", keyColumn: " + keyColumnName + 
                    ", updateTimeColumn: " + updateTimeColumnName);
            genericDAO.fetchAll2(snapshotDirectory, new Timestamp(lastRefreshTime), table, keyColumnName, updateTimeColumnName);
        }
        
        
        //orderDAO.findAll(new Timestamp(lastRefreshTime), orderJsonFileConsumer);
        Configuration updateRefreshTimeConf = new Configuration(customerName, "last-refresh-time",
                String.valueOf(currentTime));
        configurationDAO.update(updateRefreshTimeConf, String.valueOf(lastRefreshTimeConf.getValue()));

    }



    private static File getSnapshotDirectory(ConfigurationDAO configurationDAO, String customerName)
    {
        Configuration dataRootDirConfig = configurationDAO.find("global", "data_root");
        String dataRootDir = dataRootDirConfig.getValue();
        File customerDir = new File(dataRootDir, customerName);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String snapshotDirectoryName = sdf.format(new Date());
        File snapshotDirectory = new File(customerDir, snapshotDirectoryName);
        return snapshotDirectory;
    }

    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customer", true, "Customer Name");

        return posixOptions;
    }
}

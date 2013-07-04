package com.mvdb.etl;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Configuration;

public class ExtractDBChanges
{
    public static final String SEQUENCE_NAME = "com_mvdb_etl_dao_OrderDAO";

    public static void main(String[] args)
    {

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
        long currentTime = new Date().getTime();
        Configuration lastRefreshTimeConf = configurationDAO.find(customerName, "last-refresh-time");
        long lastRefreshTime = Long.parseLong(lastRefreshTimeConf.getValue());
        OrderJsonFileConsumer orderJsonFileConsumer = new OrderJsonFileConsumer(snapshotDirectory);
        Map<String, ColumnMetadata> metadataMap = orderDAO.findMetadata();
        //write file schema-orders.dat in snapshotDirectory
        genericDAO.fetchMetadata("orders", snapshotDirectory);
        //writes files: header-orders.dat, data-orders.dat in snapshotDirectory
        genericDAO.fetchAll(snapshotDirectory, new Timestamp(lastRefreshTime), "orders");
        
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

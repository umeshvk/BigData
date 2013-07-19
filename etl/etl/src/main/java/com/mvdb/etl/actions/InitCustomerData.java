package com.mvdb.etl.actions;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.monitoring.TimedExecutor;
import com.mvdb.etl.util.RandomUtil;

public class InitCustomerData  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(InitCustomerData.class);
    
    public static void main(String[] args)
    {
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.InitDB.complete", "200initdb.sh not executed yet. Exiting");
        ActionUtils.assertFileDoesNotExist("~/.mvdb/status.InitCustomerData.complete", "InitCustomerData already done. Start with 100init.sh if required. Exiting");
        ActionUtils.setUpInitFileProperty();
        ActionUtils.createMarkerFile("~/.mvdb/status.InitCustomerData.start");
        logger.error("error");
        logger.warn("warning");
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");
        
        String customerName= null; 
        int batchCountF = 0;
        int batchSizeF = 0;
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
            if (commandLine.hasOption("batchSize"))
            {
                String batchSizeStr = commandLine.getOptionValue("batchSize");
                batchSizeF = Integer.parseInt(batchSizeStr);
            }
            if (commandLine.hasOption("batchCount"))
            {
                String batchCountStr = commandLine.getOptionValue("batchCount");
                batchCountF = Integer.parseInt(batchCountStr);
            }
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }

        // if you have time,
        // it's better to create an unit test rather than testing like this :)

        ApplicationContext context = new ClassPathXmlApplicationContext("Spring-Module.xml");

        
        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");        
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");

        initData(orderDAO, batchCountF, batchSizeF);
        initConfiguration(configurationDAO, customerName);
        
        int total = orderDAO.findTotalOrders();
        System.out.println("Total : " + total);

        long max = orderDAO.findMaxId();
        System.out.println("maxid : " + max);
        
        ActionUtils.createMarkerFile("~/.mvdb/status.InitCustomerData.complete");

    }

    private static void initConfiguration(ConfigurationDAO configurationDAO, String customerName)
    {
        String schemaDescription = "{ ''root'' : [{''table'' : ''orders'', ''keyColumn'' : ''order_id'', ''updateTimeColumn'' : ''update_time''}]}";
        String[] sqlArray = new String[] {
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'last-refresh-time', '0', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'extraction-lock', '0', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'load-lock', '0', '', '');", 
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'schema-description', '" + schemaDescription + "', '', '');"
                };
        configurationDAO.executeSQl(sqlArray);        
    }

    private static void initData(final OrderDAO orderDAO, final int batchCount, final int batchSize)
    {
        final long tm = new Date().getTime();
        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++)
        {
            final int batchIndexFinal = batchIndex;
            TimedExecutor te = new TimedExecutor() {

                @Override
                public void execute()
                {

                    List<Order> orders = new ArrayList<Order>();
                    for (int recordIndex = 0; recordIndex < batchSize; recordIndex++)
                    {
                        Order order = new Order(orderDAO.getNextSequenceValue(), RandomUtil.getRandomString(5),
                                RandomUtil.getRandomInt(), new Date(tm), new Date(tm));
                        orders.add(order);
                    }
                    orderDAO.insertBatch(orders);
                    System.out.println(String.format("Completed Batch %d of %d where size of batch is %s",
                            batchIndexFinal + 1, batchCount, batchSize));
                }

            };
            long runTime = te.timedExecute();
            System.out.println("Ran for seconds: " + runTime / 1000);
        }
        
    }



    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customer", true, "Customer Name");
        posixOptions.addOption("batchCount", true, "Number of batches. Each batch is a transaction.");
        posixOptions.addOption("batchSize", true, "Number of records inserted in each batch");
        return posixOptions;
    }
}





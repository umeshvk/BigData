package com.mvdb.etl.actions;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.monitoring.TimedExecutor;
import com.mvdb.etl.util.RandomUtil;

public class InitCustomerData
{
    public static void main(String[] args)
    {

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

    }

    private static void initConfiguration(ConfigurationDAO configurationDAO, String customerName)
    {
        String[] sqlArray = new String[] {
                "INSERT INTO configuration (customer, name, value) VALUES  ('" + customerName + "', 'last-refresh-time', '0');",
                "INSERT INTO configuration (customer, name, value) VALUES  ('" + customerName + "', 'extraction-lock', '0');",
                "INSERT INTO configuration (customer, name, value) VALUES  ('" + customerName + "', 'load-lock', '0');"
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

    private static void createConfiguration(ApplicationContext context)
    {
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");

        String[] commands = {
                "DROP TABLE IF EXISTS configuration;",
                "CREATE TABLE  configuration (" + " customer varchar(128)  NOT NULL, " + " name varchar(128)  NOT NULL,"
                        + " name varchar(128)  NOT NULL" + " ); ", 
                "INSERT INTO configuration (customer, name, value) VALUES  ('global', 'data_root', '/home/umesh/data/etl');", 
                 "COMMIT;" };

        
        configurationDAO.executeSQl(commands);
        
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

// Order order1 = new Order(orderDAO.getNextSequenceValue(),
// RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
// Date(tm-10000000000L), new Date(tm-5000000000L));
// Order order3 = new Order(orderDAO.getNextSequenceValue(),
// RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
// Date(tm-20000000000L), new Date(tm-4000000000L));
// Order order2 = new Order(orderDAO.getNextSequenceValue(),
// RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
// Date(tm-30000000000L), new Date(tm-6000000000L));
// orders.add(order1);
// orders.add(order2);
// orders.add(order3);

/**
 * CREATE TABLE orders ( ORDER_ID bigint NOT NULL, NOTE varchar(100) NOT NULL,
 * SALE_CODE int NOT NULL, CREATE_TIME timestamp NOT NULL, UPDATE_TIME timestamp
 * NOT NULL ); COMMIT;
 * 
 * CREATE SEQUENCE com_etl_good_bad_Order START 101; commit; SELECT
 * nextval('com_etl_good_bad_Order');
 */

/**
 * Order orderA = orderDAO.findByOrderId(1); System.out.println("Order A : " +
 * orderA);
 * 
 * 
 * 
 * List<Order> orderAs = orderDAO.findAll(); for(Order order: orderAs){
 * System.out.println("Order As : " + order); }
 **/

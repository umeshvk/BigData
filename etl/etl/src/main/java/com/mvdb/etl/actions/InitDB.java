package com.mvdb.etl.actions;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.util.db.SequenceNames;

public class InitDB
{
    public static void main(String[] args)
    {

        ApplicationContext context = new ClassPathXmlApplicationContext("Spring-Module.xml");

        createConfiguration(context);
        createOrder(context);

    }

    private static void createOrder(ApplicationContext context)
    {
        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");

        String[] commands = {
                "DROP SEQUENCE IF EXISTS " + SequenceNames.ORDER_SEQUENCE_NAME + ";",
                "CREATE SEQUENCE com_mvdb_etl_dao_OrderDAO START 1;",
                "COMMIT;",
                "DROP TABLE IF EXISTS orders;",
                "CREATE TABLE  orders (" + " ORDER_ID bigint  NOT NULL, " + " NOTE varchar(200) NOT NULL,"
                        + " SALE_CODE int NOT NULL," + " CREATE_TIME timestamp NOT NULL,"
                        + " UPDATE_TIME timestamp NOT NULL, " + "constraint order_pk PRIMARY KEY (ORDER_ID)" + " ); ", "COMMIT;" };

        
        orderDAO.executeSQl(commands);
        
    }

    private static void createConfiguration(ApplicationContext context)
    {
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");

        String[] commands = {
                "DROP TABLE IF EXISTS configuration;",
                "CREATE TABLE  configuration (" + " customer varchar(128)  NOT NULL, " + " name varchar(128)  NOT NULL,"
                        + " value varchar(128)  NOT NULL, " + "UNIQUE (customer, name, value)); ", 
                "INSERT INTO configuration (customer, name, value) VALUES  ('global', 'data_root', '/home/umesh/data/etl');", 
                 "COMMIT;" };

        
        configurationDAO.executeSQl(commands);
        
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

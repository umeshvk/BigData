package com.mvdb.etl;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.OrderDAO;

public class ExtractDBChanges
{
    public static final String SEQUENCE_NAME = "com_mvdb_etl_dao_OrderDAO";

    public static void main(String[] args)
    {

        ApplicationContext context = new ClassPathXmlApplicationContext("Spring-Module.xml");

        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");

        OrderJsonFileConsumer orderJsonFileConsumer= new OrderJsonFileConsumer(new File("/tmp/test.out"));
        orderDAO.findAll(new Timestamp(new Date().getTime()- 10000000L), orderJsonFileConsumer);
        

    }
}

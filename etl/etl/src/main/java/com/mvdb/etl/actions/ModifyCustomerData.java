package com.mvdb.etl.actions;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.util.RandomUtil;

public class ModifyCustomerData  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(ModifyCustomerData.class);

    public static void main(String[] args)
    {
        
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.InitCustomerData.complete", "300init-customer-data.sh not executed yet. Exiting");
        //This check is not required as data can be modified any number of times
        //ActionUtils.assertFileDoesNotExist("~/.mvdb/status.ModifyCustomerData.complete", "ModifyCustomerData already done. Start with 100init.sh if required. Exiting");
        ActionUtils.setUpInitFileProperty();
        ActionUtils.createMarkerFile("~/.mvdb/status.ModifyCustomerData.start", true);
        
        logger.error("error");
        logger.warn("warning");
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");
        
        ApplicationContext context = Top.getContext();

        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");

        
        long maxId = orderDAO.findMaxId();
        long totalOrders = orderDAO.findTotalOrders();
        
        long modifyCount = (long)(totalOrders * 0.1);
       
        for(int i=0;i<modifyCount;i++)
        {
             long orderId = (long)Math.floor((Math.random() * maxId)) + 1L;
             logger.info("Modify Id " + orderId + " in orders");
             Order theOrder = orderDAO.findByOrderId(orderId);
//             System.out.println("theOrder : " + theOrder);
             theOrder.setNote(RandomUtil.getRandomString(4));
             theOrder.setUpdateTime(new Date());
             theOrder.setSaleCode(RandomUtil.getRandomInt());
             orderDAO.update(theOrder);
//             System.out.println("theOrder Modified: " + theOrder);

        }
        logger.info("Modified " + modifyCount + " orders");
        ActionUtils.createMarkerFile("~/.mvdb/status.ModifyCustomerData.complete", true);
    }
}

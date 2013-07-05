package com.mvdb.etl.consumer;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.mvdb.etl.model.Order;

public class OrderJsonFileConsumer implements Consumer
{

   File file; 
   public OrderJsonFileConsumer(File file)
   {
       this.file = file; 
   }
   
   

    public void consume(Object object)
    {
        Order order = (Order)object;
        try
        {
            FileUtils.writeStringToFile(new File(file, Order.class.getCanonicalName() + ".json"), order.toString() + System.getProperty("line.separator"), true);
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    
    

}

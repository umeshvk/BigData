package com.mvdb.etl.actions;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

//Add a comment to test new branch
public class Top
{

    private static ApplicationContext context = null; 
    
    public static synchronized ApplicationContext getContext()
    {
        if(Top.context != null)
        {
            return Top.context;
        }
        ApplicationContext context = new ClassPathXmlApplicationContext("Spring-Module.xml");
        Top.context = context;
        return Top.context;
    }

}

package com.mvdb.platform.scratch.action;

public class Test
{

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        String test = "orderslineitem-r-00000";
        String[] tokens = test.split("-r-");
        for(String token : tokens)
        {
            System.out.println(token);
        }
    }

}

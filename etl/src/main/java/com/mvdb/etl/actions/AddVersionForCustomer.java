package com.mvdb.etl.actions;

import org.codehaus.jettison.json.JSONException;

/**
 * Uses the four classes InitDB{1}, InitCustomerData{1}, ExtractDBChanges{1}, (ModifyCustomerData, ExtractDBChanges){0,} to create a VersionedCustomer 
 * @author umesh
 *
 */
public class AddVersionForCustomer
{

    /**
     * @param args
     * @throws JSONException 
     * 
     */
    public static void main(String[] args) throws JSONException
    {
        String customerName = "alpha"; 

             
        ActionUtils.setUpInitFileProperty();
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.NOOP, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        
        /**
        ModifyCustomerData.modifyCustomerData(customerName, Action.DELETE, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.UNDELETE, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        **/
        
       
       
    }

}

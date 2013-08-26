package com.mvdb.etl.util.db;

public enum SequenceNames
{
    ORDER_SEQUENCE_NAME("com_mvdb_etl_dao_OrderDAO"),
    ORDER_LINE_ITEM_SEQUENCE_NAME("com_mvdb_etl_dao_OrderLineItemDAO");
    
    String name; 
    private SequenceNames(String name)
    {
        this.name = name; 
    }
    
    public String getName()
    {
        return name;            
    }

}

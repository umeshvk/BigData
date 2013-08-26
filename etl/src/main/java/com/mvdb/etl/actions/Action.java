package com.mvdb.etl.actions;

public enum Action{
    NOOP("Noop"),
    DELETE("Delete"),
    UNDELETE("Undelete");
    
    String name; 
    private Action(String name)
    {
        this.name = name; 
    }
    
    public String getName()
    {
        return name;            
    }
    
}

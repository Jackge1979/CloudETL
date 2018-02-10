package com.cms.framework.model.bigdata;

import java.util.*;

public class HBaseTable
{
    private String tableName;
    private List<HBaseFamilyCloumn> familyCloumns;
    
    public HBaseTable() {
        this.familyCloumns = new ArrayList<HBaseFamilyCloumn>();
    }
    
    public String getTableName() {
        return this.tableName;
    }
    
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }
    
    public List<HBaseFamilyCloumn> getFamilyCloumns() {
        return this.familyCloumns;
    }
    
    public void setFamilyCloumns(final List<HBaseFamilyCloumn> familyCloumns) {
        this.familyCloumns = familyCloumns;
    }
}

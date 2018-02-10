package com.dataliance.hbase.table.vo;

public class Category
{
    private String category;
    private String url;
    private String rowKey;
    
    public String getRowKey() {
        return this.rowKey;
    }
    
    public void setRowKey(final String rowKey) {
        this.rowKey = rowKey;
    }
    
    public String getCategory() {
        return this.category;
    }
    
    public void setCategory(final String category) {
        this.category = category;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(final String url) {
        this.url = url;
    }
}

package com.dataliance.hbase.table.vo;

public class URLVO
{
    public static final int UN_FETCH = 0;
    public static final int FETCHED = 1;
    private int type;
    private String url;
    private String status;
    private String rowKey;
    
    public int getType() {
        return this.type;
    }
    
    public void setType(final int type) {
        this.type = type;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(final String url) {
        this.url = url;
    }
    
    public String getStatus() {
        return this.status;
    }
    
    public void setStatus(final String status) {
        this.status = status;
    }
    
    public String getRowKey() {
        return this.rowKey;
    }
    
    public void setRowKey(final String rowKey) {
        this.rowKey = rowKey;
    }
}

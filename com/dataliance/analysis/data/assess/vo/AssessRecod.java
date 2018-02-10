package com.dataliance.analysis.data.assess.vo;

import com.dataliance.hbase.table.vo.*;

public class AssessRecod
{
    private String rowKey;
    private String category;
    private String id;
    private ContentVO content;
    private String categoryVerify;
    
    public String getRowKey() {
        return this.rowKey;
    }
    
    public void setRowKey(final String rowKey) {
        this.rowKey = rowKey;
    }
    
    public ContentVO getContent() {
        return this.content;
    }
    
    public void setContent(final ContentVO content) {
        this.content = content;
    }
    
    public String getCategory() {
        return this.category;
    }
    
    public void setCategory(final String category) {
        this.category = category;
    }
    
    public String getId() {
        return this.id;
    }
    
    public void setId(final String id) {
        this.id = id;
    }
    
    public String getCategoryVerify() {
        return this.categoryVerify;
    }
    
    public void setCategoryVerify(final String categoryVerify) {
        this.categoryVerify = categoryVerify;
    }
}

package com.dataliance.hbase.table.vo;

public class ContentVO
{
    private String rowKey;
    private String url;
    private String title;
    private String content;
    private String categoryUrl;
    private String categoryText;
    private String categoryVerify;
    private Category category;
    
    public Category getCategory() {
        return this.category;
    }
    
    public void setCategory(final Category category) {
        this.category = category;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(final String url) {
        this.url = url;
    }
    
    public String getTitle() {
        return this.title;
    }
    
    public void setTitle(final String title) {
        this.title = title;
    }
    
    public String getContent() {
        return this.content;
    }
    
    public void setContent(final String content) {
        this.content = content;
    }
    
    public String getCategoryUrl() {
        return this.categoryUrl;
    }
    
    public void setCategoryUrl(final String categoryUrl) {
        this.categoryUrl = categoryUrl;
    }
    
    public String getCategoryText() {
        return this.categoryText;
    }
    
    public void setCategoryText(final String categoryText) {
        this.categoryText = categoryText;
    }
    
    public String getCategoryVerify() {
        return this.categoryVerify;
    }
    
    public void setCategoryVerify(final String categoryVerify) {
        this.categoryVerify = categoryVerify;
    }
    
    public String getRowKey() {
        return this.rowKey;
    }
    
    public void setRowKey(final String rowKey) {
        this.rowKey = rowKey;
    }
}

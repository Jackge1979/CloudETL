package com.dataliance.analysis.data.classifier.bean;

import java.util.*;

public class CriterionUrl
{
    private int id;
    private String url;
    private String pattern;
    private String domain;
    private int categoryId;
    private String categoryName;
    private boolean isDeleted;
    private Date createdDate;
    private int wordSpace;
    
    public CriterionUrl() {
    }
    
    public CriterionUrl(final String url, final String pattern, final int categoryId) {
        this.url = url;
        this.pattern = pattern;
        this.categoryId = categoryId;
    }
    
    public CriterionUrl(final int id, final String url, final String pattern, final int categoryId) {
        this.id = id;
        this.url = url;
        this.pattern = pattern;
        this.categoryId = categoryId;
    }
    
    public int getId() {
        return this.id;
    }
    
    public void setId(final int id) {
        this.id = id;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(final String url) {
        this.url = url;
    }
    
    public String getDomain() {
        return this.domain;
    }
    
    public void setDomain(final String domain) {
        this.domain = domain;
    }
    
    public int getCategoryId() {
        return this.categoryId;
    }
    
    public void setCategoryId(final int categoryId) {
        this.categoryId = categoryId;
    }
    
    public String getCategoryName() {
        return this.categoryName;
    }
    
    public void setCategoryName(final String categoryName) {
        this.categoryName = categoryName;
    }
    
    @Override
    public String toString() {
        return String.format("categoryId:%s, categoryName:%s, url:%s, pattern:%s, domain:%s", this.categoryId, this.categoryName, this.url, this.pattern, this.domain);
    }
    
    public boolean isDeleted() {
        return this.isDeleted;
    }
    
    public void setDeleted(final boolean isDeleted) {
        this.isDeleted = isDeleted;
    }
    
    public Date getCreatedDate() {
        return this.createdDate;
    }
    
    public void setCreatedDate(final Date createdDate) {
        this.createdDate = createdDate;
    }
    
    public String getPattern() {
        return this.pattern;
    }
    
    public void setPattern(final String pattern) {
        this.pattern = pattern;
    }
    
    public int getWordSpace() {
        return this.wordSpace;
    }
    
    public void setWordSpace(final int wordSpace) {
        this.wordSpace = wordSpace;
    }
}

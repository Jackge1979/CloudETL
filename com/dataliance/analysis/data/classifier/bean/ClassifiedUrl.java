package com.dataliance.analysis.data.classifier.bean;

import org.apache.hadoop.io.*;
import java.io.*;

public class ClassifiedUrl implements Writable
{
    private String url;
    private int categoryId;
    
    public ClassifiedUrl() {
        this.categoryId = -1;
    }
    
    public ClassifiedUrl(final String url, final int categoryId) {
        this.categoryId = -1;
        this.url = url;
        this.categoryId = categoryId;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(final String url) {
        this.url = url;
    }
    
    public int getCategoryId() {
        return this.categoryId;
    }
    
    public void setCategoryId(final int categoryId) {
        this.categoryId = categoryId;
    }
    
    public void readFields(final DataInput in) throws IOException {
        Text.readString(in);
        this.categoryId = in.readInt();
    }
    
    public void write(final DataOutput out) throws IOException {
        Text.writeString(out, this.url);
        out.writeByte(this.categoryId);
    }
    
    @Override
    public String toString() {
        return String.format("%s,%s", this.categoryId, this.url);
    }
}

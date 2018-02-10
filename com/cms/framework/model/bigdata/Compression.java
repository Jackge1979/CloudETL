package com.cms.framework.model.bigdata;

import com.cms.framework.model.*;

public class Compression extends PersistentObject
{
    private String input;
    private String output;
    private String format;
    private boolean retainOriginalFile;
    private int status;
    
    public String getInput() {
        return this.input;
    }
    
    public void setInput(final String input) {
        this.input = input;
    }
    
    public String getOutput() {
        return this.output;
    }
    
    public void setOutput(final String output) {
        this.output = output;
    }
    
    public String getFormat() {
        return this.format;
    }
    
    public void setFormat(final String format) {
        this.format = format;
    }
    
    public boolean isRetainOriginalFile() {
        return this.retainOriginalFile;
    }
    
    public void setRetainOriginalFile(final boolean retainOriginalFile) {
        this.retainOriginalFile = retainOriginalFile;
    }
    
    public int getStatus() {
        return this.status;
    }
    
    public void setStatus(final int status) {
        this.status = status;
    }
}

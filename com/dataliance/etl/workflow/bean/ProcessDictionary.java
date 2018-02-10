package com.dataliance.etl.workflow.bean;

import java.io.*;
import java.util.*;

public class ProcessDictionary implements Serializable
{
    private static final long serialVersionUID = 1L;
    private long id;
    private String name;
    private String regionId;
    private String workDir;
    private int splitLen;
    private String parseSplitSymbol;
    private String destSplitSymbol;
    private String description;
    private List<ProcessField> processFields;
    
    public ProcessDictionary() {
        this.processFields = null;
        this.processFields = new ArrayList<ProcessField>();
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public String getWorkDir() {
        return this.workDir;
    }
    
    public void setWorkDir(final String workDir) {
        this.workDir = workDir;
    }
    
    public int getSplitLen() {
        return this.splitLen;
    }
    
    public void setSplitLen(final int splitLen) {
        this.splitLen = splitLen;
    }
    
    public String getParseSplitSymbol() {
        return this.parseSplitSymbol;
    }
    
    public void setParseSplitSymbol(final String parseSplitSymbol) {
        this.parseSplitSymbol = parseSplitSymbol;
    }
    
    public String getDestSplitSymbol() {
        return this.destSplitSymbol;
    }
    
    public void setDestSplitSymbol(final String destSplitSymbol) {
        this.destSplitSymbol = destSplitSymbol;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String description) {
        this.description = description;
    }
    
    public long getId() {
        return this.id;
    }
    
    public void setId(final long id) {
        this.id = id;
    }
    
    public String getRegionId() {
        return this.regionId;
    }
    
    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }
    
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        for (final ProcessField field : this.processFields) {
            buffer.append(field).append("\n");
        }
        return String.format("{id:%s ,name:%s , workDir:%s ,regionId:%s ,parseSplitSymbol:%s ,destSplitSymbol:%s ,description:%s}\n fields: \n %s", this.id, this.name, this.workDir, this.regionId, this.parseSplitSymbol, this.destSplitSymbol, this.description, buffer.toString());
    }
    
    public List<ProcessField> getProcessFields() {
        return this.processFields;
    }
    
    public void setProcessFields(final List<ProcessField> processFields) {
        this.processFields = processFields;
    }
    
    public void addProcessField(final ProcessField processField) {
        this.processFields.add(processField);
    }
}

package com.dataliance.etl.workflow.bean;

import java.io.*;

public class ProcessField implements Serializable, Cloneable
{
    private static final long serialVersionUID = 1L;
    private long id;
    private long dictionaryId;
    private String name;
    private String regularExpressions;
    private String parseRule;
    private int parseOrder;
    private String destRule;
    private int destOrder;
    private Boolean enable;
    private Boolean needVerify;
    private Boolean needTransfer;
    
    public ProcessField() {
        this.enable = Boolean.TRUE;
        this.needVerify = Boolean.FALSE;
        this.needTransfer = Boolean.FALSE;
    }
    
    public long getId() {
        return this.id;
    }
    
    public void setId(final long id) {
        this.id = id;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public String getRegularExpressions() {
        return this.regularExpressions;
    }
    
    public void setRegularExpressions(final String regularExpressions) {
        this.regularExpressions = regularExpressions;
    }
    
    public String getParseRule() {
        return this.parseRule;
    }
    
    public void setParseRule(final String parseRule) {
        this.parseRule = parseRule;
    }
    
    public int getParseOrder() {
        return this.parseOrder;
    }
    
    public void setParseOrder(final int parseOrder) {
        this.parseOrder = parseOrder;
    }
    
    public String getDestRule() {
        return this.destRule;
    }
    
    public void setDestRule(final String destRule) {
        this.destRule = destRule;
    }
    
    public int getDestOrder() {
        return this.destOrder;
    }
    
    public void setDestOrder(final int destOrder) {
        this.destOrder = destOrder;
    }
    
    public Boolean getEnable() {
        return this.enable;
    }
    
    public void setEnable(final Boolean enable) {
        this.enable = enable;
    }
    
    public Boolean getNeedVerify() {
        return this.needVerify;
    }
    
    public void setNeedVerify(final Boolean needVerify) {
        this.needVerify = needVerify;
    }
    
    public Boolean getNeedTransfer() {
        return this.needTransfer;
    }
    
    public void setNeedTransfer(final Boolean needTransfer) {
        this.needTransfer = needTransfer;
    }
    
    public long getDictionaryId() {
        return this.dictionaryId;
    }
    
    public void setDictionaryId(final long dictionaryId) {
        this.dictionaryId = dictionaryId;
    }
    
    @Override
    public String toString() {
        return String.format("{id:%s ,name:%s ,enable:%s ,,regularExpressions:%s,parseRule:%s,parseOrder:%s,destOrder:%s,destRule:%s,needVerify:%s,needTransfer:%s}", this.id, this.name, this.enable, this.regularExpressions, this.parseRule, this.parseOrder, this.destOrder, this.destRule, this.needVerify, this.needTransfer);
    }
    
    public ProcessField clone() {
        ProcessField o = null;
        try {
            o = (ProcessField)super.clone();
        }
        catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }
}

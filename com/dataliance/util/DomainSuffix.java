package com.dataliance.util;

public class DomainSuffix
{
    private String domain;
    private Status status;
    private float boost;
    public static final float DEFAULT_BOOST = 1.0f;
    public static final Status DEFAULT_STATUS;
    
    public DomainSuffix(final String domain, final Status status, final float boost) {
        this.domain = domain;
        this.status = status;
        this.boost = boost;
    }
    
    public DomainSuffix(final String domain) {
        this(domain, DomainSuffix.DEFAULT_STATUS, 1.0f);
    }
    
    public String getDomain() {
        return this.domain;
    }
    
    public Status getStatus() {
        return this.status;
    }
    
    public float getBoost() {
        return this.boost;
    }
    
    @Override
    public String toString() {
        return this.domain;
    }
    
    static {
        DEFAULT_STATUS = Status.IN_USE;
    }
    
    public enum Status
    {
        INFRASTRUCTURE, 
        SPONSORED, 
        UNSPONSORED, 
        STARTUP, 
        PROPOSED, 
        DELETED, 
        PSEUDO_DOMAIN, 
        DEPRECATED, 
        IN_USE, 
        NOT_IN_USE, 
        REJECTED;
    }
}

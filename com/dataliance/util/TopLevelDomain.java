package com.dataliance.util;

public class TopLevelDomain extends DomainSuffix
{
    private Type type;
    private String countryName;
    
    public TopLevelDomain(final String domain, final Type type, final Status status, final float boost) {
        super(domain, status, boost);
        this.countryName = null;
        this.type = type;
    }
    
    public TopLevelDomain(final String domain, final Status status, final float boost, final String countryName) {
        super(domain, status, boost);
        this.countryName = null;
        this.type = Type.COUNTRY;
        this.countryName = countryName;
    }
    
    public Type getType() {
        return this.type;
    }
    
    public String getCountryName() {
        return this.countryName;
    }
    
    public enum Type
    {
        INFRASTRUCTURE, 
        GENERIC, 
        COUNTRY;
    }
}

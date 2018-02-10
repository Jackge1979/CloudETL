package com.cms.framework.model.bigdata;

public class HBaseFamilyCloumn
{
    private String familyName;
    private String bloomFilter;
    private String compression;
    private boolean inMemory;
    private int version;
    
    public String getFamilyName() {
        return this.familyName;
    }
    
    public void setFamilyName(final String familyName) {
        this.familyName = familyName;
    }
    
    public String getBloomFilter() {
        return this.bloomFilter;
    }
    
    public void setBloomFilter(final String bloomFilter) {
        this.bloomFilter = bloomFilter;
    }
    
    public String getCompression() {
        return this.compression;
    }
    
    public void setCompression(final String compression) {
        this.compression = compression;
    }
    
    public boolean isInMemory() {
        return this.inMemory;
    }
    
    public void setInMemory(final boolean inMemory) {
        this.inMemory = inMemory;
    }
    
    public int getVersion() {
        return this.version;
    }
    
    public void setVersion(final int version) {
        this.version = version;
    }
}

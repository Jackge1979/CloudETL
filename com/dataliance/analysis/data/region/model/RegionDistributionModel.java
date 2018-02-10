package com.dataliance.analysis.data.region.model;

public class RegionDistributionModel
{
    private String region;
    private String date;
    private int num;
    
    public String getRegion() {
        return this.region;
    }
    
    public void setRegion(final String region) {
        this.region = region;
    }
    
    public String getDate() {
        return this.date;
    }
    
    public void setDate(final String date) {
        this.date = date;
    }
    
    public int getNum() {
        return this.num;
    }
    
    public void setNum(final int num) {
        this.num = num;
    }
    
    @Override
    public String toString() {
        return "RegionDistributionModel [region=" + this.region + ", date=" + this.date + ", num=" + this.num + "]";
    }
}

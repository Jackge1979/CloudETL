package com.dataliance.analysis.data.uv.model;

public class UVModel
{
    private String region;
    private int hour;
    private int num;
    
    public String getRegion() {
        return this.region;
    }
    
    public void setRegion(final String region) {
        this.region = region;
    }
    
    public int getHour() {
        return this.hour;
    }
    
    public void setHour(final int hour) {
        this.hour = hour;
    }
    
    public int getNum() {
        return this.num;
    }
    
    public void setNum(final int num) {
        this.num = num;
    }
    
    @Override
    public String toString() {
        return "UVModel [region=" + this.region + ", hour=" + this.hour + ", num=" + this.num + "]";
    }
}

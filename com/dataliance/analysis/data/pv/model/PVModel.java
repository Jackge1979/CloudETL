package com.dataliance.analysis.data.pv.model;

public class PVModel
{
    private String region;
    private int hour;
    private int num;
    private int unum;
    private int activity;
    
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
    
    public int getUnum() {
        return this.unum;
    }
    
    public void setUnum(final int unum) {
        this.unum = unum;
    }
    
    public int getActivity() {
        return this.activity;
    }
    
    public void setActivity(final int activity) {
        this.activity = activity;
    }
    
    @Override
    public String toString() {
        return "PVModel [region=" + this.region + ", hour=" + this.hour + ", num=" + this.num + ", unum=" + this.unum + ", activity=" + this.activity + "]";
    }
}

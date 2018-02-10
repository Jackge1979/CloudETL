package com.dataliance.analysis.data.recommend;

public class RecommendUserInfo
{
    private String phoneNumber;
    private long freq;
    private long flow;
    private String style;
    private String ratType;
    
    public RecommendUserInfo() {
        this.phoneNumber = null;
        this.freq = 0L;
        this.flow = 0L;
        this.style = null;
        this.ratType = null;
    }
    
    public String getPhoneNumber() {
        return this.phoneNumber;
    }
    
    public void setPhoneNumber(final String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
    
    public long getFreq() {
        return this.freq;
    }
    
    public void setFreq(final long freq) {
        this.freq = freq;
    }
    
    public long getFlow() {
        return this.flow;
    }
    
    public void setFlow(final long flow) {
        this.flow = flow;
    }
    
    public String getStyle() {
        return this.style;
    }
    
    public void setStyle(final String style) {
        this.style = style;
    }
    
    public String getRatType() {
        return this.ratType;
    }
    
    public void setRatType(final String ratType) {
        this.ratType = ratType;
    }
    
    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s", this.phoneNumber, this.freq, this.flow, this.ratType, this.style);
    }
}

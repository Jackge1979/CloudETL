package com.dataliance.analysis.data.flow.model;

public class FlowModel
{
    private String region;
    private Long flow;
    private String date;
    
    public String getRegion() {
        return this.region;
    }
    
    public void setRegion(final String region) {
        this.region = region;
    }
    
    public Long getFlow() {
        return this.flow;
    }
    
    public void setFlow(final Long flow) {
        this.flow = flow;
    }
    
    public String getDate() {
        return this.date;
    }
    
    public void setDate(final String date) {
        this.date = date;
    }
    
    @Override
    public String toString() {
        return "FlowModel [region=" + this.region + ", flow=" + this.flow + ", date=" + this.date + "]";
    }
}

package com.dataliance.analysis.data.recommend;

import org.apache.hadoop.io.*;
import java.io.*;

public class UserFeature implements Writable
{
    private static byte VERSION;
    private long flow;
    private String style;
    private String phoneNum;
    private String ratType;
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(UserFeature.VERSION);
        out.writeLong(this.flow);
        Text.writeString(out, this.getSrc(this.style));
        Text.writeString(out, this.getSrc(this.phoneNum));
        Text.writeString(out, this.getSrc(this.ratType));
    }
    
    public void readFields(final DataInput in) throws IOException {
        final int v = in.readByte();
        if (v == UserFeature.VERSION) {
            this.flow = in.readLong();
            this.style = Text.readString(in);
            this.phoneNum = Text.readString(in);
            this.ratType = Text.readString(in);
            return;
        }
        throw new IOException("Version is not matches!");
    }
    
    private String getSrc(final String src) {
        return (src != null) ? src : "";
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
    
    public String getPhoneNum() {
        return this.phoneNum;
    }
    
    public void setPhoneNum(final String phoneNum) {
        this.phoneNum = phoneNum;
    }
    
    public String getRatType() {
        return this.ratType;
    }
    
    public void setRatType(final String ratType) {
        this.ratType = ratType;
    }
    
    @Override
    public String toString() {
        return String.format("phoneNum:%s, style:%s, ratType:%s, flow:%s", this.phoneNum, this.style, this.ratType, this.flow);
    }
    
    static {
        UserFeature.VERSION = 1;
    }
}

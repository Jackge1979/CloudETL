package com.dataliance.log.url.vo;

import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class AllDayVO implements Writable
{
    private Map<String, Long> dayInfos;
    
    public AllDayVO() {
        this.dayInfos = new HashMap<String, Long>();
    }
    
    public void add(final String day, final long num) {
        this.dayInfos.put(day, num);
    }
    
    public Map<String, Long> getDayInfos() {
        return this.dayInfos;
    }
    
    public void addAll(final Map<String, Long> days) {
        this.dayInfos.putAll(days);
    }
    
    public int getSize() {
        return this.dayInfos.size();
    }
    
    public void init() {
        this.dayInfos.clear();
    }
    
    public void write(final DataOutput out) throws IOException {
        final int num = this.dayInfos.size();
        WritableUtils.writeVInt(out, num);
        for (final Map.Entry<String, Long> entry : this.dayInfos.entrySet()) {
            WritableUtils.writeString(out, (String)entry.getKey());
            WritableUtils.writeVLong(out, (long)entry.getValue());
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        for (int num = WritableUtils.readVInt(in), i = 0; i < num; ++i) {
            final String key = WritableUtils.readString(in);
            final long value = WritableUtils.readVLong(in);
            this.dayInfos.put(key, value);
        }
    }
    
    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        for (final Map.Entry<String, Long> entry : this.dayInfos.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append("|");
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - "|".length());
        }
        return sb.toString();
    }
}

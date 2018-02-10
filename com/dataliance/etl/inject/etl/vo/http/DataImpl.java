package com.dataliance.etl.inject.etl.vo.http;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.etl.vo.*;

import java.util.*;
import java.io.*;

public class DataImpl implements Data
{
    private String src;
    private String destDir;
    private long length;
    private boolean dir;
    private List<Data> children;
    private byte status;
    private Set<String> parseHosts;
    private String datahost;
    
    public DataImpl() {
        this.children = new ArrayList<Data>();
        this.parseHosts = new HashSet<String>();
    }
    
    @Override
    public String getSrc() {
        return this.src;
    }
    
    public void setSrc(final String src) {
        this.src = src;
    }
    
    @Override
    public boolean isDir() {
        return this.dir;
    }
    
    public void setDir(final boolean dir) {
        this.dir = dir;
    }
    
    @Override
    public List<Data> getChildren() {
        return this.children;
    }
    
    public void setChildren(final List<Data> children) {
        this.children = children;
    }
    
    @Override
    public long length() {
        return this.length;
    }
    
    public void setLength(final long length) {
        this.length = length;
    }
    
    @Override
    public String getDestDir() {
        return this.destDir;
    }
    
    public void setDestDir(final String destDir) {
        this.destDir = destDir;
    }
    
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.datahost);
        WritableUtils.writeString(out, this.src);
        WritableUtils.writeString(out, this.destDir);
        WritableUtils.writeVLong(out, this.length);
        out.writeBoolean(this.dir);
        WritableUtils.writeVInt(out, this.children.size());
        for (final Data child : this.children) {
            child.write(out);
        }
        WritableUtils.writeVInt(out, this.parseHosts.size());
        for (final String host : this.parseHosts) {
            WritableUtils.writeString(out, host);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.datahost = WritableUtils.readString(in);
        this.src = WritableUtils.readString(in);
        this.destDir = WritableUtils.readString(in);
        this.length = WritableUtils.readVLong(in);
        this.dir = in.readBoolean();
        for (int size = WritableUtils.readVInt(in), i = 0; i < size; ++i) {
            final Data child = new DataImpl();
            child.readFields(in);
            this.children.add(child);
        }
        for (int size = WritableUtils.readVInt(in), i = 0; i < size; ++i) {
            this.addParseHost(WritableUtils.readString(in));
        }
    }
    
    @Override
    public byte getStatus() {
        return this.status;
    }
    
    @Override
    public void setStatus(final byte status) {
        this.status = status;
    }
    
    @Override
    public Set<String> getParseHosts() {
        return this.parseHosts;
    }
    
    @Override
    public void addParseHost(final String host) {
        this.parseHosts.add(host);
    }
    
    @Override
    public String getDataHost() {
        return this.datahost;
    }
    
    @Override
    public void setDatahost(final String host) {
        this.datahost = host;
    }
    
    @Override
    public String toString() {
        return "src = " + this.src + " dest = " + this.destDir;
    }
}

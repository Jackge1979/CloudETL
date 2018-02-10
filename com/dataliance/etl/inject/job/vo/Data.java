package com.dataliance.etl.inject.job.vo;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hbase.util.*;

public class Data implements Writable
{
    private static final byte VERSION = 1;
    private String path;
    private String dataHost;
    private long datasize;
    private byte status;
    private List<String> importHosts;
    
    public Data() {
        this.importHosts = new ArrayList<String>();
    }
    
    public boolean isWaiting() {
        return this.status == 0;
    }
    
    public boolean isRunning() {
        return this.status == 1;
    }
    
    public boolean isFinish() {
        return this.status == 2;
    }
    
    public boolean isError() {
        return this.status == -1;
    }
    
    public String getStatusStr() {
        switch (this.status) {
            case -1: {
                return "error";
            }
            case 2: {
                return "finish";
            }
            case 1: {
                return "running";
            }
            case 0: {
                return "waiting";
            }
            default: {
                return "unknow";
            }
        }
    }
    
    public String getPath() {
        return this.path;
    }
    
    public void setPath(final String path) {
        this.path = path;
    }
    
    public String getDataHost() {
        return this.dataHost;
    }
    
    public void setDataHost(final String dataHost) {
        this.dataHost = dataHost;
    }
    
    public long getDataSize() {
        return this.datasize;
    }
    
    public void setDataSize(final long size) {
        this.datasize = size;
    }
    
    public byte getStatus() {
        return this.status;
    }
    
    public void setStatus(final byte status) {
        this.status = status;
    }
    
    public List<String> getImportHosts() {
        return this.importHosts;
    }
    
    public void addImportHost(final String host) {
        this.importHosts.add(host);
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        WritableUtils.writeString(out, this.path);
        WritableUtils.writeString(out, this.dataHost);
        WritableUtils.writeVLong(out, this.datasize);
        out.writeByte(this.status);
        out.writeInt(this.importHosts.size());
        for (final String host : this.importHosts) {
            WritableUtils.writeString(out, host);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == 1) {
            this.path = WritableUtils.readString(in);
            this.dataHost = WritableUtils.readString(in);
            this.datasize = WritableUtils.readVLong(in);
            this.status = in.readByte();
            for (int n = in.readInt(), i = 0; i < n; ++i) {
                this.addImportHost(WritableUtils.readString(in));
            }
            return;
        }
        throw new VersionException();
    }
    
    public static Data read(final DataInput in) throws IOException {
        final Data data = new Data();
        data.readFields(in);
        return data;
    }
    
    public static void main(final String[] args) {
        try {
            final byte[][] splits = Bytes.split(Bytes.toBytes("0|http://0"), Bytes.toBytes("0|http://~~~~~~~~~~~~~"), 90);
            System.out.println("............" + splits);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

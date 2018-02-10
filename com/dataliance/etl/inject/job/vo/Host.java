package com.dataliance.etl.inject.job.vo;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;

import java.util.*;
import java.io.*;

public class Host implements Writable
{
    private static final byte VERSION = 1;
    public static final byte SERVER_DATA = 0;
    public static final byte SERVER_IMPORT = 1;
    public static final byte SERVER_OUTPUT = 2;
    public static final byte SERVER_MANAGER = -1;
    private String host;
    private int port;
    private byte hostType;
    private List<Data> datas;
    
    public Host() {
        this.datas = new ArrayList<Data>();
    }
    
    public boolean isDataServer() {
        return this.hostType == 0;
    }
    
    public boolean isImportServer() {
        return this.hostType == 1;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public void setPort(final int port) {
        this.port = port;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
    
    public byte getHostType() {
        return this.hostType;
    }
    
    public void setHostType(final byte hostType) {
        this.hostType = hostType;
    }
    
    public List<Data> getDatas() {
        return this.datas;
    }
    
    public void addData(final Data data) {
        this.datas.add(data);
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        WritableUtils.writeString(out, this.host);
        WritableUtils.writeVInt(out, this.port);
        out.writeByte(this.hostType);
        out.writeInt(this.datas.size());
        for (final Data data : this.datas) {
            data.write(out);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == 1) {
            this.host = WritableUtils.readString(in);
            this.port = WritableUtils.readVInt(in);
            this.hostType = in.readByte();
            for (int size = in.readInt(), i = 0; i < size; ++i) {
                this.addData(Data.read(in));
            }
            return;
        }
        throw new VersionException();
    }
    
    public static Host read(final DataInput in) throws IOException {
        final Host host = new Host();
        host.readFields(in);
        return host;
    }
}

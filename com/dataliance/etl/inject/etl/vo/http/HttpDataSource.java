package com.dataliance.etl.inject.etl.vo.http;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.etl.vo.*;

import java.util.*;
import java.io.*;

public class HttpDataSource extends Host implements DataSource
{
    private int port;
    private String protocol;
    private Map<String, Data> datas;
    
    public HttpDataSource() {
        this.datas = new HashMap<String, Data>();
    }
    
    @Override
    public int getPort() {
        return this.port;
    }
    
    @Override
    public String getProtocol() {
        return this.protocol;
    }
    
    @Override
    public Map<String, Data> getDatas() {
        return this.datas;
    }
    
    @Override
    public void setPort(final int port) {
        this.port = port;
    }
    
    public void setProtocol(final String protocol) {
        this.protocol = protocol;
    }
    
    @Override
    public void addData(final Data data) {
        this.datas.put(data.getSrc(), data);
    }
    
    public Data getData(final String path) {
        return this.datas.get(path);
    }
    
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.getHost());
        WritableUtils.writeString(out, this.getPassword());
        WritableUtils.writeString(out, this.getUser());
        WritableUtils.writeString(out, this.protocol);
        out.writeInt(this.port);
        out.writeInt(this.datas.size());
        for (final Data data : this.datas.values()) {
            data.write(out);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.setHost(WritableUtils.readString(in));
        this.setPassword(WritableUtils.readString(in));
        this.setUser(WritableUtils.readString(in));
        this.protocol = WritableUtils.readString(in);
        this.port = in.readInt();
        for (int size = in.readInt(), i = 0; i < size; ++i) {
            final Data data = new DataImpl();
            data.readFields(in);
            this.addData(data);
        }
    }
    
    @Override
    public String toString() {
        return this.datas.toString();
    }
}

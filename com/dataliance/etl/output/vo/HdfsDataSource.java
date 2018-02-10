package com.dataliance.etl.output.vo;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.etl.vo.http.*;

import java.util.*;
import java.io.*;

public class HdfsDataSource extends Host implements DataSource
{
    public static final String PROTOCOL = "hdfs";
    private Map<String, Data> datas;
    
    public HdfsDataSource() {
        this.datas = new HashMap<String, Data>();
    }
    
    @Override
    public String getProtocol() {
        return "hdfs";
    }
    
    @Override
    public Map<String, Data> getDatas() {
        return this.datas;
    }
    
    @Override
    public void addData(final Data data) {
        this.datas.put(data.getSrc(), data);
    }
    
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.getHost());
        WritableUtils.writeString(out, this.getPassword());
        WritableUtils.writeString(out, this.getUser());
        out.writeInt(this.datas.size());
        for (final Data data : this.datas.values()) {
            data.write(out);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.setHost(WritableUtils.readString(in));
        this.setPassword(WritableUtils.readString(in));
        this.setUser(WritableUtils.readString(in));
        for (int size = in.readInt(), i = 0; i < size; ++i) {
            final Data data = new DataImpl();
            data.readFields(in);
            this.addData(data);
        }
    }
    
    @Override
    public String toString() {
        return "DataSource PROTOCOL = hdfs";
    }
}

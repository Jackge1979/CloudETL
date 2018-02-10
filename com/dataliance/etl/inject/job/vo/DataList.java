package com.dataliance.etl.inject.job.vo;

import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class DataList implements Writable
{
    private List<Data> datas;
    
    public DataList() {
        this.datas = new ArrayList<Data>();
    }
    
    public List<Data> getDatas() {
        return this.datas;
    }
    
    public void setDatas(final List<Data> datas) {
        this.datas = datas;
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.datas.size());
        for (final Data data : this.datas) {
            data.write(out);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        for (int size = in.readInt(), i = 0; i < size; ++i) {
            this.datas.add(Data.read(in));
        }
    }
}

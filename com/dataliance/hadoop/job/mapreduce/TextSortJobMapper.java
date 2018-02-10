package com.dataliance.hadoop.job.mapreduce;

import org.apache.hadoop.mapreduce.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;

import java.util.regex.*;
import java.io.*;

public class TextSortJobMapper extends Mapper<WritableComparable<?>, Text, DescLongWritable, Text>
{
    private static final String SPLIT = "\t|\\s";
    private static Pattern pattern;
    private DescLongWritable lastKey;
    private Text lastValue;
    
    public TextSortJobMapper() {
        this.lastKey = new DescLongWritable(0L);
        this.lastValue = new Text();
    }
    
    protected void map(final WritableComparable<?> key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        final String[] vs = TextSortJobMapper.pattern.split(value.toString());
        if (vs.length == 2) {
            this.lastKey.set(Long.parseLong(vs[0]));
            this.lastValue.set(vs[1]);
            context.write((Object)this.lastKey, (Object)this.lastValue);
        }
    }
    
    public static void main(final String[] args) {
        final String[] vs = TextSortJobMapper.pattern.split("75499\thttp://10.0.0.172/cgi-bin/mail_list?hittype=0&sid=_MriDmNCMgUcbmU12gR1dFXi%2C5%2Czg8E7iDzB&s=unread&folderid=1&flag=new&page=0&pagesize=10&ftype=&t=mail_list");
        System.out.println(vs.length);
        System.out.println(vs[0]);
        System.out.println(vs[1]);
    }
    
    static {
        TextSortJobMapper.pattern = Pattern.compile("\t|\\s");
    }
}

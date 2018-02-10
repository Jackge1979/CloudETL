package com.dataliance.log.url.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import com.dataliance.hadoop.vo.*;
import com.dataliance.log.url.vo.*;
import java.util.regex.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class AllDayCompareMapper extends Mapper<FileKey, Text, Text, AllDayVO>
{
    private String split;
    private Pattern pattern;
    private Text lastKey;
    private AllDayVO lastValue;
    
    public AllDayCompareMapper() {
        this.split = "\t";
        this.pattern = Pattern.compile(this.split);
        this.lastKey = new Text();
        this.lastValue = new AllDayVO();
    }
    
    protected void map(final FileKey key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        this.lastValue.init();
        final Path path = new Path(key.getPath());
        final String day = path.getParent().getName();
        final String[] vs = this.pattern.split(value.toString());
        final long num = Long.parseLong(vs[0]);
        final String url = vs[1];
        this.lastKey.set(url);
        this.lastValue.add(day, num);
        context.write((Object)this.lastKey, (Object)this.lastValue);
    }
}

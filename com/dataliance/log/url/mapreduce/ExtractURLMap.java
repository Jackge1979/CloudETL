package com.dataliance.log.url.mapreduce;

import org.apache.hadoop.mapreduce.*;

import com.dataliance.util.*;

import org.apache.hadoop.io.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;

public class ExtractURLMap extends Mapper<WritableComparable<?>, Text, Text, LongWritable>
{
    private LongWritable COUNT_1;
    private String split;
    private Pattern pattern;
    private Text lastKey;
    
    public ExtractURLMap() {
        this.COUNT_1 = new LongWritable(1L);
        this.lastKey = new Text();
    }
    
    protected void map(final WritableComparable<?> key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        try {
            final String url = this.getURL(value.toString());
            if (url != null) {
                this.lastKey.set(url);
                context.write((Object)this.lastKey, (Object)this.COUNT_1);
            }
        }
        catch (Exception e) {
            context.setStatus(value.toString() + " is error!!!");
        }
    }
    
    private String getURL(final String value) throws MalformedURLException {
        final String[] urls = this.pattern.split(value);
        if (urls.length > 0) {
            for (final String url : urls) {
                if (url.startsWith("http")) {
                    final String host = URLUtil.getHost(url);
                    if (host != null && URLUtil.isIpOrHost(host)) {
                        return url;
                    }
                }
            }
        }
        return null;
    }
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        this.split = context.getConfiguration().get("com.DA.log.split", "@#\\$");
        this.pattern = Pattern.compile(this.split);
    }
}

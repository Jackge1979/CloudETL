package com.dataliance.analysis.data.compress;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.mapreduce.*;
import com.dataliance.hadoop.mapreduce.unsplit.*;
import com.dataliance.hadoop.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.mapreduce.*;

public class CopyOfCompressConvert extends AbstractTool
{
    private Path in;
    private Path out;
    private String format;
    private boolean retainOriginalFile;
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = DAConfigUtil.create();
        ToolRunner.run(conf, (Tool)new CopyOfCompressConvert(), args);
    }
    
    public CopyOfCompressConvert() {
    }
    
    public CopyOfCompressConvert(final Path in, final Path out) {
        this.in = in;
        this.out = out;
    }
    
    public String getFormat() {
        return this.format;
    }
    
    public void setFormat(final String format) {
        this.format = format;
    }
    
    public boolean isRetainOriginalFile() {
        return this.retainOriginalFile;
    }
    
    public void setRetainOriginalFile(final boolean retainOriginalFile) {
        this.retainOriginalFile = retainOriginalFile;
    }
    
    public Path getIn() {
        return this.in;
    }
    
    public void setIn(final Path in) {
        this.in = in;
    }
    
    public Path getOut() {
        return this.out;
    }
    
    public void setOut(final Path out) {
        this.out = out;
    }
    
    @Override
    protected int doAction(final String[] args) throws Exception {
        try {
            this.doAction();
            return 0;
        }
        catch (Exception e) {
            return -1;
        }
    }
    
    protected void doAction() throws Exception {
        final Configuration conf = this.getJobConf();
        if (this.format == null || this.format.equalsIgnoreCase("none")) {
            conf.set("mapred.output.compress", "false");
        }
        else {
            conf.set("mapred.output.compress", "true");
            if (this.format.equalsIgnoreCase("snappy")) {
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            }
            else if (this.format.equalsIgnoreCase("gz")) {
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
            }
            else if (this.format.equalsIgnoreCase("lzo")) {
                conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
            }
            else if (this.format.equalsIgnoreCase("default")) {
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
            }
        }
        this.addInputPath(this.in);
        this.setOutputPath(this.out);
        this.setOutputKeyClass(FileKey.class);
        this.setOutputValueClass(Text.class);
        this.setMapperClass(BasicMapper.class);
        this.setInputFormatClass((Class<? extends InputFormat>)UnSplitInputFormat.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)FileNameOutputFormat.class);
        this.setNumReduceTasks(0);
        this.runJob(true);
    }
}

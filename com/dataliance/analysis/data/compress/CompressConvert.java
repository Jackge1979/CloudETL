package com.dataliance.analysis.data.compress;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.text.*;

import org.apache.hadoop.io.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.mapreduce.*;
import com.dataliance.hadoop.mapreduce.unsplit.*;
import com.dataliance.hadoop.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.mapreduce.*;
import java.net.*;
import java.io.*;

public class CompressConvert extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = DAConfigUtil.create();
        ToolRunner.run(conf, (Tool)new CompressConvert(), args);
    }
    
    @Override
    protected int doAction(final String[] args) throws Exception {
        final String className = this.clazz.getSimpleName();
        final String useAge = "Usage: " + className + " <in> <out> <compress> <id>";
        if (args.length < 4) {
            System.err.println(useAge);
            return -1;
        }
        final Path in = new Path(args[0]);
        final Path out = new Path(args[1]);
        final String compress = args[2];
        final String id = args[3];
        if (this.job == null) {
            this.initJob(className + "-" + out);
        }
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        CompressConvert.LOG.info("Add in path : " + in);
        CompressConvert.LOG.info("Set out path : " + out);
        final long start = System.currentTimeMillis();
        CompressConvert.LOG.info(className + " : starting at " + sdf.format(start));
        this.doAction(in, out, compress, id);
        final long end = System.currentTimeMillis();
        CompressConvert.LOG.info(className + " : finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
        return 0;
    }
    
    protected void doAction(final Path in, final Path out, final String format, final String id) throws Exception {
        final Configuration conf = this.getJobConf();
        if (format == null || format.equalsIgnoreCase("none")) {
            conf.set("mapred.output.compress", "false");
        }
        else {
            conf.set("mapred.output.compress", "true");
            if (format.equalsIgnoreCase("snappy")) {
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            }
            else if (format.equalsIgnoreCase("gz")) {
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
            }
            else if (format.equalsIgnoreCase("lzo")) {
                conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
            }
            else if (format.equalsIgnoreCase("default")) {
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
            }
        }
        this.addInputPath(in);
        this.setOutputPath(out);
        this.setOutputKeyClass(FileKey.class);
        this.setOutputValueClass(Text.class);
        this.setMapperClass(BasicMapper.class);
        this.setInputFormatClass((Class<? extends InputFormat>)UnSplitInputFormat.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)FileNameOutputFormat.class);
        this.setNumReduceTasks(0);
        final String servie = this.getConf().get("com.DA.job.service", "http://" + InetAddress.getLocalHost().getHostName() + ":8080/updateCompressionTaskStatus");
        final StringBuffer sb = new StringBuffer();
        sb.append("id=").append(id).append("&");
        sb.append("status").append("=");
        try {
            this.runJob(true);
            sb.append("2");
        }
        catch (Exception e) {
            sb.append("3");
        }
        final URL url = new URL(servie + "?" + (Object)sb);
        CompressConvert.LOG.info("updata status url " + url);
        final BufferedReader br = StreamUtil.getBufferedReader(url.openStream());
        final String line = br.readLine();
        if (line != null && line.equals("0")) {
            System.out.println("SUCCESS");
        }
        else {
            System.out.println("ERROR");
        }
        br.close();
    }
}

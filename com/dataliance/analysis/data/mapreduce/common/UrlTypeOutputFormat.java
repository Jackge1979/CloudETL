package com.dataliance.analysis.data.mapreduce.common;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.io.*;

public class UrlTypeOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    public static final String PIC_NAME = "pic_url";
    public static final String TEXT_NAME = "text_url";
    
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        final boolean isCompressed = getCompressOutput((JobContext)job);
        final String keyValueSeparator = conf.get("mapred.textoutputformat.separator", "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = (Class<? extends CompressionCodec>)getOutputCompressorClass((JobContext)job, (Class)GzipCodec.class);
            codec = (CompressionCodec)ReflectionUtils.newInstance((Class)codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path file = getOutputPath((JobContext)job);
        final Path picPath = new Path(file, "pic_url");
        final Path textPath = new Path(file, "text_url");
        final FileSystem fs = file.getFileSystem(conf);
        final FSDataOutputStream picOut = fs.create(picPath, false);
        final FSDataOutputStream textOut = fs.create(textPath, false);
        if (!isCompressed) {
            return new LineRecordWriter<K, V>((DataOutputStream)picOut, (DataOutputStream)textOut, keyValueSeparator);
        }
        return new LineRecordWriter<K, V>(new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)picOut)), new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)textOut)), keyValueSeparator);
    }
    
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V>
    {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        protected DataOutputStream picOut;
        protected DataOutputStream textOut;
        private final byte[] keyValueSeparator;
        
        public LineRecordWriter(final DataOutputStream out, final String keyValueSeparator) {
            this.picOut = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find UTF-8 encoding");
            }
        }
        
        public LineRecordWriter(final DataOutputStream picOut, final DataOutputStream textOut, final String keyValueSeparator) {
            this.picOut = picOut;
            this.textOut = textOut;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find UTF-8 encoding");
            }
        }
        
        public LineRecordWriter(final DataOutputStream out) {
            this(out, "\t");
        }
        
        public synchronized void write(final K key, final V value) throws IOException {
            if (key.toString().endsWith("jpg") || key.toString().endsWith("gif")) {
                this.write(this.picOut, key, value);
            }
            else {
                this.write(this.textOut, key, value);
            }
        }
        
        public synchronized void write(final DataOutputStream out, final K key, final V value) throws IOException {
            final boolean nullKey = key == null || key instanceof NullWritable;
            final boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                out.writeBytes(key.toString());
            }
            if (!nullKey && !nullValue) {
                out.write(this.keyValueSeparator);
            }
            if (!nullValue) {
                out.writeBytes(value.toString());
            }
            out.write(LineRecordWriter.newline);
        }
        
        public synchronized void close(final TaskAttemptContext context) throws IOException {
            this.picOut.close();
            this.textOut.close();
        }
        
        static {
            try {
                newline = "\n".getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find UTF-8 encoding");
            }
        }
    }
}

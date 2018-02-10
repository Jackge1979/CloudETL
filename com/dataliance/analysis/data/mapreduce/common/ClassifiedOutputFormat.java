package com.dataliance.analysis.data.mapreduce.common;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.io.*;

public class ClassifiedOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    public static final String CLASSIFIED_NAME = "classified";
    public static final String UNCLASSIFIED_NAME = "un_classified";
    
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
        final FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(job);
        Path classifiedPath = new Path(committer.getWorkPath(), "classified");
        Path unclassifiedPath = new Path(committer.getWorkPath(), "un_classified");
        classifiedPath = new Path(classifiedPath, getUniqueFile(job, getOutputName((JobContext)job), extension));
        unclassifiedPath = new Path(unclassifiedPath, getUniqueFile(job, getOutputName((JobContext)job), extension));
        final FileSystem fs = file.getFileSystem(conf);
        final FSDataOutputStream classifiedOut = fs.create(classifiedPath, false);
        final FSDataOutputStream unclassifiedOut = fs.create(unclassifiedPath, false);
        if (!isCompressed) {
            return new LineRecordWriter<K, V>((DataOutputStream)classifiedOut, (DataOutputStream)unclassifiedOut, keyValueSeparator);
        }
        return new LineRecordWriter<K, V>(new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)classifiedOut)), new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)unclassifiedOut)), keyValueSeparator);
    }
    
    public Path getDefaultWorkFile(final TaskAttemptContext context, final String extension) throws IOException {
        final FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
        System.out.println("committer.getWorkPath():" + committer.getWorkPath());
        return new Path(committer.getWorkPath(), getUniqueFile(context, getOutputName((JobContext)context), extension));
    }
    
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V>
    {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        protected DataOutputStream classifiedOut;
        protected DataOutputStream unclassifiedOut;
        private final byte[] keyValueSeparator;
        
        public LineRecordWriter(final DataOutputStream out, final String keyValueSeparator) {
            this.classifiedOut = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find UTF-8 encoding");
            }
        }
        
        public LineRecordWriter(final DataOutputStream classifiedOut, final DataOutputStream unclassifiedOut, final String keyValueSeparator) {
            this.classifiedOut = classifiedOut;
            this.unclassifiedOut = unclassifiedOut;
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
            if ("-1".equals(value.toString())) {
                this.write(this.unclassifiedOut, key, value);
            }
            else {
                this.write(this.classifiedOut, key, value);
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
            this.classifiedOut.close();
            this.unclassifiedOut.close();
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

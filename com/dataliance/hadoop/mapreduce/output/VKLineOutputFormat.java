package com.dataliance.hadoop.mapreduce.output;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class VKLineOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    public static final String NEW_LINE = "\n";
    public static final String SPLIT = "\t";
    public static final String OUTPUT_SPLIT = "com.DA.output.split";
    
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final String split = conf.get("com.DA.output.split", "\t");
        final boolean isCompressed = getCompressOutput((JobContext)context);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = (Class<? extends CompressionCodec>)getOutputCompressorClass((JobContext)context, (Class)GzipCodec.class);
            codec = (CompressionCodec)ReflectionUtils.newInstance((Class)codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path path = getOutputPath((JobContext)context);
        final FileSystem fs = path.getFileSystem(conf);
        final Path file = this.getDefaultWorkFile(context, extension);
        final FSDataOutputStream fileWrite = fs.create(new Path(path, file.getName()), false);
        if (!isCompressed) {
            return new LineRecordWriter<K, V>((DataOutputStream)fileWrite, split);
        }
        return new LineRecordWriter<K, V>(new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)fileWrite)), split);
    }
    
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V>
    {
        private final DataOutputStream fileWrite;
        private final String split;
        
        LineRecordWriter(final DataOutputStream fileWrite, final String split) {
            this.fileWrite = fileWrite;
            this.split = split;
        }
        
        public synchronized void write(final K key, final V value) throws IOException, InterruptedException {
            this.fileWrite.write(StringUtil.toBytes(value + this.split + key + "\n"));
        }
        
        public synchronized void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileWrite.close();
        }
    }
}

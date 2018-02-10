package com.dataliance.hadoop.mapreduce.output;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class KeyLineOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    public static final String NEW_LINE = "\n";
    
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
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
            return new LineRecordWriter<K, V>((DataOutputStream)fileWrite);
        }
        return new LineRecordWriter<K, V>(new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)fileWrite)));
    }
    
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V>
    {
        private final DataOutputStream fileWrite;
        
        LineRecordWriter(final DataOutputStream fileWrite) {
            this.fileWrite = fileWrite;
        }
        
        public synchronized void write(final K key, final V value) throws IOException, InterruptedException {
            this.fileWrite.write(StringUtil.toBytes(key.toString() + "\n"));
        }
        
        public synchronized void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileWrite.close();
        }
    }
}

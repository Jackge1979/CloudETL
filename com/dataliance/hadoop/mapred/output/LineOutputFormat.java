package com.dataliance.hadoop.mapred.output;

import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.mapred.*;

public class LineOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    public static final String NEW_LINE = "\n";
    
    public RecordWriter<K, V> getRecordWriter(final FileSystem ignored, final JobConf job, final String name, final Progressable progress) throws IOException {
        final boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = (Class<? extends CompressionCodec>)getOutputCompressorClass(job, (Class)GzipCodec.class);
            codec = (CompressionCodec)ReflectionUtils.newInstance((Class)codecClass, (Configuration)job);
            extension = codec.getDefaultExtension();
        }
        final Path path = getOutputPath(job);
        final FileSystem fs = path.getFileSystem((Configuration)job);
        final FSDataOutputStream fileWrite = fs.create(new Path(path, name + extension), false);
        if (!isCompressed) {
            return (RecordWriter<K, V>)new LineRecordWriter((DataOutputStream)fileWrite);
        }
        return (RecordWriter<K, V>)new LineRecordWriter(new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)fileWrite)));
    }
    
    protected static class LineRecordWriter<K, V> implements RecordWriter<K, V>
    {
        private final DataOutputStream fileWrite;
        
        LineRecordWriter(final DataOutputStream fileWrite) {
            this.fileWrite = fileWrite;
        }
        
        public void close(final Reporter reporter) throws IOException {
            this.fileWrite.close();
        }
        
        public void write(final K key, final V value) throws IOException {
            this.fileWrite.write(StringUtil.toBytes(value.toString() + "\n"));
        }
    }
}

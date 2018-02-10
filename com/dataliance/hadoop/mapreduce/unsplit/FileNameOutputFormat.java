package com.dataliance.hadoop.mapreduce.unsplit;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import java.io.*;

import com.dataliance.hadoop.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.fs.*;
import java.util.*;

public class FileNameOutputFormat<K extends FileKey, V> extends FileOutputFormat<K, V>
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
        return new FileNameRecordWriter<K, V>(path, extension, isCompressed, codec, conf);
    }
    
    public static void main(final String[] args) {
        final String name = "name.txt";
        System.out.println(name.substring(0, name.lastIndexOf(".")));
    }
    
    protected static class FileNameRecordWriter<K extends FileKey, V> extends RecordWriter<K, V>
    {
        private final Map<String, DataOutputStream> writers;
        private final Path dir;
        private final String extension;
        private final boolean compressed;
        private final CompressionCodec codec;
        private final FileSystem fs;
        
        FileNameRecordWriter(final Path dir, final String extension, final boolean compressed, final CompressionCodec codec, final Configuration conf) throws IOException {
            this.writers = new HashMap<String, DataOutputStream>();
            this.dir = dir;
            this.extension = extension;
            this.compressed = compressed;
            this.codec = codec;
            this.fs = dir.getFileSystem(conf);
        }
        
        public synchronized void write(final K key, final V value) throws IOException, InterruptedException {
            final String path = key.getPath();
            DataOutputStream writer = this.writers.get(path);
            if (writer == null) {
                final Path src = new Path(path);
                final String name = src.getName();
                final int index = name.lastIndexOf(".");
                String realName = name;
                if (index != -1) {
                    realName = name.substring(0, index);
                }
                final Path file = new Path(this.dir, realName + this.extension);
                final FSDataOutputStream fileWrite = this.fs.create(file, false);
                if (this.compressed) {
                    writer = new DataOutputStream((OutputStream)this.codec.createOutputStream((OutputStream)fileWrite));
                }
                else {
                    writer = (DataOutputStream)fileWrite;
                }
                this.writers.put(path, writer);
            }
            writer.write(StringUtil.toBytes(value.toString() + "\n"));
        }
        
        public synchronized void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            for (final DataOutputStream writer : this.writers.values()) {
                writer.close();
            }
        }
    }
}

package com.dataliance.hadoop.mapreduce.output;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

public class MapFileOutputFormat extends FileOutputFormat<WritableComparable<?>, Writable>
{
    public void checkOutputSpecs(final JobContext job) throws FileAlreadyExistsException, IOException {
        final Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        final FileSystem fs = outDir.getFileSystem(job.getConfiguration());
        if (fs.exists(outDir)) {
            throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
        }
    }
    
    public static SequenceFile.CompressionType getOutputCompressionType(final JobContext job) {
        final String val = job.getConfiguration().get("mapred.output.compression.type", SequenceFile.CompressionType.RECORD.toString());
        return SequenceFile.CompressionType.valueOf(val);
    }
    
    public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        final boolean isCompressed = getCompressOutput((JobContext)job);
        CompressionCodec codec = null;
        String extension = "";
        SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
        if (isCompressed) {
            compressionType = getOutputCompressionType((JobContext)job);
            final Class<? extends CompressionCodec> codecClass = (Class<? extends CompressionCodec>)getOutputCompressorClass((JobContext)job, (Class)GzipCodec.class);
            codec = (CompressionCodec)ReflectionUtils.newInstance((Class)codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path path = getOutputPath((JobContext)job);
        final FileSystem fs = path.getFileSystem(conf);
        final Path file = this.getDefaultWorkFile(job, extension);
        final Class<? extends WritableComparable> keyClass = job.getOutputKeyClass().asSubclass(WritableComparable.class);
        final Class<?> valueClass = job.getOutputValueClass().asSubclass(Writable.class);
        final MapFile.Writer writer = new MapFile.Writer(conf, fs, file.toString(), WritableComparator.get((Class)keyClass), (Class)valueClass, compressionType, codec, (Progressable)job);
        return new MapRecordWriter(writer);
    }
    
    protected static class MapRecordWriter extends RecordWriter<WritableComparable<?>, Writable>
    {
        private final MapFile.Writer writer;
        
        MapRecordWriter(final MapFile.Writer writer) {
            this.writer = writer;
        }
        
        public synchronized void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.writer.close();
        }
        
        public void write(final WritableComparable<?> key, final Writable value) throws IOException, InterruptedException {
            this.writer.append((WritableComparable)key, value);
        }
    }
}

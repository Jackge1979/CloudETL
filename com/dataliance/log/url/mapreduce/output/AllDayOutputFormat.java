package com.dataliance.log.url.mapreduce.output;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;

import com.dataliance.util.*;
import com.dataliance.log.url.vo.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.*;

public class AllDayOutputFormat extends FileOutputFormat<Text, AllDayVO>
{
    public static final String NEW_LINE = "\n";
    public static final String SPLIT = "\t";
    public static final String OUTPUT_SPLIT = "com.DA.output.split";
    public static final String INPUT_PATH_NUM = "com.DA.intput.path.num";
    public static final int PATH_NUM = 6;
    
    public RecordWriter<Text, AllDayVO> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
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
        final int pathNum = conf.getInt("com.DA.intput.path.num", 6);
        final Map<Integer, DataOutputStream> writers = new HashMap<Integer, DataOutputStream>();
        for (int i = 2; i <= pathNum; ++i) {
            final Path numPath = new Path(path, Integer.toString(i));
            final FSDataOutputStream fileWrite = fs.create(new Path(numPath, file.getName()), false);
            if (!isCompressed) {
                writers.put(i, (DataOutputStream)fileWrite);
            }
            else {
                writers.put(i, new DataOutputStream((OutputStream)codec.createOutputStream((OutputStream)fileWrite)));
            }
        }
        return new LineRecordWriter(writers, split);
    }
    
    protected static class LineRecordWriter extends RecordWriter<Text, AllDayVO>
    {
        private final String split;
        private final Map<Integer, DataOutputStream> writers;
        
        LineRecordWriter(final Map<Integer, DataOutputStream> writers, final String split) {
            this.writers = writers;
            this.split = split;
        }
        
        public synchronized void write(final Text key, final AllDayVO value) throws IOException, InterruptedException {
            final DataOutputStream writer = this.writers.get(value.getSize());
            if (writer != null) {
                writer.write(StringUtil.toBytes(value + this.split + key + "\n"));
            }
        }
        
        public synchronized void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            for (final DataOutputStream writer : this.writers.values()) {
                writer.close();
            }
        }
    }
}

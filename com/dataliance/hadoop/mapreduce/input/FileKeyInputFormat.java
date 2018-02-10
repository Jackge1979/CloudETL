package com.dataliance.hadoop.mapreduce.input;

import org.apache.hadoop.mapreduce.lib.input.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.*;

public class FileKeyInputFormat extends FileInputFormat<FileKey, Text>
{
    public RecordReader<FileKey, Text> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        return new FileKeyLineRecordReader();
    }
    
    protected boolean isSplitable(final JobContext context, final Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}

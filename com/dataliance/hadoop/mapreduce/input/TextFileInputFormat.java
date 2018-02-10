package com.dataliance.hadoop.mapreduce.input;

import org.apache.hadoop.mapreduce.lib.input.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.*;

public class TextFileInputFormat extends FileInputFormat<FileInfo, Text>
{
    public RecordReader<FileInfo, Text> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        return new TextFileLineRecordReader();
    }
    
    protected boolean isSplitable(final JobContext context, final Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}

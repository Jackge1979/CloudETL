package com.dataliance.hadoop.mapreduce.unsplit;

import org.apache.hadoop.mapreduce.lib.input.*;

import com.dataliance.hadoop.mapreduce.input.*;
import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;

public class UnSplitInputFormat extends FileInputFormat<FileKey, Text>
{
    public RecordReader<FileKey, Text> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        return new FileKeyLineRecordReader();
    }
    
    protected boolean isSplitable(final JobContext context, final Path file) {
        return false;
    }
}

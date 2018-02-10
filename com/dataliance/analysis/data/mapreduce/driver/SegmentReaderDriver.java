package com.dataliance.analysis.data.mapreduce.driver;

import org.apache.hadoop.fs.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import com.dataliance.nutch.segment.*;
import java.io.*;

public class SegmentReaderDriver
{
    public static void runJob(final Path input, final Path output, final Properties params) {
        final Configuration conf = new Configuration();
        final SegmentReader segmentReader = new SegmentReader(conf, false, false, false, false, true, true);
        try {
            segmentReader.dump(input, output);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(final String[] args) {
        final String input = "D:/workspace/Nutch/crawler/1/segments/20120228144804";
        final String output = "./dump";
        final Path inputPath = new Path(input);
        final Path outputPath = new Path(output);
        runJob(inputPath, outputPath, null);
    }
}

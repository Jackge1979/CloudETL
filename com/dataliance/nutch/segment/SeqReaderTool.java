package com.dataliance.nutch.segment;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import java.io.*;

public class SeqReaderTool
{
    public static void readWebDocument(final Path path) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        final FileSystem fs = FileSystem.get(conf);
        final SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        final Class<Writable> keyCLass = (Class<Writable>)reader.getKeyClass();
        final Class<Writable> valClass = (Class<Writable>)reader.getValueClass();
        final Writable key = (Writable)ReflectionUtils.newInstance((Class)keyCLass, conf);
        final Writable val = (Writable)ReflectionUtils.newInstance((Class)valClass, conf);
        while (reader.next(key, val)) {
            System.out.println(key + " : " + val.toString().trim());
        }
    }
    
    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: cmd inputpath");
            System.exit(-1);
        }
        final Path inputPath = new Path(args[0]);
        readWebDocument(inputPath);
    }
}

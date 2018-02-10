package com.dataliance.log.url;

import java.util.regex.*;
import org.apache.hadoop.fs.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import java.io.*;

public class TestSplit
{
    private static String split;
    private static Pattern pattern;
    
    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage : TestSplit <path>");
            return;
        }
        final Path path = new Path(args[0]);
        final Configuration conf = DAConfigUtil.create();
        final FileSystem fs = FileSystem.get(conf);
        final BufferedReader br = StreamUtil.getBufferedReader((InputStream)fs.open(path));
        final String line = br.readLine();
        for (int num = 0; line != null && num < 5; ++num) {
            final String[] vs = TestSplit.pattern.split(line);
            System.out.println(num + "_vs.length = " + vs.length);
            for (int i = 0; i < vs.length; ++i) {
                System.out.println(num + "_" + i + " = " + vs[i]);
            }
            System.out.println("========================================");
        }
    }
    
    static {
        TestSplit.split = "\t";
        TestSplit.pattern = Pattern.compile(TestSplit.split);
    }
}

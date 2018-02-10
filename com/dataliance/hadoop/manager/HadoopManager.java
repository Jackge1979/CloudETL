package com.dataliance.hadoop.manager;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import java.io.*;

public class HadoopManager extends Manager
{
    public HadoopManager(final Configuration conf) throws IOException, InterruptedException {
        super(conf);
    }
    
    public static void main(final String[] args) {
        final String usage = "HadoopManager Usage: -stop <JobID>";
        if (args.length < 2) {
            System.err.println(usage);
        }
    }
}

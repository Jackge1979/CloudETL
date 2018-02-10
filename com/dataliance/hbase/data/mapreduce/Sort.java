package com.dataliance.hbase.data.mapreduce;

import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.conf.*;
import java.net.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class Sort<K, V> extends Configured implements Tool
{
    private RunningJob jobResult;
    
    public Sort() {
        this.jobResult = null;
    }
    
    static int printUsage() {
        System.out.println("sort [-m <maps>] [-r <reduces>] [-inFormat <input format class>] [-outFormat <output format class>] [-outKey <output key class>] [-outValue <output value class>] [-totalOrder <pcnt> <num samples> <max splits>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    
    public int run(final String[] args) throws Exception {
        final JobConf jobConf = new JobConf(this.getConf(), (Class)Sort.class);
        jobConf.setJobName("sorter");
        jobConf.setMapperClass((Class)IdentityMapper.class);
        jobConf.setReducerClass((Class)IdentityReducer.class);
        final JobClient client = new JobClient(jobConf);
        final ClusterStatus cluster = client.getClusterStatus();
        int num_reduces = (int)(cluster.getMaxReduceTasks() * 0.9);
        final String sort_reduces = jobConf.get("test.sort.reduces_per_host");
        if (sort_reduces != null) {
            num_reduces = cluster.getTaskTrackers() * Integer.parseInt(sort_reduces);
        }
        Class<? extends InputFormat> inputFormatClass = (Class<? extends InputFormat>)SequenceFileInputFormat.class;
        Class<? extends OutputFormat> outputFormatClass = (Class<? extends OutputFormat>)SequenceFileOutputFormat.class;
        Class<? extends WritableComparable> outputKeyClass = (Class<? extends WritableComparable>)BytesWritable.class;
        Class<? extends Writable> outputValueClass = (Class<? extends Writable>)BytesWritable.class;
        final List<String> otherArgs = new ArrayList<String>();
        InputSampler.Sampler<K, V> sampler = null;
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    jobConf.setNumMapTasks(Integer.parseInt(args[++i]));
                }
                else if ("-r".equals(args[i])) {
                    num_reduces = Integer.parseInt(args[++i]);
                }
                else if ("-inFormat".equals(args[i])) {
                    inputFormatClass = Class.forName(args[++i]).asSubclass(InputFormat.class);
                }
                else if ("-outFormat".equals(args[i])) {
                    outputFormatClass = Class.forName(args[++i]).asSubclass(OutputFormat.class);
                }
                else if ("-outKey".equals(args[i])) {
                    outputKeyClass = Class.forName(args[++i]).asSubclass(WritableComparable.class);
                }
                else if ("-outValue".equals(args[i])) {
                    outputValueClass = Class.forName(args[++i]).asSubclass(Writable.class);
                }
                else if ("-totalOrder".equals(args[i])) {
                    final double pcnt = Double.parseDouble(args[++i]);
                    final int numSamples = Integer.parseInt(args[++i]);
                    int maxSplits = Integer.parseInt(args[++i]);
                    if (0 >= maxSplits) {
                        maxSplits = Integer.MAX_VALUE;
                    }
                    sampler = (InputSampler.Sampler<K, V>)new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
                }
                else {
                    otherArgs.add(args[i]);
                }
            }
            catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            }
            catch (ArrayIndexOutOfBoundsException except2) {
                System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
                return printUsage();
            }
        }
        jobConf.setNumReduceTasks(num_reduces);
        jobConf.setInputFormat((Class)inputFormatClass);
        jobConf.setOutputFormat((Class)outputFormatClass);
        jobConf.setOutputKeyClass((Class)outputKeyClass);
        jobConf.setOutputValueClass((Class)outputValueClass);
        if (otherArgs.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(jobConf, (String)otherArgs.get(0));
        FileOutputFormat.setOutputPath(jobConf, new Path((String)otherArgs.get(1)));
        if (sampler != null) {
            System.out.println("Sampling input to effect total-order sort...");
            jobConf.setPartitionerClass((Class)TotalOrderPartitioner.class);
            Path inputDir = FileInputFormat.getInputPaths(jobConf)[0];
            inputDir = inputDir.makeQualified(inputDir.getFileSystem((Configuration)jobConf));
            final Path partitionFile = new Path(inputDir, "_sortPartitioning");
            TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
            InputSampler.writePartitionFile(jobConf, (InputSampler.Sampler)sampler);
            final URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning");
            DistributedCache.addCacheFile(partitionUri, (Configuration)jobConf);
            DistributedCache.createSymlink((Configuration)jobConf);
        }
        System.out.println("Running on " + cluster.getTaskTrackers() + " nodes to sort from " + FileInputFormat.getInputPaths(jobConf)[0] + " into " + FileOutputFormat.getOutputPath(jobConf) + " with " + num_reduces + " reduces.");
        final Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        this.jobResult = JobClient.runJob(jobConf);
        final Date end_time = new Date();
        System.out.println("Job ended: " + end_time);
        System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000L + " seconds.");
        return 0;
    }
    
    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run((Tool)new Sort(), args);
        System.exit(res);
    }
    
    public RunningJob getResult() {
        return this.jobResult;
    }
}

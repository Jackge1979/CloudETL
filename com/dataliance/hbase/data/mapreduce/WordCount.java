package com.dataliance.hbase.data.mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;

public class WordCount
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        final Job job = new Job(conf, "word count");
        job.setJarByClass((Class)WordCount.class);
        job.setMapperClass((Class)TokenizerMapper.class);
        job.setCombinerClass((Class)IntSumReducer.class);
        job.setReducerClass((Class)IntSumReducer.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result;
        
        public IntSumReducer() {
            this.result = new IntWritable();
        }
        
        public void reduce(final Text key, final Iterable<IntWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            this.result.set(sum);
            context.write((Object)key, (Object)this.result);
        }
    }
    
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private static final IntWritable one;
        private Text word;
        
        public TokenizerMapper() {
            this.word = new Text();
        }
        
        public void map(final Object key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            final String text = value.toString();
            final StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write((Object)this.word, (Object)TokenizerMapper.one);
            }
        }
        
        static {
            one = new IntWritable(1);
        }
    }
}

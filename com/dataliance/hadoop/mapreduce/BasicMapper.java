package com.dataliance.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.*;
import java.io.*;

public class BasicMapper<KEY, VALUE> extends Mapper<KEY, VALUE, KEY, VALUE>
{
    protected void map(final KEY key, final VALUE value, final Mapper.Context context) throws IOException, InterruptedException {
        context.write((Object)key, (Object)value);
    }
}

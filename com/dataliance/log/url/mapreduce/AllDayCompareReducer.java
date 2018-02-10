package com.dataliance.log.url.mapreduce;

import org.apache.hadoop.io.*;
import com.dataliance.log.url.vo.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;

public class AllDayCompareReducer extends Reducer<Text, AllDayVO, Text, AllDayVO>
{
    public static final String COMPARE_COUNT_GROUP = "CompareGroup";
    public static final String COMPARE_COUNT_NAME = "Compare_";
    private AllDayVO lastValue;
    
    public AllDayCompareReducer() {
        this.lastValue = new AllDayVO();
    }
    
    protected void reduce(final Text key, final Iterable<AllDayVO> values, final Reducer.Context context) throws IOException, InterruptedException {
        this.lastValue.init();
        for (final AllDayVO value : values) {
            this.lastValue.addAll(value.getDayInfos());
        }
        final int size = this.lastValue.getSize();
        final String counterName = "Compare_" + size;
        final Counter counter = context.getCounter("CompareGroup", counterName);
        counter.increment(1L);
        if (size > 1) {
            context.write((Object)key, (Object)this.lastValue);
        }
    }
}

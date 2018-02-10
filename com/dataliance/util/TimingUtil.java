package com.dataliance.util;

import java.text.*;

public class TimingUtil
{
    private static long[] TIME_FACTOR;
    
    public static String elapsedTime(long start, final long end) {
        if (start > end) {
            return null;
        }
        final long[] elapsedTime = new long[TimingUtil.TIME_FACTOR.length];
        for (int i = 0; i < TimingUtil.TIME_FACTOR.length; ++i) {
            elapsedTime[i] = ((start > end) ? -1L : ((end - start) / TimingUtil.TIME_FACTOR[i]));
            start += TimingUtil.TIME_FACTOR[i] * elapsedTime[i];
        }
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(2);
        final StringBuffer buf = new StringBuffer();
        for (int j = 0; j < elapsedTime.length; ++j) {
            if (j > 0) {
                buf.append(":");
            }
            buf.append(nf.format(elapsedTime[j]));
        }
        return buf.toString();
    }
    
    public static void main(final String[] args) {
        System.out.println(elapsedTime(System.currentTimeMillis(), System.currentTimeMillis() + 1L));
        System.out.println();
    }
    
    static {
        TimingUtil.TIME_FACTOR = new long[] { 3600000L, 60000L, 1000L, 1L };
    }
}

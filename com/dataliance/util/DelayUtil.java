package com.dataliance.util;

import java.text.*;
import java.util.logging.Logger;

import org.slf4j.*;

public class DelayUtil
{
    public static final Logger LOG;
    private static final SimpleDateFormat sdf;
    private long start;
    private long end;
    
    public long getStart() {
        return this.start;
    }
    
    public void setStart(final long start) {
        this.start = start;
    }
    
    public long getEnd() {
        return this.end;
    }
    
    public void setEnd(final long end) {
        this.end = end;
    }
    
    public void printStart(final String type) {
        DelayUtil.LOG.info(type + " : starting at " + DelayUtil.sdf.format(this.start));
    }
    
    public void printEnd(final String type) {
        DelayUtil.LOG.info(type + " : finished at " + DelayUtil.sdf.format(this.end) + ", elapsed: " + TimingUtil.elapsedTime(this.start, this.end));
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)DelayUtil.class);
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
}

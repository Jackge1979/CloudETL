package com.dataliance.dom.protocol;

import java.text.*;
import java.util.*;

public class HttpDateFormat
{
    protected static SimpleDateFormat format;
    
    public static String toString(final Date date) {
        final String string;
        synchronized (HttpDateFormat.format) {
            string = HttpDateFormat.format.format(date);
        }
        return string;
    }
    
    public static String toString(final Calendar cal) {
        final String string;
        synchronized (HttpDateFormat.format) {
            string = HttpDateFormat.format.format(cal.getTime());
        }
        return string;
    }
    
    public static String toString(final long time) {
        final String string;
        synchronized (HttpDateFormat.format) {
            string = HttpDateFormat.format.format(new Date(time));
        }
        return string;
    }
    
    public static Date toDate(final String dateString) throws ParseException {
        final Date date;
        synchronized (HttpDateFormat.format) {
            date = HttpDateFormat.format.parse(dateString);
        }
        return date;
    }
    
    public static long toLong(final String dateString) throws ParseException {
        final long time;
        synchronized (HttpDateFormat.format) {
            time = HttpDateFormat.format.parse(dateString).getTime();
        }
        return time;
    }
    
    static {
        (HttpDateFormat.format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)).setTimeZone(TimeZone.getTimeZone("GMT"));
    }
}

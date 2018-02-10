package com.dataliance.util;

import java.util.regex.*;
import java.text.*;
import java.util.*;

public class DateUtil
{
    private static final Pattern secondPatt;
    private static final Pattern minPatt;
    private static final Pattern hourPatt;
    private static final Pattern hourMinPatt;
    private static final Pattern datePatt;
    private static final Pattern todayPatt;
    private static final Pattern todayPattQ;
    private static final String format = "MM\u6708dd\u65e5 HH:mm";
    private static final String dateFormat = "yyyyMMddHHmmss";
    private static final SimpleDateFormat sdf;
    private static final SimpleDateFormat sdateFormat;
    private static final String[] DATEFORMAT;
    
    public static long getDate(final String value) throws ParseException {
        Matcher matcher = DateUtil.datePatt.matcher(value);
        long time = 0L;
        if (matcher.find()) {
            return parseDate((value.length() >= 22) ? value.substring(0, 22) : value).getTime();
        }
        if ((matcher = DateUtil.secondPatt.matcher(value)).find()) {
            time = Integer.parseInt(matcher.group(1)) * 1000;
        }
        else if ((matcher = DateUtil.minPatt.matcher(value)).find()) {
            time = Integer.parseInt(matcher.group(1)) * 1000 * 60;
        }
        else if ((matcher = DateUtil.hourPatt.matcher(value)).find()) {
            time = Integer.parseInt(matcher.group(1)) * 1000 * 60 * 60;
            if ((matcher = DateUtil.hourMinPatt.matcher(value)).find()) {
                time += Integer.parseInt(matcher.group(2)) * 1000 * 60;
            }
        }
        else {
            if ((matcher = DateUtil.todayPatt.matcher(value)).find()) {
                final Calendar c = Calendar.getInstance();
                c.set(11, Integer.parseInt(matcher.group(1)));
                c.set(12, Integer.parseInt(matcher.group(2)));
                return c.getTimeInMillis();
            }
            if ((matcher = DateUtil.todayPattQ.matcher(value)).find()) {
                final Calendar c = Calendar.getInstance();
                c.set(5, c.get(5) - Integer.parseInt(matcher.group(1)));
                return c.getTimeInMillis();
            }
            return parseDate(value).getTime();
        }
        return System.currentTimeMillis() - time;
    }
    
    public static String getHourLater(final long hour) {
        return DateUtil.sdateFormat.format(System.currentTimeMillis() - hour * 3600L * 1000L);
    }
    
    public static long getCurrentYearMillis() {
        final Calendar c = Calendar.getInstance();
        c.set(2, 0);
        c.set(5, 1);
        c.set(10, 8);
        c.set(12, 0);
        return c.getTimeInMillis();
    }
    
    public static Date parseDate(final String dateValue) throws ParseException {
        if (dateValue == null) {
            return null;
        }
        final String[] arr$ = DateUtil.DATEFORMAT;
        final int len$ = arr$.length;
        int i$ = 0;
        while (i$ < len$) {
            final String format = arr$[i$];
            final SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.US);
            try {
                final Date date = sdf.parse(dateValue);
                return date;
            }
            catch (ParseException e) {
                ++i$;
                break;
            }
            
        }
        return new Date();
    }
    
    public static String formate(final long date) {
        return DateUtil.sdf.format(new Date(date));
    }
    
    public static int getYear(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(1);
    }
    
    public static int getMonth(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(2);
    }
    
    public static int getDay(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(5);
    }
    
    static {
        secondPatt = Pattern.compile("^(\\d{1,})\u79d2\u949f");
        minPatt = Pattern.compile("^(\\d{1,})\u5206\u949f");
        hourPatt = Pattern.compile("^(\\d{1,})\u5c0f\u65f6");
        hourMinPatt = Pattern.compile("^(\\d{1,})\u5c0f\u65f6(\\d{1,})\u5206");
        datePatt = Pattern.compile("^\\d{4}\u5e74\\d{2}\u6708\\d{2}\u65e5");
        todayPatt = Pattern.compile("^\u4eca\u5929 (\\d{2}):(\\d{2})");
        todayPattQ = Pattern.compile("(\\d{1,})\u5929\u524d");
        sdf = new SimpleDateFormat("MM\u6708dd\u65e5 HH:mm", Locale.US);
        sdateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US);
        DATEFORMAT = new String[] { "yyyyMMddHHmmss", "yyyyMMdd", "yyyy\u5e74 MM\u6708 dd\u65e5, HH : mm", "yyyy\u5e74MM\u6708dd\u65e5, HH:mm", "MM-dd HH:mm", "M\u6708d\u65e5 HH:mm", "MM\u6708d\u65e5 HH:mm", "M\u6708dd\u65e5 HH:mm", "MM\u6708dd\u65e5 HH:mm", "MM\u6708dd\u65e5HH\u65f6mm\u5206", "yyyy-MM-dd HH:mm:sszzzzzz", "yyyy-MM-dd HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss", "EEE MMM dd HH:mm:ss Z yyyy", "EEE MMM dd HH:mm:ss yyyy", "EEE MMM dd HH:mm:ss yyyy zzz", "EEE, MMM dd HH:mm:ss yyyy zzz", "EEE, dd MMM yyyy HH:mm:ss zzz", "EEE,dd MMM yyyy HH:mm:ss zzz", "EEE, dd MMM yyyy HH:mm:sszzz", "EEE, dd MMM yyyy HH:mm:ss", "EEE, dd-MMM-yy HH:mm:ss zzz", "yyyy/MM/dd HH:mm:ss.SSS zzz", "yyyy/MM/dd HH:mm:ss.SSS", "yyyy/MM/dd HH:mm:ss zzz", "yyyy/MM/dd", "yyyy.MM.dd HH:mm:ss", "yyyy-MM-dd HH:mm", "MMM dd yyyy HH:mm:ss. zzz", "MMM dd yyyy HH:mm:ss zzz", "dd.MM.yyyy HH:mm:ss zzz", "dd MM yyyy HH:mm:ss zzz", "dd.MM.yyyy; HH:mm:ss", "dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy zzz", "yyyy-MM-dd", "HH:ss" };
    }
}

package com.dataliance.util;

import java.util.regex.*;
import java.net.*;

public class URLUtil
{
    static Pattern IP_Pattern;
    static Pattern HOST_pattern;
    private static Pattern IP_PATTERN;
    
    public static boolean isIP(final String ip) {
        return URLUtil.IP_Pattern.matcher(ip).matches();
    }
    
    public static boolean isHost(final String host) {
        return URLUtil.HOST_pattern.matcher(host).matches();
    }
    
    public static boolean isIpOrHost(final String host) {
        return isIP(host) || isHost(host);
    }
    
    public static URL resolveURL(final URL base, String target) throws MalformedURLException {
        target = target.trim();
        if (target.startsWith("?")) {
            return fixPureQueryTargets(base, target);
        }
        return new URL(base, target);
    }
    
    static URL fixPureQueryTargets(final URL base, String target) throws MalformedURLException {
        if (!target.startsWith("?")) {
            return new URL(base, target);
        }
        final String basePath = base.getPath();
        String baseRightMost = "";
        final int baseRightMostIdx = basePath.lastIndexOf("/");
        if (baseRightMostIdx != -1) {
            baseRightMost = basePath.substring(baseRightMostIdx + 1);
        }
        if (target.startsWith("?")) {
            target = baseRightMost + target;
        }
        return new URL(base, target);
    }
    
    private static URL fixEmbeddedParams(final URL base, String target) throws MalformedURLException {
        if (target.indexOf(59) >= 0 || base.toString().indexOf(59) == -1) {
            return new URL(base, target);
        }
        final String baseURL = base.toString();
        final int startParams = baseURL.indexOf(59);
        final String params = baseURL.substring(startParams);
        final int startQS = target.indexOf(63);
        if (startQS >= 0) {
            target = target.substring(0, startQS) + params + target.substring(startQS);
        }
        else {
            target += params;
        }
        return new URL(base, target);
    }
    
    public static String getDomainName(final URL url) {
        final DomainSuffixes tlds = DomainSuffixes.getInstance();
        String host = url.getHost();
        if (host.endsWith(".")) {
            host = host.substring(0, host.length() - 1);
        }
        if (URLUtil.IP_PATTERN.matcher(host).matches()) {
            return host;
        }
        int index = 0;
        String candidate = host;
        while (index >= 0) {
            index = candidate.indexOf(46);
            final String subCandidate = candidate.substring(index + 1);
            if (tlds.isDomainSuffix(subCandidate)) {
                return candidate;
            }
            candidate = subCandidate;
        }
        return candidate;
    }
    
    public static String getDomainName(final String url) throws MalformedURLException {
        return getDomainName(new URL(url));
    }
    
    public static boolean isSameDomainName(final URL url1, final URL url2) {
        return getDomainName(url1).equalsIgnoreCase(getDomainName(url2));
    }
    
    public static boolean isSameDomainName(final String url1, final String url2) throws MalformedURLException {
        return isSameDomainName(new URL(url1), new URL(url2));
    }
    
    public static DomainSuffix getDomainSuffix(final URL url) {
        final DomainSuffixes tlds = DomainSuffixes.getInstance();
        final String host = url.getHost();
        if (URLUtil.IP_PATTERN.matcher(host).matches()) {
            return null;
        }
        int index = 0;
        String candidate = host;
        while (index >= 0) {
            index = candidate.indexOf(46);
            final String subCandidate = candidate.substring(index + 1);
            final DomainSuffix d = tlds.get(subCandidate);
            if (d != null) {
                return d;
            }
            candidate = subCandidate;
        }
        return null;
    }
    
    public static DomainSuffix getDomainSuffix(final String url) throws MalformedURLException {
        return getDomainSuffix(new URL(url));
    }
    
    public static String[] getHostSegments(final URL url) {
        final String host = url.getHost();
        if (URLUtil.IP_PATTERN.matcher(host).matches()) {
            return new String[] { host };
        }
        return host.split("\\.");
    }
    
    public static String[] getHostSegments(final String url) throws MalformedURLException {
        return getHostSegments(new URL(url));
    }
    
    public static String chooseRepr(final String src, final String dst, final boolean temp) {
        URL srcUrl;
        URL dstUrl;
        try {
            srcUrl = new URL(src);
            dstUrl = new URL(dst);
        }
        catch (MalformedURLException e) {
            return dst;
        }
        final String srcDomain = getDomainName(srcUrl);
        final String dstDomain = getDomainName(dstUrl);
        final String srcHost = srcUrl.getHost();
        final String dstHost = dstUrl.getHost();
        final String srcFile = srcUrl.getFile();
        final String dstFile = dstUrl.getFile();
        final boolean srcRoot = srcFile.equals("/") || srcFile.length() == 0;
        final boolean destRoot = dstFile.equals("/") || dstFile.length() == 0;
        if (!srcDomain.equals(dstDomain)) {
            return dst;
        }
        if (!temp) {
            if (srcRoot) {
                return src;
            }
            return dst;
        }
        else {
            if (srcRoot && !destRoot) {
                return src;
            }
            if (!srcRoot && destRoot) {
                return dst;
            }
            if (srcRoot || destRoot || !srcHost.equals(dstHost)) {
                final int numSrcSubs = srcHost.split("\\.").length;
                final int numDstSubs = dstHost.split("\\.").length;
                return (numDstSubs < numSrcSubs) ? dst : src;
            }
            final int numSrcPaths = srcFile.split("/").length;
            final int numDstPaths = dstFile.split("/").length;
            if (numSrcPaths != numDstPaths) {
                return (numDstPaths < numSrcPaths) ? dst : src;
            }
            final int srcPathLength = srcFile.length();
            final int dstPathLength = dstFile.length();
            return (dstPathLength < srcPathLength) ? dst : src;
        }
    }
    
    public static String getHost(final String url) {
        try {
            return new URL(url).getHost().toLowerCase();
        }
        catch (MalformedURLException e) {
            return null;
        }
    }
    
    public static String getPage(String url) {
        try {
            url = url.toLowerCase();
            final String queryStr = new URL(url).getQuery();
            return (queryStr != null) ? url.replace("?" + queryStr, "") : url;
        }
        catch (MalformedURLException e) {
            return null;
        }
    }
    
    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("Usage : URLUtil <url>");
            return;
        }
        final String url = args[0];
        try {
            System.out.println(getDomainName(new URL(url)));
        }
        catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
    }
    
    static {
        URLUtil.IP_Pattern = Pattern.compile("((25[0-5])|(2[0-4]\\d)|(1\\d\\d)|([1-9]\\d)|\\d)(\\.((25[0-5])|(2[0-4]\\d)|(1\\d\\d)|([1-9]\\d)|\\d)){3}");
        URLUtil.HOST_pattern = Pattern.compile("[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+\\.?");
        URLUtil.IP_PATTERN = Pattern.compile("(\\d{1,3}\\.){3}(\\d{1,3})");
    }
}

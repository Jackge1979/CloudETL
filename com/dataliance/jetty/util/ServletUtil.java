package com.dataliance.jetty.util;

import java.io.*;
import javax.servlet.*;

import com.dataliance.util.*;

public class ServletUtil
{
    public static final String HTML_TAIL;
    
    public static PrintWriter initHTML(final ServletResponse response, final String title) throws IOException {
        response.setContentType("text/html");
        final PrintWriter out = response.getWriter();
        out.println("<html>\n<link rel='stylesheet' type='text/css' href='/static/hadoop.css'>\n<title>" + title + "</title>\n" + "<body>\n" + "<h1>" + title + "</h1>\n");
        return out;
    }
    
    public static String getParameter(final ServletRequest request, final String name) {
        String s = request.getParameter(name);
        if (s == null) {
            return null;
        }
        s = s.trim();
        return (s.length() == 0) ? null : s;
    }
    
    public static String htmlFooter() {
        return ServletUtil.HTML_TAIL;
    }
    
    public static String percentageGraph(final int perc, final int width) throws IOException {
        assert perc >= 0;
        assert perc <= 100;
        final StringBuilder builder = new StringBuilder();
        builder.append("<table border=\"1px\" width=\"");
        builder.append(width);
        builder.append("px\"><tr>");
        if (perc > 0) {
            builder.append("<td cellspacing=\"0\" class=\"perc_filled\" width=\"");
            builder.append(perc);
            builder.append("%\"></td>");
        }
        if (perc < 100) {
            builder.append("<td cellspacing=\"0\" class=\"perc_nonfilled\" width=\"");
            builder.append(100 - perc);
            builder.append("%\"></td>");
        }
        builder.append("</tr></table>");
        return builder.toString();
    }
    
    public static String percentageGraph(final float perc, final int width) throws IOException {
        return percentageGraph((int)perc, width);
    }
    
    static {
        HTML_TAIL = "<hr />\nThis is <a href='http://hadoop.apache.org/'>Apache Hadoop</a> release " + VersionInfo.getVersion() + "\n" + "</body></html>";
    }
}

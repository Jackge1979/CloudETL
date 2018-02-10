package com.dataliance.etl.inject.http.Servlet;

import javax.servlet.http.*;
import java.util.*;
import net.sf.json.*;
import javax.servlet.*;
import java.io.*;

public class ListSourceServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;
    public static final String SRC_PATHS = "srcpaths";
    
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String[] srcs = req.getParameterValues("srcpaths");
        System.out.println(Arrays.toString(srcs));
        final JSONArray jsons = new JSONArray();
        if (srcs != null) {
            for (final String src : srcs) {
                final File srcFile = new File(src);
                final File[] arr;
                final File[] files = arr = srcFile.listFiles();
                for (final File file : arr) {
                    final JSONObject json = new JSONObject();
                    json.accumulate("path", (Object)file.getAbsolutePath());
                    if (file.isDirectory()) {
                        json.accumulate("isDir", true);
                    }
                    else {
                        json.accumulate("isDir", false);
                        json.accumulate("length", file.length());
                    }
                    jsons.add((Object)json);
                }
            }
        }
        final PrintWriter pw = resp.getWriter();
        pw.write(jsons.toString());
        pw.flush();
    }
    
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }
}

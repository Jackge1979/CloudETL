package com.dataliance.etl.inject.http.Servlet;

import javax.servlet.http.*;

import com.dataliance.util.*;

import javax.servlet.*;
import java.io.*;

public class GetSourceServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;
    private static final String FILE_PATH = "file";
    private static final String START = "start";
    private static final String LIMIT = "limit";
    
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String filePath = req.getParameter("file");
        if (!StringUtil.isEmpty(filePath)) {
            final File file = new File(filePath);
            if (file.exists()) {
                final long length = file.length();
                resp.setHeader("Content-Length", Long.toString(length));
                resp.setHeader("Content-Type", "application/octet-stream");
                final long start = StringUtil.toLong(req.getParameter("start"));
                final long limit = StringUtil.toLong(req.getParameter("limit"));
                final FileInputStream in = new FileInputStream(file);
                final OutputStream out = (OutputStream)resp.getOutputStream();
                if (start <= 0L && limit <= 0L) {
                    StreamUtil.output(in, out);
                }
                else if (start <= 0L && limit >= 0L) {
                    if (limit < length) {
                        StreamUtil.output(in, out, limit);
                    }
                    else {
                        StreamUtil.output(in, out);
                    }
                }
                else if (start >= 0L && limit <= 0L) {
                    StreamUtil.output(start, in, out);
                }
                else if (limit < length) {
                    StreamUtil.output(start, in, out, limit);
                }
                else {
                    StreamUtil.output(start, in, out);
                }
            }
            else {
                resp.setStatus(404);
            }
        }
        else {
            resp.setStatus(404);
        }
    }
    
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }
}

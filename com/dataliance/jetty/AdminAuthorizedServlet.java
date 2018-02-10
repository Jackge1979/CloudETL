package com.dataliance.jetty;

import org.mortbay.jetty.servlet.*;
import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;

public class AdminAuthorizedServlet extends DefaultServlet
{
    private static final long serialVersionUID = 1L;
    
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        if (HttpServer.hasAdministratorAccess(this.getServletContext(), request, response)) {
            super.doGet(request, response);
        }
    }
}

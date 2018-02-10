package com.dataliance.etl.inject.http;

import javax.servlet.http.*;

import com.dataliance.etl.inject.http.Servlet.*;
import com.dataliance.jetty.*;

import java.io.*;

public class HttpServerTest
{
    public static void main(final String[] args) throws IOException {
        final HttpServer server = new HttpServer("test", "0.0.0.0", 0, true);
        server.addServlet("test", "/list", ListSourceServlet.class);
        server.addServlet("test2", "/get", GetSourceServlet.class);
        server.start();
        System.out.println("dd");
    }
}

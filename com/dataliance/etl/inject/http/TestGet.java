package com.dataliance.etl.inject.http;

import java.net.*;

import com.dataliance.util.*;

import java.io.*;

public class TestGet
{
    public static void main(final String[] args) throws IOException {
        final URL url = new URL("http://localhost:48941/list?srcpaths=/");
        final URLConnection conn = url.openConnection();
        conn.setRequestProperty("User-Agent", "NetFox");
        conn.setRequestProperty("RANGE", "bytes=100000");
        final InputStream input = conn.getInputStream();
        StreamUtil.output(input, System.out);
    }
}

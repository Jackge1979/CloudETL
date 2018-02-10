package com.dataliance.vo;

import java.util.*;
import java.io.*;
import java.net.*;

public class URLEntry
{
    private Map<String, String> params;
    private String host;
    private String action;
    private int port;
    
    public URLEntry(final String host, final int port, final String action) {
        this.params = new HashMap<String, String>();
        this.host = host;
        this.port = port;
        this.action = action;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
    
    public String getAction() {
        return this.action;
    }
    
    public void setAction(final String action) {
        this.action = action;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public void setPort(final int port) {
        this.port = port;
    }
    
    public URLEntry addParams(final String key, final Object value) {
        if (value != null) {
            this.params.put(key, value.toString());
        }
        return this;
    }
    
    public void remove(final String key) {
        this.params.remove(key);
    }
    
    public String toStr() throws IOException {
        final StringBuffer sb = new StringBuffer();
        sb.append("http://").append(this.host).append(":").append(this.port).append("/");
        sb.append(this.action).append("?");
        for (final Map.Entry<String, String> entry : this.params.entrySet()) {
            sb.append(entry.getKey()).append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8")).append("&");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
    
    public URL toURL() throws IOException {
        return new URL(this.toStr());
    }
    
    public static void main(final String[] args) throws IOException {
        final URLEntry uu = new URLEntry("localhost", 8080, "get");
        uu.addParams("start", "333").addParams("end", "88");
        System.out.println(uu.toURL());
    }
}

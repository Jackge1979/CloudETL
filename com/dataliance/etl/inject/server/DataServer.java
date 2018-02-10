package com.dataliance.etl.inject.server;

import org.apache.hadoop.conf.*;
import javax.servlet.http.*;

import java.net.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import com.dataliance.service.util.*;
import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.etl.vo.http.*;
import com.dataliance.etl.inject.http.Servlet.*;
import com.dataliance.etl.inject.rpc.*;
import com.dataliance.jetty.*;
import com.dataliance.util.*;

public class DataServer extends AbstractServer
{
    public static final String PROTOCOL = "http";
    public static final String STOP_FILE_DATA = ".stop_data";
    private SourceClient client;
    private HttpServer httpServer;
    private int httpPort;
    private String hostName;
    private FileSystem fs;
    private String programID;
    
    public DataServer(final Configuration conf, final String master, final int port, final String programID) throws IOException {
        super(conf);
        this.hostName = InetAddress.getLocalHost().getHostName();
        this.programID = programID;
        (this.httpServer = new HttpServer("dataServer", "0.0.0.0", 0, true)).addServlet("dataServer", "/list", ListSourceServlet.class);
        this.httpServer.addServlet("dataServer", "/get", GetSourceServlet.class);
        this.httpServer.start();
        this.fs = FileSystem.get(conf);
        this.httpPort = this.httpServer.getPort();
        this.client = (SourceClient)RPC.getProxy((Class)SourceClient.class, 1L, new InetSocketAddress(master, port), conf);
    }
    
    public void doRegist() throws IOException {
        final Text host = new Text(this.hostName);
        final DataHost dataHost = this.client.regist(host, new IntWritable(this.httpPort));
        if (dataHost != null) {
            final StopListener stop = new StopListener(dataHost.getProgramPath(), 10000L);
            stop.start();
            final HttpDataSource dataSource = this.parseDataHost(dataHost);
            this.client.list(host, dataSource);
        }
    }
    
    private HttpDataSource parseDataHost(final DataHost dataHost) throws IOException {
        final HttpDataSource dataSource = new HttpDataSource();
        dataSource.setHost(dataHost.getHost());
        dataSource.setUser(dataHost.getUser());
        dataSource.setPassword(dataHost.getPassword());
        dataSource.setPort(this.httpPort);
        dataSource.setProtocol("http");
        for (final DirSource dirSource : dataHost.getSources()) {
            final File file = new File(dirSource.getSrc());
            if (file.exists()) {
                if (file.isDirectory()) {
                    this.parseData(dataSource, file, dirSource.getDest());
                }
                else {
                    final DataImpl data = new DataImpl();
                    data.setDestDir(dirSource.getDest());
                    data.setDir(false);
                    data.setLength(file.length());
                    data.setSrc(file.getAbsolutePath());
                    data.setDatahost(dataHost.getHost());
                    data.setStatus((byte)0);
                    dataSource.addData(data);
                }
            }
            else {
                this.client.log(new Text(this.hostName), new Text(file.getAbsolutePath() + " is not exists!"));
            }
        }
        return dataSource;
    }
    
    private void parseData(final HttpDataSource dataSource, final File dir, final String destDir) throws IOException {
        final File[] arr$;
        final File[] files = arr$ = dir.listFiles();
        for (final File file : arr$) {
            if (file.isDirectory()) {
                final Path path = new Path(destDir, file.getName());
                if (!this.fs.exists(path)) {
                    this.fs.mkdirs(path);
                }
                this.parseData(dataSource, file, path.toString());
            }
            else {
                final DataImpl data = new DataImpl();
                data.setDatahost(dataSource.getHost());
                data.setDestDir(destDir);
                data.setDir(false);
                data.setLength(file.length());
                data.setSrc(file.getAbsolutePath());
                data.setStatus((byte)0);
                dataSource.addData(data);
            }
        }
    }
    
    public void finish(final Text host) throws Exception {
        this.client.finish(host);
        this.httpServer.stop();
    }
    
    public static void main(final String[] args) throws IOException {
        final String usAge = "Usage : ImportServer <master> <port> <programID>";
        if (args.length < 3) {
            System.err.println(usAge);
            return;
        }
        final Configuration conf = ConfigUtils.getConfig();
        final String master = args[0];
        final int port = StringUtil.toInt(args[1]);
        final String programID = args[2];
        final DataServer ds = new DataServer(conf, master, port, programID);
        ds.doRegist();
    }
    
    class StopListener extends Thread
    {
        private final long delay;
        private final String path;
        
        StopListener(final String path, final long delay) {
            this.path = path;
            this.delay = delay;
        }
        
        @Override
        public void run() {
            final File file = new File(this.path, ".stop_data/" + DataServer.this.programID);
            while (!file.exists()) {
                try {
                    Thread.sleep(this.delay);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            file.deleteOnExit();
            try {
                DataServer.this.httpServer.stop();
            }
            catch (Exception e2) {
                e2.printStackTrace();
            }
            System.exit(0);
        }
    }
}

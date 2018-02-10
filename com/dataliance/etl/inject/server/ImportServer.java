package com.dataliance.etl.inject.server;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.io.*;

import java.net.*;

import com.dataliance.etl.inject.etl.*;
import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.rpc.*;
import com.dataliance.util.*;
import com.dataliance.vo.*;

import org.apache.hadoop.fs.*;
import java.io.*;
import com.dataliance.service.util.*;

import org.slf4j.*;

public class ImportServer extends AbstractServer implements Importer
{
    private static final Logger LOG;
    public static final FloatWritable NOT_UPLOAD;
    public static final String STOP_FILE_IMPORT = ".stop_import";
    public static final int SIZE = 5;
    public static final String PARAM_ACTION = "get";
    public static final String PARAM_FILE = "file";
    public static final String PARAM_START = "start";
    public static final String PARAM_LIMIT = "limit";
    public static final String SPLIT = "-";
    private ImportClient client;
    private String host;
    private String programID;
    private FileSystem fs;
    private File stopFile;
    
    public ImportServer(final Configuration conf, final String master, final int port, final String programID) throws IOException {
        super(conf);
        this.host = InetAddress.getLocalHost().getHostName();
        this.programID = programID;
        this.fs = FileSystem.get(conf);
        this.client = (ImportClient)RPC.getProxy((Class)ImportClient.class, 1L, new InetSocketAddress(master, port), conf);
    }
    
    @Override
    public void doImport() throws IOException {
        final Text h = new Text(this.host);
        ImportVO importVO = this.client.regist(h);
        this.stopFile = new File(importVO.getProgramPath(), ".stop_import/" + this.programID);
        while (!importVO.isNull()) {
            ImportServer.LOG.info(importVO.toString());
            if (this.parse(importVO)) {
                this.client.finish(h, importVO);
            }
            else {
                this.client.error(h, importVO);
            }
            if (this.stopFile.exists()) {
                break;
            }
            importVO = this.client.next(h);
        }
        this.stopFile.delete();
        if (this.stopFile.exists()) {
            this.stopFile.deleteOnExit();
        }
        ImportServer.LOG.info("Will stop import server host = " + this.host);
    }
    
    private URL parseURL(final ImportVO ivo) throws IOException {
        final URLEntry uu = new URLEntry(ivo.getHost(), ivo.getPort(), "get");
        uu.addParams("file", ivo.getSource());
        if (ivo.isSplit()) {
            uu.addParams("start", ivo.getStart());
            uu.addParams("limit", ivo.getLimit());
        }
        return uu.toURL();
    }
    
    private boolean parse(final ImportVO ivo) throws IOException {
        final URL url = this.parseURL(ivo);
        final String name = new File(ivo.getSource()).getName();
        Path outPath = null;
        if (ivo.isSplit()) {
            outPath = new Path(ivo.getDestDir(), name + "-" + NumFormat.suffixByZero(ivo.getStart(), 5));
        }
        else {
            outPath = new Path(ivo.getDestDir(), name);
        }
        if (this.fs.exists(outPath)) {
            final String msg = "Output path '" + outPath + "' is exists ! Will not upload host = " + ivo.getHost() + " path = " + ivo.getSource();
            ImportServer.LOG.warn(msg);
            this.client.report(new Text(this.host), new Text(msg), ImportServer.NOT_UPLOAD);
            return false;
        }
        try {
            final OutputStream out = (OutputStream)this.fs.create(outPath);
            StreamUtil.out(url, out);
            return true;
        }
        catch (Exception e) {
            ImportServer.LOG.error(e.getMessage(), (Throwable)e);
            return false;
        }
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
        final ImportServer is = new ImportServer(conf, master, port, programID);
        is.doImport();
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)ImportServer.class);
        NOT_UPLOAD = new FloatWritable(-1.0f);
    }
}

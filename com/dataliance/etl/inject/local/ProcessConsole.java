package com.dataliance.etl.inject.local;

import java.io.*;
import org.slf4j.*;

public class ProcessConsole extends Thread
{
    public static final Logger LOG;
    InputStream fin;
    String type;
    OutputStream fout;
    
    public ProcessConsole(final InputStream fin, final String type) {
        this(fin, type, null);
    }
    
    public ProcessConsole(final InputStream fin, final String type, final OutputStream fout) {
        this.fin = fin;
        this.type = type;
        this.fout = fout;
    }
    
    @Override
    public void run() {
        PrintStream pw = null;
        BufferedReader reader = null;
        try {
            if (this.fout != null) {
                pw = new PrintStream(this.fout, true);
            }
            else if (this.type.equalsIgnoreCase("err")) {
                pw = System.err;
            }
            else {
                pw = System.out;
            }
            reader = new BufferedReader(new InputStreamReader(this.fin));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (pw != null) {
                    pw.println(line);
                }
                else {
                    ProcessConsole.LOG.info(this.type + ">" + line);
                }
            }
        }
        catch (IOException e) {
            ProcessConsole.LOG.error(e.getMessage(), (Throwable)e);
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {}
            }
            if (pw != null) {
                pw.flush();
                pw.close();
            }
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)ProcessConsole.class);
    }
}

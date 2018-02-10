package com.dataliance.util;

import java.io.*;
import java.util.zip.*;
import org.apache.commons.logging.*;

public class GZIPUtils
{
    private static final Log LOG;
    private static final int EXPECTED_COMPRESSION_RATIO = 5;
    private static final int BUF_SIZE = 4096;
    
    public static final byte[] unzipBestEffort(final byte[] in) {
        return unzipBestEffort(in, Integer.MAX_VALUE);
    }
    
    public static final byte[] unzipBestEffort(final byte[] in, final int sizeLimit) {
        try {
            final ByteArrayOutputStream outStream = new ByteArrayOutputStream(5 * in.length);
            final GZIPInputStream inStream = new GZIPInputStream(new ByteArrayInputStream(in));
            final byte[] buf = new byte[4096];
            int written = 0;
            try {
                while (true) {
                    final int size = inStream.read(buf);
                    if (size <= 0) {
                        break;
                    }
                    if (written + size > sizeLimit) {
                        outStream.write(buf, 0, sizeLimit - written);
                        break;
                    }
                    outStream.write(buf, 0, size);
                    written += size;
                }
            }
            catch (Exception e) {}
            try {
                outStream.close();
            }
            catch (IOException ex) {}
            return outStream.toByteArray();
        }
        catch (IOException e2) {
            return null;
        }
    }
    
    public static final void unzipBestEffort(final InputStream in, final OutputStream out) {
        try {
            final GZIPInputStream inStream = new GZIPInputStream(in);
            final byte[] buf = new byte[4096];
            try {
                while (true) {
                    final int size = inStream.read(buf);
                    if (size <= 0) {
                        break;
                    }
                    out.write(buf, 0, size);
                }
            }
            catch (Exception e2) {}
            try {
                out.close();
            }
            catch (IOException ex) {}
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static final byte[] unzip(final byte[] in) throws IOException {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream(5 * in.length);
        final GZIPInputStream inStream = new GZIPInputStream(new ByteArrayInputStream(in));
        final byte[] buf = new byte[4096];
        while (true) {
            final int size = inStream.read(buf);
            if (size <= 0) {
                break;
            }
            outStream.write(buf, 0, size);
        }
        outStream.close();
        return outStream.toByteArray();
    }
    
    public static final byte[] zip(final byte[] in) {
        try {
            final ByteArrayOutputStream byteOut = new ByteArrayOutputStream(in.length / 5);
            final GZIPOutputStream outStream = new GZIPOutputStream(byteOut);
            try {
                outStream.write(in);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            try {
                outStream.close();
            }
            catch (IOException e2) {
                e2.printStackTrace();
            }
            return byteOut.toByteArray();
        }
        catch (IOException e3) {
            e3.printStackTrace();
            return null;
        }
    }
    
    static {
        LOG = LogFactory.getLog((Class)GZIPUtils.class);
    }
}

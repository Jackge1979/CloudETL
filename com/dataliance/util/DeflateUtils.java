package com.dataliance.util;

import java.util.zip.*;
import java.io.*;
import org.apache.commons.logging.*;

public class DeflateUtils
{
    private static final Log LOG;
    private static final int EXPECTED_COMPRESSION_RATIO = 5;
    private static final int BUF_SIZE = 4096;
    
    public static final byte[] inflateBestEffort(final byte[] in) {
        return inflateBestEffort(in, Integer.MAX_VALUE);
    }
    
    public static final byte[] inflateBestEffort(final byte[] in, final int sizeLimit) {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream(5 * in.length);
        final Inflater inflater = new Inflater(true);
        final InflaterInputStream inStream = new InflaterInputStream(new ByteArrayInputStream(in), inflater);
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
    
    public static final byte[] inflate(final byte[] in) throws IOException {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream(5 * in.length);
        final InflaterInputStream inStream = new InflaterInputStream(new ByteArrayInputStream(in));
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
    
    public static final byte[] deflate(final byte[] in) {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream(in.length / 5);
        final DeflaterOutputStream outStream = new DeflaterOutputStream(byteOut);
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
    
    static {
        LOG = LogFactory.getLog((Class)DeflateUtils.class);
    }
}

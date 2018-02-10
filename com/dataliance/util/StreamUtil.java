package com.dataliance.util;

import java.util.regex.*;
import java.net.*;
import org.xml.sax.*;
import com.dataliance.core.util.*;
import java.nio.charset.*;
import java.io.*;

public class StreamUtil
{
    private static final String USER_HOME = "user.home";
    private static final String USER_NAME = "user.name";
    public static final Pattern pattern;
    public static final int BUFFER_SIZE = 2048;
    
    public static File getHome() {
        return new File(System.getProperty("user.home"));
    }
    
    public static String getUser() {
        return System.getProperty("user.name");
    }
    
    public static byte[] getByte(final InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        output(in, out);
        return out.toByteArray();
    }
    
    public static byte[] getByte(final URL url) throws IOException {
        final URLConnection urlc = url.openConnection();
        final byte[] content = getByte(urlc.getInputStream());
        return parseContent(content, urlc.getContentEncoding());
    }
    
    public static InputSource getInputSource(final URL url) throws IOException {
        final byte[] content = getByte(url);
        if (content != null) {
            return new InputSource(new ByteArrayInputStream(content));
        }
        return null;
    }
    
    public static byte[] getByte(final String url) throws IOException {
        return getByte(new URL(url));
    }
    
    public static void output(final byte[] content, final OutputStream out) throws IOException {
        output(new ByteArrayInputStream(content), out);
    }
    
    public static void out(final URL url, final OutputStream... out) throws IOException {
        final InputStream in = url.openStream();
        final byte[] b = new byte[2048];
        int n;
        while ((n = in.read(b)) != -1) {
            for (final OutputStream o : out) {
                o.write(b, 0, n);
            }
        }
        in.close();
        for (final OutputStream o : out) {
            o.flush();
            o.close();
        }
    }
    
    public static void output(final InputStream in, final OutputStream out) throws IOException {
        output(in, out, true);
    }
    
    public static void output(final InputStream in, final OutputStream out, final boolean closeOut) throws IOException {
        output(in, out, closeOut, true);
    }
    
    public static void output(final InputStream in, final OutputStream out, final boolean closeOut, final boolean closeIn) throws IOException {
        final byte[] b = new byte[2048];
        int n;
        while ((n = in.read(b)) != -1) {
            out.write(b, 0, n);
        }
        out.flush();
        if (closeIn) {
            in.close();
        }
        if (closeOut) {
            out.close();
        }
    }
    
    public static void output(final long start, final InputStream in, final OutputStream out, final long limit) throws IOException {
        in.skip(start);
        final byte[] b = new byte[2048];
        long l = 0L;
        int n;
        while ((n = in.read(b)) != -1) {
            l += n;
            if (l < limit) {
                out.write(b, 0, n);
            }
            else {
                out.write(b, 0, (int)(limit - (l - n)));
            }
        }
        out.flush();
        in.close();
        out.close();
    }
    
    public static void output(final InputStream in, final OutputStream out, final long limit) throws IOException {
        final byte[] b = new byte[2048];
        long l = 0L;
        int n;
        while ((n = in.read(b)) != -1) {
            l += n;
            if (l < limit) {
                out.write(b, 0, n);
            }
            else {
                out.write(b, 0, (int)(limit - (l - n)));
            }
        }
        out.flush();
        in.close();
        out.close();
    }
    
    public static void output(final long start, final InputStream in, final OutputStream out) throws IOException {
        in.skip(start);
        final byte[] b = new byte[2048];
        int n;
        while ((n = in.read(b)) != -1) {
            out.write(b, 0, n);
        }
        out.flush();
        in.close();
        out.close();
    }
    
    public static void output(final String content, final OutputStream out) throws IOException {
        output(new ByteArrayInputStream(content.getBytes()), out);
    }
    
    public static void output(final String content, final File out) throws IOException {
        output(new ByteArrayInputStream(content.getBytes()), out);
    }
    
    public static void output(final InputStream in, final File out) throws IOException {
        if (!out.getParentFile().exists()) {
            out.getParentFile().mkdirs();
        }
        output(in, new FileOutputStream(out));
    }
    
    public static void output(final URL url, final OutputStream out) throws IOException {
        final InputStream in = url.openStream();
        output(in, out);
    }
    
    public static String output(final URL url, final File out) throws IOException {
        final URLConnection uconn = url.openConnection();
        final InputStream in = uconn.getInputStream();
        final String file = uconn.getURL().getPath();
        if (!out.getParentFile().exists()) {
            out.getParentFile().mkdirs();
        }
        output(in, new FileOutputStream(out));
        if (file != null) {
            final int index = file.lastIndexOf(46);
            return (index > 0) ? file.substring(index) : file;
        }
        return null;
    }
    
    public static String getContnet(final File file) throws IOException {
        final InputStream in = new BufferedInputStream(new FileInputStream(file));
        Charset charset = DomUtil.getFileCharacterEnding(in);
        if (charset.name().equalsIgnoreCase("windows-1252")) {
            charset = Charset.forName("gbk");
        }
        return new String(getByte(new FileInputStream(file)), charset);
    }
    
    public static void output(final Object inputSource, final OutputStream out) throws IOException {
        if (inputSource instanceof InputStream) {
            output((InputStream)inputSource, out);
        }
        else if (inputSource instanceof byte[]) {
            output((byte[])inputSource, out);
        }
        else if (inputSource instanceof URL) {
            output((URL)inputSource, out);
        }
        else {
            if (!(inputSource instanceof String)) {
                throw new IOException("Don't support this source");
            }
            output((String)inputSource, out);
        }
    }
    
    public static byte[] parseContent(final byte[] content, final String contentEncoding) throws IOException {
        if (content == null) {
            return null;
        }
        if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
            return processGzipEncoded(content);
        }
        if ("deflate".equals(contentEncoding)) {
            return processDeflateEncoded(content);
        }
        return content;
    }
    
    public static byte[] processGzipEncoded(final byte[] compressed) throws IOException {
        final byte[] content = GZIPUtils.unzipBestEffort(compressed);
        if (content == null) {
            throw new IOException("unzipBestEffort returned null");
        }
        return content;
    }
    
    public static byte[] processDeflateEncoded(final byte[] compressed) throws IOException {
        final byte[] content = DeflateUtils.inflateBestEffort(compressed);
        if (content == null) {
            throw new IOException("inflateBestEffort returned null");
        }
        return content;
    }
    
    public static boolean isImage(final String url) {
        try {
            final URL u = new URL(url);
            final URLConnection uc = u.openConnection();
            final String contentType = uc.getContentType();
            return contentType.startsWith("image");
        }
        catch (Exception e) {
            return false;
        }
    }
    
    public static BufferedReader getBufferedReader(final File file) throws IOException {
        return getBufferedReader(new FileInputStream(file));
    }
    
    public static BufferedReader getBufferedReader(final InputStream in) throws IOException {
        return getBufferedReader(in, "utf-8");
    }
    
    public static BufferedReader getBufferedReader(final File file, final String encode) throws IOException {
        return getBufferedReader(new FileInputStream(file), encode);
    }
    
    public static BufferedReader getBufferedReader(final InputStream in, final String encode) throws IOException {
        return new BufferedReader(new InputStreamReader(in, encode));
    }
    
    public static BufferedReader getBufferedReader(final String file) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    }
    
    public static String getPriKeyPath() {
        final File file = getPriKeyFile();
        if (file != null) {
            return file.toString();
        }
        return null;
    }
    
    public static File getPriKeyFile() {
        final String userHome = System.getProperty("user.home");
        final File sshHome = new File(userHome, ".ssh");
        if (sshHome.exists()) {
            final File rsaFile = new File(sshHome, "id_rsa");
            if (rsaFile.exists()) {
                return rsaFile;
            }
            final File[] priFiles = sshHome.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(final File dir, final String name) {
                    return name.startsWith("id_") && !name.endsWith(".pub");
                }
            });
            if (priFiles != null && priFiles.length > 0) {
                return priFiles[0];
            }
        }
        return null;
    }
    
    public static File getTmpDir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }
    
    static {
        pattern = Pattern.compile("attachment;.*filename=(.+)", 2);
    }
}

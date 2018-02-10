package com.dataliance.core.util;

import java.nio.charset.*;
import net.sf.json.*;
import org.apache.html.dom.*;
import com.sun.org.apache.xml.internal.utils.*;
import org.w3c.dom.*;
import org.ccil.cowan.tagsoup.*;
import org.xml.sax.*;
import javax.xml.parsers.*;
import java.util.regex.*;
import com.dataliance.dom.protocol.*;

import org.apache.hadoop.conf.*;
import java.net.*;
import java.io.*;
import org.apache.commons.httpclient.*;

import com.dataliance.util.*;
import com.dataliance.dom.meta.*;
import info.monitorenter.cpdetector.io.*;

public class DomUtil
{
    private static final int CHUNK_SIZE = 2000;
    private static final Pattern metaPattern;
    private static final Pattern charsetPattern;
    private static CodepageDetectorProxy detector;
    
    public static Charset getFileCharacterEnding(final InputStream ios) {
        final String fileCharacterEnding = "utf-8";
        Charset charset = null;
        try {
            charset = DomUtil.detector.detectCodepage(ios, ios.available());
        }
        catch (IllegalArgumentException e) {
            LogUtil.error(DomUtil.class, e.getMessage());
        }
        catch (IOException e2) {
            LogUtil.error(DomUtil.class, e2.getMessage());
        }
        finally {
            if (ios != null) {
                try {
                    ios.close();
                }
                catch (IOException e3) {
                    LogUtil.error(DomUtil.class, e3.getMessage());
                }
            }
        }
        if (charset != null) {
            return charset;
        }
        return Charset.forName(fileCharacterEnding);
    }
    
    public static JSONArray getJsonArray(final String encode, final byte[] content) throws IOException {
        try {
            return JSONArray.fromObject((Object)new String(content, encode));
        }
        catch (JSONException e) {
            LogUtil.error(DomUtil.class, e.getMessage());
            return null;
        }
    }
    
    public static JSONObject getJSONObject(final String encode, final byte[] content) throws IOException {
        try {
            return JSONObject.fromObject((Object)new String(content, encode));
        }
        catch (JSONException e) {
            LogUtil.error(DomUtil.class, e.getMessage());
            return null;
        }
    }
    
    public static DocumentFragment getDom(final String encode, final byte[] content) throws IOException {
        final InputSource input = new InputSource(new ByteArrayInputStream(content));
        DocumentFragment root = null;
        input.setEncoding(encode);
        try {
            final HTMLDocumentImpl doc = new HTMLDocumentImpl();
            root = doc.createDocumentFragment();
            final DOMBuilder builder = new DOMBuilder((Document)doc, root);
            final Parser reader = new Parser();
            reader.setContentHandler((ContentHandler)builder);
            reader.setFeature("http://www.ccil.org/~cowan/tagsoup/features/ignore-bogons", false);
            reader.setFeature("http://www.ccil.org/~cowan/tagsoup/features/bogons-empty", false);
            reader.setProperty("http://xml.org/sax/properties/lexical-handler", (Object)builder);
            reader.parse(input);
            return root;
        }
        catch (Exception e) {
            LogUtil.error(DomUtil.class, e.getMessage());
            return null;
        }
    }
    
    public static Document getXML(final String encode, final InputStream in) throws IOException {
        final InputSource input = new InputSource(in);
        Document root = null;
        input.setEncoding(encode);
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            final DocumentBuilder builder = factory.newDocumentBuilder();
            root = builder.parse(input);
        }
        catch (Exception e) {
            LogUtil.error(DomUtil.class, e.getMessage());
        }
        return root;
    }
    
    public static Document getXML(final String encode, final byte[] content) throws IOException {
        return getXML(encode, new ByteArrayInputStream(content));
    }
    
    private static String sniffCharacterEncoding(final byte[] content) {
        final int length = (content.length < 2000) ? content.length : 2000;
        String str = "";
        try {
            str = new String(content, 0, length, Charset.forName("ASCII").toString());
        }
        catch (UnsupportedEncodingException e) {
            LogUtil.error(DomUtil.class, e.getMessage());
            return null;
        }
        final Matcher metaMatcher = DomUtil.metaPattern.matcher(str);
        String encoding = null;
        if (metaMatcher.find()) {
            final Matcher charsetMatcher = DomUtil.charsetPattern.matcher(metaMatcher.group(1));
            if (charsetMatcher.find()) {
                encoding = new String(charsetMatcher.group(1));
            }
        }
        return encoding;
    }
    
    private static String getEncodeing(final String contentType, final URL url, final byte[] content) {
        if (!StringUtil.isEmpty(contentType)) {
            final int m = contentType.indexOf("charset=");
            if (m != -1) {
                return contentType.substring(m + 8).replace("]", "");
            }
        }
        String encode = null;
        if (content != null) {
            encode = sniffCharacterEncoding(content);
            if (encode != null) {
                return encode;
            }
        }
        Charset charset = null;
        try {
            charset = DomUtil.detector.detectCodepage((InputStream)new ByteArrayInputStream(content), content.length);
        }
        catch (Exception e) {
            LogUtil.error(DomUtil.class, e.getMessage());
        }
        if (charset != null) {
            encode = charset.name();
        }
        else {
            try {
                charset = DomUtil.detector.detectCodepage(url);
            }
            catch (IOException e2) {
                LogUtil.error(DomUtil.class, e2.getMessage());
            }
            if (charset != null) {
                encode = charset.name();
            }
        }
        if (encode == null) {
            encode = "utf-8";
        }
        return encode;
    }
    
    public static Content getContentByFile(final File file) throws IOException {
        final byte[] content = StreamUtil.getByte(new FileInputStream(file));
        final String encode = getEncodeing("", file.toURI().toURL(), content);
        final Metadata headers = new Metadata();
        headers.set("Location", file.getCanonicalFile().toURI().toURL().toString());
        headers.set("Content-Length", Long.toString(file.getTotalSpace()));
        headers.set("Last-Modified", HttpDateFormat.toString(file.lastModified()));
        return new Content(getXML(encode, content), headers);
    }
    
    private static String getAgentString() throws IOException {
        final Configuration conf = DAConfigUtil.create();
        final String agentName = conf.get("http.agent.name", "Mobier-weibo");
        final String agentVersion = conf.get("http.agent.version", "1.0");
        final String agentDesc = conf.get("http.agent.description", "mobier weibo sousuo");
        final String agentURL = conf.get("http.agent.url", "http://mobier.mobi/");
        final String agentEmail = conf.get("http.agent.email", "service@mobier.mobi");
        final StringBuffer buf = new StringBuffer();
        buf.append(agentName);
        if (agentVersion != null) {
            buf.append("/");
            buf.append(agentVersion);
        }
        if ((agentDesc != null && agentDesc.length() != 0) || (agentEmail != null && agentEmail.length() != 0) || (agentURL != null && agentURL.length() != 0)) {
            buf.append(" (");
            if (agentDesc != null && agentDesc.length() != 0) {
                buf.append(agentDesc);
                if (agentURL != null || agentEmail != null) {
                    buf.append("; ");
                }
            }
            if (agentURL != null && agentURL.length() != 0) {
                buf.append(agentURL);
                if (agentEmail != null) {
                    buf.append("; ");
                }
            }
            if (agentEmail != null && agentEmail.length() != 0) {
                buf.append(agentEmail);
            }
            buf.append(")");
        }
        return buf.toString();
    }
    
    public static Content getContent(final URL url) throws IOException {
        final String protocol = url.getProtocol();
        if (!StringUtil.isEmpty(protocol) && protocol.equals("file")) {
            try {
                return getContentByFile(new File(url.toURI()));
            }
            catch (URISyntaxException e) {
                final File file = new File(url.toString());
                if (file.exists()) {
                    return getContentByFile(file);
                }
                return null;
            }
        }
        final String path = "".equals(url.getFile()) ? "/" : url.getFile();
        final String host = url.getHost();
        int port;
        String portString;
        if (url.getPort() == -1) {
            port = 80;
            portString = "";
        }
        else {
            port = url.getPort();
            portString = ":" + port;
        }
        Socket socket = null;
        try {
            socket = new Socket();
            socket.setSoTimeout(10000);
            final String sockHost = host;
            final int sockPort = port;
            final StringBuffer reqStr = new StringBuffer("GET ");
            reqStr.append(path);
            reqStr.append(" HTTP/1.0\r\n");
            reqStr.append("Host: ");
            reqStr.append(host);
            reqStr.append(portString);
            reqStr.append("\r\n");
            reqStr.append("Accept-Encoding: x-gzip, gzip, deflate\r\n");
            reqStr.append("Accept-Language: zh-cn,zh;q=0.5\r\n");
            reqStr.append("Accept-Charset: GB2312,utf-8;q=0.7,*;q=0.7\r\n");
            reqStr.append("Accept: application/xhtml+xml,text/html,application/xml;q=0.9,*/*;q=0.8\r\n");
            reqStr.append("User-Agent: ");
            reqStr.append(getAgentString());
            reqStr.append("\r\n");
            reqStr.append("\r\n");
            final byte[] reqBytes = reqStr.toString().getBytes();
            final InetSocketAddress sockAddr = new InetSocketAddress(sockHost, sockPort);
            socket.connect(sockAddr, 5000);
            final OutputStream req = socket.getOutputStream();
            req.write(reqBytes);
            req.flush();
            final PushbackInputStream in = new PushbackInputStream(new BufferedInputStream(socket.getInputStream(), 32768), 32768);
            final StringBuffer line = new StringBuffer();
            boolean haveSeenNonContinueStatus = false;
            Metadata headers = null;
            while (!haveSeenNonContinueStatus) {
                final int code = parseStatusLine(in, line);
                headers = parseHeaders(in, line);
                haveSeenNonContinueStatus = (code != 100);
            }
            final String contentType = headers.get("Content-Type");
            final String contentEncoding = headers.get("Content-Encoding");
            byte[] content = readContent(in, headers);
            in.close();
            content = StreamUtil.parseContent(content, contentEncoding);
            final String encode = getEncodeing(contentType, url, content);
            headers.set("OriginalCharEncoding", encode);
            headers.set("CharEncodingForConversion", encode);
            if (!StringUtil.isEmpty(contentType) && (contentType.toLowerCase().contains("json") || contentType.toLowerCase().contains("plain"))) {
                return new Content(getJsonArray(encode, content), headers);
            }
            if (!StringUtil.isEmpty(contentType) && contentType.toLowerCase().contains("html")) {
                return new Content(getDom(encode, content), headers);
            }
            return new Content(getXML(encode, content), headers);
        }
        finally {
            if (socket != null) {
                socket.close();
            }
        }
    }
    
    private static byte[] readContent(final InputStream in, final Metadata headers) throws HttpException, IOException {
        int contentLength = 0;
        String contentLengthString = headers.get("Content-Length");
        ByteArrayOutputStream out;
        if (contentLengthString != null) {
            contentLengthString = contentLengthString.trim();
            contentLength = Integer.parseInt(contentLengthString);
            out = new ByteArrayOutputStream(contentLength);
        }
        else {
            out = new ByteArrayOutputStream();
        }
        final byte[] bytes = new byte[32768];
        for (int i = in.read(bytes); i != -1; i = in.read(bytes)) {
            out.write(bytes, 0, i);
        }
        out.flush();
        return out.toByteArray();
    }
    
    private static Metadata parseHeaders(final PushbackInputStream in, final StringBuffer line) throws IOException {
        final Metadata headers = new SpellCheckedMetadata();
        while (readLine(in, line, true) != 0) {
            int pos;
            if ((pos = line.indexOf("<!DOCTYPE")) != -1 || (pos = line.indexOf("<HTML")) != -1 || (pos = line.indexOf("<html")) != -1) {
                in.unread(line.substring(pos).getBytes("UTF-8"));
                line.setLength(pos);
                try {
                    processHeaderLine(line, headers);
                }
                catch (Exception e) {
                    LogUtil.error(DomUtil.class, e.getMessage());
                }
                break;
            }
            processHeaderLine(line, headers);
        }
        return headers;
    }
    
    private static void processHeaderLine(final StringBuffer line, final Metadata headers) throws IOException {
        final int colonIndex = line.indexOf(":");
        if (colonIndex == -1) {
            int i;
            for (i = 0; i < line.length() && Character.isWhitespace(line.charAt(i)); ++i) {}
            if (i == line.length()) {
                return;
            }
        }
        final String key = line.substring(0, colonIndex);
        int valueStart;
        for (valueStart = colonIndex + 1; valueStart < line.length(); ++valueStart) {
            final int c = line.charAt(valueStart);
            if (c != 32 && c != 9) {
                break;
            }
        }
        final String value = line.substring(valueStart);
        headers.add(key, value);
    }
    
    private static int parseStatusLine(final PushbackInputStream in, final StringBuffer line) throws IOException {
        final int l = readLine(in, line, false);
        if (l != 0) {
            final int codeStart = line.indexOf(" ");
            int codeEnd = line.indexOf(" ", codeStart + 1);
            if (codeEnd == -1) {
                codeEnd = line.length();
            }
            int c = 0;
            try {
                c = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
            }
            catch (NumberFormatException e) {
                LogUtil.error(DomUtil.class, e.getMessage());
            }
            return c;
        }
        return 600;
    }
    
    private static int readLine(final PushbackInputStream in, final StringBuffer line, final boolean allowContinuedLine) throws IOException {
        line.setLength(0);
        for (int c = in.read(); c != -1; c = in.read()) {
            switch (c) {
                case 13: {
                    if (peek(in) == 10) {
                        in.read();
                    }
                }
                case 10: {
                    if (line.length() > 0 && allowContinuedLine) {
                        switch (peek(in)) {
                            case 9:
                            case 32: {
                                in.read();
                                continue;
                            }
                        }
                    }
                    return line.length();
                }
                default: {
                    line.append((char)c);
                    break;
                }
            }
        }
        return 0;
    }
    
    private static int peek(final PushbackInputStream in) throws IOException {
        final int value = in.read();
        in.unread(value);
        return value;
    }
    
    static {
        metaPattern = Pattern.compile("<meta\\s+([^>]*http-equiv=\"?content-type\"?[^>]*)>", 2);
        charsetPattern = Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)", 2);
        (DomUtil.detector = CodepageDetectorProxy.getInstance()).add((ICodepageDetector)new HTMLCodepageDetector(false));
        DomUtil.detector.add((ICodepageDetector)JChardetFacade.getInstance());
        DomUtil.detector.add((ICodepageDetector)new ParsingDetector(false));
        DomUtil.detector.add(ASCIIDetector.getInstance());
        DomUtil.detector.add(UnicodeDetector.getInstance());
    }
}

package com.dataliance.jetty;

import org.apache.hadoop.security.authorize.*;
import org.mortbay.jetty.webapp.*;
import org.apache.hadoop.conf.*;
import org.mortbay.thread.*;
import org.mortbay.jetty.handler.*;
import org.mortbay.jetty.nio.*;
import org.mortbay.jetty.*;
import org.apache.hadoop.log.*;
import org.apache.hadoop.jmx.*;

import com.dataliance.jetty.security.*;
import com.sun.jersey.spi.container.servlet.*;
import org.apache.hadoop.security.*;
import org.mortbay.jetty.servlet.*;
import org.mortbay.jetty.security.*;
import java.net.*;
import org.mortbay.util.*;
import org.apache.commons.logging.*;
import java.io.*;
import org.apache.hadoop.util.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.util.*;

public class HttpServer implements FilterContainer
{
    public static final Log LOG;
    static final String FILTER_INITIALIZER_PROPERTY = "hadoop.http.filter.initializers";
    static final String CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";
    static final String ADMINS_ACL = "admins.acl";
    private AccessControlList adminsAcl;
    protected final Server webServer;
    protected final Connector listener;
    protected final WebAppContext webAppContext;
    protected final boolean findPort;
    protected final Map<Context, Boolean> defaultContexts;
    protected final List<String> filterNames;
    private static final int MAX_RETRIES = 10;
    private final Configuration conf;
    private boolean listenerStartedExternally;
    
    public HttpServer(final String name, final String bindAddress, final int port, final boolean findPort) throws IOException {
        this(name, bindAddress, port, findPort, new Configuration());
    }
    
    public HttpServer(final String name, final String bindAddress, final int port, final boolean findPort, final Configuration conf) throws IOException {
        this(name, bindAddress, port, findPort, conf, null, null);
    }
    
    public HttpServer(final String name, final String bindAddress, final int port, final boolean findPort, final Configuration conf, final Connector connector) throws IOException {
        this(name, bindAddress, port, findPort, conf, null, connector);
    }
    
    public HttpServer(final String name, final String bindAddress, final int port, final boolean findPort, final Configuration conf, final AccessControlList adminsAcl) throws IOException {
        this(name, bindAddress, port, findPort, conf, adminsAcl, null);
    }
    
    public HttpServer(final String name, final String bindAddress, final int port, final boolean findPort, final Configuration conf, final AccessControlList adminsAcl, final Connector connector) throws IOException {
        this.defaultContexts = new HashMap<Context, Boolean>();
        this.filterNames = new ArrayList<String>();
        this.listenerStartedExternally = false;
        this.webServer = new Server();
        this.findPort = findPort;
        this.conf = conf;
        this.adminsAcl = adminsAcl;
        if (connector == null) {
            this.listenerStartedExternally = false;
            (this.listener = this.createBaseListener(conf)).setHost(bindAddress);
            this.listener.setPort(port);
        }
        else {
            this.listenerStartedExternally = true;
            this.listener = connector;
        }
        this.webServer.addConnector(this.listener);
        this.webServer.setThreadPool((ThreadPool)new QueuedThreadPool());
        final String appDir = this.getWebAppsPath();
        final ContextHandlerCollection contexts = new ContextHandlerCollection();
        this.webServer.setHandler((Handler)contexts);
        (this.webAppContext = new WebAppContext()).setDisplayName("WepAppsContext");
        this.webAppContext.setContextPath("/");
        this.webAppContext.setWar(appDir + "/" + name);
        this.webAppContext.getServletContext().setAttribute("hadoop.conf", (Object)conf);
        this.webAppContext.getServletContext().setAttribute("admins.acl", (Object)adminsAcl);
        this.webServer.addHandler((Handler)this.webAppContext);
        this.addDefaultApps(contexts, appDir);
        this.defineFilter((Context)this.webAppContext, "krb5Filter", Krb5AndCertsSslSocketConnector.Krb5SslFilter.class.getName(), null, null);
        this.addGlobalFilter("safety", QuotingInputFilter.class.getName(), null);
        this.addDefaultServlets();
    }
    
    public Connector createBaseListener(final Configuration conf) throws IOException {
        return createDefaultChannelConnector();
    }
    
    public static Connector createDefaultChannelConnector() {
        final SelectChannelConnector ret = new SelectChannelConnector();
        ret.setLowResourceMaxIdleTime(10000);
        ret.setAcceptQueueSize(128);
        ret.setResolveNames(false);
        ret.setUseDirectBuffers(false);
        return (Connector)ret;
    }
    
    protected void addDefaultApps(final ContextHandlerCollection parent, final String appDir) throws IOException {
        final String logDir = System.getProperty("hadoop.log.dir");
        if (logDir != null) {
            final Context logContext = new Context((HandlerContainer)parent, "/logs");
            logContext.setResourceBase(logDir);
            logContext.addServlet((Class)AdminAuthorizedServlet.class, "/");
            logContext.setDisplayName("logs");
            this.setContextAttributes(logContext);
            this.defaultContexts.put(logContext, true);
        }
        final Context staticContext = new Context((HandlerContainer)parent, "/static");
        staticContext.setResourceBase(appDir + "/static");
        staticContext.addServlet((Class)DefaultServlet.class, "/*");
        staticContext.setDisplayName("static");
        this.setContextAttributes(staticContext);
        this.defaultContexts.put(staticContext, true);
    }
    
    private void setContextAttributes(final Context context) {
        context.getServletContext().setAttribute("hadoop.conf", (Object)this.conf);
        context.getServletContext().setAttribute("admins.acl", (Object)this.adminsAcl);
    }
    
    protected void addDefaultServlets() {
        this.addServlet("stacks", "/stacks", StackServlet.class);
        this.addServlet("logLevel", "/logLevel", (Class<? extends HttpServlet>)LogLevel.Servlet.class);
        this.addServlet("jmx", "/jmx", (Class<? extends HttpServlet>)JMXJsonServlet.class);
    }
    
    public void addContext(final Context ctxt, final boolean isFiltered) throws IOException {
        this.webServer.addHandler((Handler)ctxt);
        this.defaultContexts.put(ctxt, isFiltered);
    }
    
    protected void addContext(final String pathSpec, final String dir, final boolean isFiltered) throws IOException {
        if (0 == this.webServer.getHandlers().length) {
            throw new RuntimeException("Couldn't find handler");
        }
        final WebAppContext webAppCtx = new WebAppContext();
        webAppCtx.setContextPath(pathSpec);
        webAppCtx.setWar(dir);
        this.addContext((Context)webAppCtx, true);
    }
    
    public void setAttribute(final String name, final Object value) {
        this.setAttribute((Context)this.webAppContext, name, value);
    }
    
    public void setAttribute(final Context context, final String name, final Object value) {
        context.setAttribute(name, value);
    }
    
    public void addJerseyResourcePackage(final String packageName, final String pathSpec) {
        HttpServer.LOG.info((Object)("addJerseyResourcePackage: packageName=" + packageName + ", pathSpec=" + pathSpec));
        final ServletHolder sh = new ServletHolder((Class)ServletContainer.class);
        sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig");
        sh.setInitParameter("com.sun.jersey.config.property.packages", packageName);
        this.webAppContext.addServlet(sh, pathSpec);
    }
    
    public void addServlet(final String name, final String pathSpec, final Class<? extends HttpServlet> clazz) {
        this.addInternalServlet(name, pathSpec, clazz, false);
        this.addFilterPathMapping(pathSpec, (Context)this.webAppContext);
    }
    
    @Deprecated
    public void addInternalServlet(final String name, final String pathSpec, final Class<? extends HttpServlet> clazz) {
        this.addInternalServlet(name, pathSpec, clazz, false);
    }
    
    public void addInternalServlet(final String name, final String pathSpec, final Class<? extends HttpServlet> clazz, final boolean requireAuth) {
        final ServletHolder holder = new ServletHolder((Class)clazz);
        if (name != null) {
            holder.setName(name);
        }
        this.webAppContext.addServlet(holder, pathSpec);
        if (requireAuth && UserGroupInformation.isSecurityEnabled()) {
            HttpServer.LOG.info((Object)("Adding Kerberos filter to " + name));
            final ServletHandler handler = this.webAppContext.getServletHandler();
            final FilterMapping fmap = new FilterMapping();
            fmap.setPathSpec(pathSpec);
            fmap.setFilterName("krb5Filter");
            fmap.setDispatches(15);
            handler.addFilterMapping(fmap);
        }
    }
    
    @Override
    public void addFilter(final String name, final String classname, final Map<String, String> parameters) {
        final String[] USER_FACING_URLS = { "*.html", "*.jsp" };
        this.defineFilter((Context)this.webAppContext, name, classname, parameters, USER_FACING_URLS);
        HttpServer.LOG.info((Object)("Added filter " + name + " (class=" + classname + ") to context " + this.webAppContext.getDisplayName()));
        final String[] ALL_URLS = { "/*" };
        for (final Map.Entry<Context, Boolean> e : this.defaultContexts.entrySet()) {
            if (e.getValue()) {
                final Context ctx = e.getKey();
                this.defineFilter(ctx, name, classname, parameters, ALL_URLS);
                HttpServer.LOG.info((Object)("Added filter " + name + " (class=" + classname + ") to context " + ctx.getDisplayName()));
            }
        }
        this.filterNames.add(name);
    }
    
    @Override
    public void addGlobalFilter(final String name, final String classname, final Map<String, String> parameters) {
        final String[] ALL_URLS = { "/*" };
        this.defineFilter((Context)this.webAppContext, name, classname, parameters, ALL_URLS);
        for (final Context ctx : this.defaultContexts.keySet()) {
            this.defineFilter(ctx, name, classname, parameters, ALL_URLS);
        }
        HttpServer.LOG.info((Object)("Added global filter" + name + " (class=" + classname + ")"));
    }
    
    protected void defineFilter(final Context ctx, final String name, final String classname, final Map<String, String> parameters, final String[] urls) {
        final FilterHolder holder = new FilterHolder();
        holder.setName(name);
        holder.setClassName(classname);
        holder.setInitParameters((Map)parameters);
        final FilterMapping fmap = new FilterMapping();
        fmap.setPathSpecs(urls);
        fmap.setDispatches(15);
        fmap.setFilterName(name);
        final ServletHandler handler = ctx.getServletHandler();
        handler.addFilter(holder, fmap);
    }
    
    protected void addFilterPathMapping(final String pathSpec, final Context webAppCtx) {
        final ServletHandler handler = webAppCtx.getServletHandler();
        for (final String name : this.filterNames) {
            final FilterMapping fmap = new FilterMapping();
            fmap.setPathSpec(pathSpec);
            fmap.setFilterName(name);
            fmap.setDispatches(15);
            handler.addFilterMapping(fmap);
        }
    }
    
    public Object getAttribute(final String name) {
        return this.webAppContext.getAttribute(name);
    }
    
    protected String getWebAppsPath() throws IOException {
        final URL url = this.getClass().getClassLoader().getResource("webapps");
        if (url == null) {
            throw new IOException("webapps not found in CLASSPATH");
        }
        return url.toString();
    }
    
    public int getPort() {
        return this.webServer.getConnectors()[0].getLocalPort();
    }
    
    public void setThreads(final int min, final int max) {
        final QueuedThreadPool pool = (QueuedThreadPool)this.webServer.getThreadPool();
        pool.setMinThreads(min);
        pool.setMaxThreads(max);
    }
    
    @Deprecated
    public void addSslListener(final InetSocketAddress addr, final String keystore, final String storPass, final String keyPass) throws IOException {
        if (this.webServer.isStarted()) {
            throw new IOException("Failed to add ssl listener");
        }
        final SslSocketConnector sslListener = new SslSocketConnector();
        sslListener.setHost(addr.getHostName());
        sslListener.setPort(addr.getPort());
        sslListener.setKeystore(keystore);
        sslListener.setPassword(storPass);
        sslListener.setKeyPassword(keyPass);
        this.webServer.addConnector((Connector)sslListener);
    }
    
    public void addSslListener(final InetSocketAddress addr, final Configuration sslConf, final boolean needClientAuth) throws IOException {
        this.addSslListener(addr, sslConf, needClientAuth, false);
    }
    
    public void addSslListener(final InetSocketAddress addr, final Configuration sslConf, final boolean needCertsAuth, final boolean needKrbAuth) throws IOException {
        if (this.webServer.isStarted()) {
            throw new IOException("Failed to add ssl listener");
        }
        if (needCertsAuth) {
            System.setProperty("javax.net.ssl.trustStore", sslConf.get("ssl.server.truststore.location", ""));
            System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get("ssl.server.truststore.password", ""));
            System.setProperty("javax.net.ssl.trustStoreType", sslConf.get("ssl.server.truststore.type", "jks"));
        }
        Krb5AndCertsSslSocketConnector.MODE mode;
        if (needCertsAuth && needKrbAuth) {
            mode = Krb5AndCertsSslSocketConnector.MODE.BOTH;
        }
        else if (!needCertsAuth && needKrbAuth) {
            mode = Krb5AndCertsSslSocketConnector.MODE.KRB;
        }
        else {
            mode = Krb5AndCertsSslSocketConnector.MODE.CERTS;
        }
        final SslSocketConnector sslListener = new Krb5AndCertsSslSocketConnector(mode);
        sslListener.setHost(addr.getHostName());
        sslListener.setPort(addr.getPort());
        sslListener.setKeystore(sslConf.get("ssl.server.keystore.location"));
        sslListener.setPassword(sslConf.get("ssl.server.keystore.password", ""));
        sslListener.setKeyPassword(sslConf.get("ssl.server.keystore.keypassword", ""));
        sslListener.setKeystoreType(sslConf.get("ssl.server.keystore.type", "jks"));
        sslListener.setNeedClientAuth(needCertsAuth);
        this.webServer.addConnector((Connector)sslListener);
    }
    
    public void start() throws IOException {
        try {
            if (this.listenerStartedExternally) {
                if (this.listener.getLocalPort() == -1) {
                    throw new Exception("Exepected webserver's listener to be startedpreviously but wasn't");
                }
                this.webServer.start();
            }
            else {
                int port = 0;
                int oriPort = this.listener.getPort();
                while (true) {
                    try {
                        port = this.webServer.getConnectors()[0].getLocalPort();
                        HttpServer.LOG.info((Object)("Port returned by webServer.getConnectors()[0].getLocalPort() before open() is " + port + ". Opening the listener on " + oriPort));
                        this.listener.open();
                        port = this.listener.getLocalPort();
                        HttpServer.LOG.info((Object)("listener.getLocalPort() returned " + this.listener.getLocalPort() + " webServer.getConnectors()[0].getLocalPort() returned " + this.webServer.getConnectors()[0].getLocalPort()));
                        if (port < 0) {
                            Thread.sleep(100L);
                            int numRetries = 1;
                            while (port < 0) {
                                HttpServer.LOG.warn((Object)("listener.getLocalPort returned " + port));
                                if (numRetries++ > 10) {
                                    throw new Exception(" listener.getLocalPort is returning less than 0 even after " + numRetries + " resets");
                                }
                                for (int i = 0; i < 2; ++i) {
                                    HttpServer.LOG.info((Object)"Retrying listener.getLocalPort()");
                                    port = this.listener.getLocalPort();
                                    if (port > 0) {
                                        break;
                                    }
                                    Thread.sleep(200L);
                                }
                                if (port > 0) {
                                    break;
                                }
                                HttpServer.LOG.info((Object)"Bouncing the listener");
                                this.listener.close();
                                Thread.sleep(1000L);
                                this.listener.setPort((oriPort == 0) ? 0 : (++oriPort));
                                this.listener.open();
                                Thread.sleep(100L);
                                port = this.listener.getLocalPort();
                            }
                        }
                        HttpServer.LOG.info((Object)("Jetty bound to port " + port));
                        this.webServer.start();
                        break;
                    }
                    catch (IOException ex) {
                        if (!(ex instanceof BindException)) {
                            HttpServer.LOG.info((Object)"HttpServer.start() threw a non Bind IOException");
                            throw ex;
                        }
                        if (!this.findPort) {
                            throw (BindException)ex;
                        }
                    }
                    catch (MultiException ex2) {
                        HttpServer.LOG.info((Object)"HttpServer.start() threw a MultiException");
                        throw ex2;
                    }
                    this.listener.setPort(++oriPort);
                }
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e2) {
            throw new IOException("Problem starting http server", e2);
        }
    }
    
    public void stop() throws Exception {
        this.listener.close();
        this.webServer.stop();
    }
    
    public void join() throws InterruptedException {
        this.webServer.join();
    }
    
    public static boolean hasAdministratorAccess(final ServletContext servletContext, final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        final Configuration conf = (Configuration)servletContext.getAttribute("hadoop.conf");
        if (!conf.getBoolean("hadoop.security.authorization", false)) {
            return true;
        }
        final String remoteUser = request.getRemoteUser();
        if (remoteUser == null) {
            return true;
        }
        final AccessControlList adminsAcl = (AccessControlList)servletContext.getAttribute("admins.acl");
        final UserGroupInformation remoteUserUGI = UserGroupInformation.createRemoteUser(remoteUser);
        if (adminsAcl != null && !adminsAcl.isUserAllowed(remoteUserUGI)) {
            response.sendError(401, "User " + remoteUser + " is unauthorized to access this page. " + "AccessControlList for accessing this page : " + adminsAcl.toString());
            return false;
        }
        return true;
    }
    
    static {
        LOG = LogFactory.getLog((Class)HttpServer.class);
    }
    
    public static class StackServlet extends HttpServlet
    {
        private static final long serialVersionUID = -6284183679759467039L;
        
        public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
            if (!HttpServer.hasAdministratorAccess(this.getServletContext(), request, response)) {
                return;
            }
            final PrintWriter out = new PrintWriter(HtmlQuoting.quoteOutputStream((OutputStream)response.getOutputStream()));
            ReflectionUtils.printThreadInfo(out, "");
            out.close();
            ReflectionUtils.logThreadInfo(HttpServer.LOG, "jsp requested", 1L);
        }
    }
    
    public static class QuotingInputFilter implements Filter
    {
        public void init(final FilterConfig config) throws ServletException {
        }
        
        public void destroy() {
        }
        
        public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
            final HttpServletRequestWrapper quoted = new RequestQuoter((HttpServletRequest)request);
            final HttpServletResponse httpResponse = (HttpServletResponse)response;
            httpResponse.setContentType("text/html;charset=utf-8");
            chain.doFilter((ServletRequest)quoted, response);
        }
        
        public static class RequestQuoter extends HttpServletRequestWrapper
        {
            private final HttpServletRequest rawRequest;
            
            public RequestQuoter(final HttpServletRequest rawRequest) {
                super(rawRequest);
                this.rawRequest = rawRequest;
            }
            
            public Enumeration<String> getParameterNames() {
                return new Enumeration<String>() {
                    private Enumeration<String> rawIterator = RequestQuoter.this.rawRequest.getParameterNames();
                    
                    @Override
                    public boolean hasMoreElements() {
                        return this.rawIterator.hasMoreElements();
                    }
                    
                    @Override
                    public String nextElement() {
                        return HtmlQuoting.quoteHtmlChars(this.rawIterator.nextElement());
                    }
                };
            }
            
            public String getParameter(final String name) {
                return HtmlQuoting.quoteHtmlChars(this.rawRequest.getParameter(HtmlQuoting.unquoteHtmlChars(name)));
            }
            
            public String[] getParameterValues(final String name) {
                final String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
                final String[] unquoteValue = this.rawRequest.getParameterValues(unquoteName);
                final String[] result = new String[unquoteValue.length];
                for (int i = 0; i < result.length; ++i) {
                    result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
                }
                return result;
            }
            
            public Map<String, String[]> getParameterMap() {
                final Map<String, String[]> result = new HashMap<String, String[]>();
                final Map<String, String[]> raw = (Map<String, String[]>)this.rawRequest.getParameterMap();
                for (final Map.Entry<String, String[]> item : raw.entrySet()) {
                    final String[] rawValue = item.getValue();
                    final String[] cookedValue = new String[rawValue.length];
                    for (int i = 0; i < rawValue.length; ++i) {
                        cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
                    }
                    result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
                }
                return result;
            }
            
            public StringBuffer getRequestURL() {
                final String url = this.rawRequest.getRequestURL().toString();
                return new StringBuffer(HtmlQuoting.quoteHtmlChars(url));
            }
            
            public String getServerName() {
                return HtmlQuoting.quoteHtmlChars(this.rawRequest.getServerName());
            }
        }
    }
}

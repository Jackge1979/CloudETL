package com.dataliance.jetty.security;

import java.net.*;
import java.io.*;
import org.mortbay.io.*;
import org.mortbay.jetty.*;
import javax.net.ssl.*;
import org.mortbay.jetty.security.*;
import java.security.*;
import java.util.*;
import org.apache.commons.logging.*;
import javax.security.auth.kerberos.*;
import javax.servlet.http.*;
import javax.servlet.*;

public class Krb5AndCertsSslSocketConnector extends SslSocketConnector
{
    public static final List<String> KRB5_CIPHER_SUITES;
    private static final Log LOG;
    private static final String REMOTE_PRINCIPAL = "remote_principal";
    private final boolean useKrb;
    private final boolean useCerts;
    
    public Krb5AndCertsSslSocketConnector() {
        this.useKrb = true;
        this.useCerts = false;
        this.setPasswords();
    }
    
    public Krb5AndCertsSslSocketConnector(final MODE mode) {
        this.useKrb = (mode == MODE.KRB || mode == MODE.BOTH);
        this.useCerts = (mode == MODE.CERTS || mode == MODE.BOTH);
        this.setPasswords();
        this.logIfDebug("useKerb = " + this.useKrb + ", useCerts = " + this.useCerts);
    }
    
    private void setPasswords() {
        if (!this.useCerts) {
            final Random r = new Random();
            System.setProperty("jetty.ssl.password", String.valueOf(r.nextLong()));
            System.setProperty("jetty.ssl.keypassword", String.valueOf(r.nextLong()));
        }
    }
    
    protected SSLServerSocketFactory createFactory() throws Exception {
        if (this.useCerts) {
            return super.createFactory();
        }
        final SSLContext context = (super.getProvider() == null) ? SSLContext.getInstance(super.getProtocol()) : SSLContext.getInstance(super.getProtocol(), super.getProvider());
        context.init(null, null, null);
        return context.getServerSocketFactory();
    }
    
    protected ServerSocket newServerSocket(final String host, final int port, final int backlog) throws IOException {
        this.logIfDebug("Creating new KrbServerSocket for: " + host);
        SSLServerSocket ss = null;
        if (this.useCerts) {
            ss = (SSLServerSocket)super.newServerSocket(host, port, backlog);
        }
        else {
            try {
                ss = (SSLServerSocket)((host == null) ? this.createFactory().createServerSocket(port, backlog) : this.createFactory().createServerSocket(port, backlog, InetAddress.getByName(host)));
            }
            catch (Exception e) {
                Krb5AndCertsSslSocketConnector.LOG.warn((Object)"Could not create KRB5 Listener", (Throwable)e);
                throw new IOException("Could not create KRB5 Listener: " + e.toString());
            }
        }
        if (this.useKrb) {
            ss.setNeedClientAuth(true);
            String[] combined;
            if (this.useCerts) {
                final String[] certs = ss.getEnabledCipherSuites();
                combined = new String[certs.length + Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.size()];
                System.arraycopy(certs, 0, combined, 0, certs.length);
                System.arraycopy(Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.toArray(new String[0]), 0, combined, certs.length, Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.size());
            }
            else {
                combined = Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.toArray(new String[0]);
            }
            ss.setEnabledCipherSuites(combined);
        }
        return ss;
    }
    
    public void customize(final EndPoint endpoint, final Request request) throws IOException {
        if (this.useKrb) {
            final SSLSocket sslSocket = (SSLSocket)endpoint.getTransport();
            final Principal remotePrincipal = sslSocket.getSession().getPeerPrincipal();
            this.logIfDebug("Remote principal = " + remotePrincipal);
            request.setScheme("https");
            request.setAttribute("remote_principal", (Object)remotePrincipal);
            if (!this.useCerts) {
                final String cipherSuite = sslSocket.getSession().getCipherSuite();
                final Integer keySize = ServletSSL.deduceKeyLength(cipherSuite);
                request.setAttribute("javax.servlet.request.cipher_suite", (Object)cipherSuite);
                request.setAttribute("javax.servlet.request.key_size", (Object)keySize);
            }
        }
        if (this.useCerts) {
            super.customize(endpoint, request);
        }
    }
    
    private void logIfDebug(final String s) {
        if (Krb5AndCertsSslSocketConnector.LOG.isDebugEnabled()) {
            Krb5AndCertsSslSocketConnector.LOG.debug((Object)s);
        }
    }
    
    static {
        KRB5_CIPHER_SUITES = Collections.unmodifiableList((List<? extends String>)Collections.singletonList((T)"TLS_KRB5_WITH_3DES_EDE_CBC_SHA"));
        System.setProperty("https.cipherSuites", Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.get(0));
        LOG = LogFactory.getLog((Class)Krb5AndCertsSslSocketConnector.class);
    }
    
    public enum MODE
    {
        KRB, 
        CERTS, 
        BOTH;
    }
    
    public static class Krb5SslFilter implements Filter
    {
        public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain) throws IOException, ServletException {
            final Principal princ = (Principal)req.getAttribute("remote_principal");
            if (princ == null || !(princ instanceof KerberosPrincipal)) {
                Krb5AndCertsSslSocketConnector.LOG.warn((Object)("User not authenticated via kerberos from " + req.getRemoteAddr()));
                ((HttpServletResponse)resp).sendError(403, "User not authenticated via Kerberos");
                return;
            }
            final ServletRequest wrapper = (ServletRequest)new HttpServletRequestWrapper((HttpServletRequest)req) {
                public Principal getUserPrincipal() {
                    return princ;
                }
                
                public String getRemoteUser() {
                    return princ.getName();
                }
            };
            chain.doFilter(wrapper, resp);
        }
        
        public void init(final FilterConfig arg0) throws ServletException {
        }
        
        public void destroy() {
        }
    }
}

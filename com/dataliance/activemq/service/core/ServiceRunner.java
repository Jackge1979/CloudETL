package com.dataliance.activemq.service.core;

import java.util.concurrent.atomic.*;
import java.util.*;
import javax.naming.*;
import javax.jms.*;
import java.io.*;
import com.dataliance.service.util.*;
import org.apache.commons.logging.*;

public class ServiceRunner implements MessageListener
{
    private static Log LOG;
    protected QueueConnection qConnection;
    protected QueueSession qSession;
    protected Queue requestQueue;
    private QueueReceiver qReceiver;
    private Context context;
    private final String queueConnFactoryName = "service.qcf";
    private Service service;
    private String serviceName;
    private final AtomicBoolean isRunning;
    
    public ServiceRunner() {
        this.qConnection = null;
        this.qSession = null;
        this.requestQueue = null;
        this.qReceiver = null;
        this.context = null;
        this.service = null;
        this.serviceName = null;
        this.isRunning = new AtomicBoolean(false);
    }
    
    public void run(final String clsName, final String messageContent) throws ServiceRunningException {
        try {
            this.newServiceInstance(clsName);
            if (null == messageContent || messageContent.length() == 0) {
                this.serveForever();
            }
            else {
                final JobInfo jobInfo = JobInfoParser.parse(messageContent, clsName);
                ServiceRunner.LOG.info((Object)String.format("start %s with message '%s'", this.serviceName, messageContent));
                this.runService(this.service, jobInfo);
                ServiceRunner.LOG.info((Object)String.format("finish %s with message '%s'", this.serviceName, messageContent));
            }
        }
        catch (Exception e) {
            throw new ServiceRunningException(e);
        }
    }
    
    private void newServiceInstance(final String clsName) {
        try {
            final Class cls = Class.forName(clsName);
            this.service = cls.newInstance();
            this.serviceName = parseServiceNameFromClsName(clsName);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("can not create instance for " + clsName);
        }
    }
    
    private void runService(final Service service, final JobInfo jobInfo) throws ServiceRunningException {
        try {
            service.serve(jobInfo.jobArgs);
        }
        catch (Exception e) {
            throw new ServiceRunningException(e);
        }
    }
    
    public boolean shutDown() {
        this.finish();
        return true;
    }
    
    private synchronized void finish() {
        ServiceRunner.LOG.info((Object)"Finish the service, close the queue connection.");
        try {
            if (this.qReceiver != null) {
                this.qReceiver.close();
                this.qReceiver = null;
            }
            if (this.qSession != null) {
                this.qSession.close();
                this.qSession = null;
            }
            if (this.qConnection != null) {
                this.qConnection.close();
                this.qConnection = null;
            }
            if (this.context != null) {
                this.context.close();
                this.context = null;
            }
            this.service.hookOnExit();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void serveForever() throws ServiceRunningException {
        this.initMessageQueue();
        ServiceRunner.LOG.info((Object)"Service running...");
        this.isRunning.set(true);
        while (this.isRunning.get()) {
            try {
                synchronized (this.isRunning) {
                    this.isRunning.wait(0L);
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.finish();
    }
    
    private void createQConnection() throws NamingException, JMSException, IOException {
        final Properties jndiProperties = JndiPropertyUtil.getBaseJndiProperties();
        this.context = new InitialContext(jndiProperties);
        final QueueConnectionFactory qFactory = (QueueConnectionFactory)this.context.lookup("service.qcf");
        this.qConnection = qFactory.createQueueConnection();
    }
    
    protected void initMessageQueue() throws ServiceRunningException {
        ServiceRunner.LOG.info((Object)"Start to init the message queue...");
        try {
            this.createQConnection();
            this.qSession = this.qConnection.createQueueSession(false, 1);
            this.requestQueue = (Queue)this.context.lookup(this.serviceName);
            this.qConnection.start();
            (this.qReceiver = this.qSession.createReceiver(this.requestQueue)).setMessageListener((MessageListener)this);
            ServiceRunner.LOG.info((Object)"Waiting for service requests...");
        }
        catch (Exception e) {
            throw new ServiceRunningException("Failed to init the message queue connection, caused by : " + e.toString());
        }
        ServiceRunner.LOG.info((Object)"Succeed to init the message queue.");
    }
    
    public void onMessage(final Message message) {
        try {
            ServiceRunner.LOG.info((Object)MessageUtil.dumpMessage(message));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (this.shouldStopServe(message)) {
                message.acknowledge();
                this.stopServe();
                return;
            }
            final JobInfo jobInfo = JobInfoParser.parse(message);
            System.out.println(String.format("============new job===========\n%s\n", jobInfo.toString()));
            this.runService(this.service, jobInfo);
        }
        catch (Exception e) {
            ServiceRunner.LOG.error((Object)"service\u51fa\u73b0\u5f02\u5e38", (Throwable)e);
        }
    }
    
    public static String parseServiceNameFromClsName(String clsName) {
        if (null == clsName) {
            return "";
        }
        final int lastIndex = clsName.lastIndexOf(46);
        clsName = clsName.substring(lastIndex + 1);
        return clsName;
    }
    
    private boolean shouldStopServe(final Message message) {
        if (message instanceof TextMessage) {
            try {
                return ((TextMessage)message).getText().trim().equalsIgnoreCase("stop");
            }
            catch (JMSException e) {
                throw new RuntimeException((Throwable)e);
            }
        }
        return false;
    }
    
    private void stopServe() throws IOException {
        ServiceRunner.LOG.info((Object)"Stop the service by the message.");
        final File stopFile = new File("stop");
        if (!stopFile.exists()) {
            stopFile.createNewFile();
        }
        this.isRunning.set(false);
        synchronized (this.isRunning) {
            this.isRunning.notifyAll();
        }
        this.finish();
    }
    
    public static void main(final String[] args) {
        if (args.length < 1) {
            System.out.println("arg count : " + args.length);
            for (int i = 0; i < args.length; ++i) {
                System.out.println(String.format("arg %d : %s", i + 1, args[i]));
            }
            System.out.println("Usage : ServiceRunner <class name>  <message string>");
            System.exit(1);
        }
        final String clsName = args[0];
        final StringBuffer messageBuffer = new StringBuffer();
        for (int argv = 1; argv < args.length; ++argv) {
            messageBuffer.append(args[argv]);
        }
        System.out.println("use service : " + clsName);
        try {
            final ServiceRunner runner = new ServiceRunner();
            runner.run(clsName, messageBuffer.toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("exit service in java");
        ThreadTracer.printAllThread();
        System.exit(0);
    }
    
    static {
        ServiceRunner.LOG = LogFactory.getLog(ServiceRunner.class.getCanonicalName());
    }
}

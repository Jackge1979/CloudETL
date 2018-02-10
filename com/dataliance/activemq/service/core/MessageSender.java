package com.dataliance.activemq.service.core;

import com.dataliance.service.util.*;
import javax.jms.*;
import javax.naming.*;
import java.util.*;

public class MessageSender
{
    private static QueueConnection qConnection;
    private static Context context;
    private static final MessageSender MESSAGE_SENDER;
    
    public static final MessageSender get() {
        return MessageSender.MESSAGE_SENDER;
    }
    
    private void createQConnection() {
        if (MessageSender.qConnection != null) {
            return;
        }
        try {
            final Properties jndiProperties = JndiPropertyUtil.getBaseJndiProperties();
            MessageSender.context = new InitialContext(jndiProperties);
            final QueueConnectionFactory qFactory = (QueueConnectionFactory)MessageSender.context.lookup("service.qcf");
            (MessageSender.qConnection = qFactory.createQueueConnection()).start();
        }
        catch (Exception e) {
            throw new RuntimeException("failed to create the connection!");
        }
    }
    
    public boolean sendMessage(final String serviceName, final Map<String, String> argnames2values) {
        final JobInfo jobInfo = buildJobInfo(serviceName, argnames2values);
        QueueSession qSession = null;
        QueueSender qSender = null;
        String messageText = null;
        try {
            messageText = JobInfoParser.buildMessageText(jobInfo);
            if (null == MessageSender.qConnection) {
                this.createQConnection();
            }
            qSession = MessageSender.qConnection.createQueueSession(false, 1);
            final Queue sendQueue = (Queue)MessageSender.context.lookup(serviceName);
            qSender = qSession.createSender(sendQueue);
            final TextMessage message = qSession.createTextMessage(messageText);
            qSender.send((Message)message);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println(String.format("failed to send message [%s] to %s", messageText, serviceName));
            return false;
        }
        finally {
            try {
                if (qSender != null) {
                    qSender.close();
                }
                if (qSession != null) {
                    qSession.close();
                }
            }
            catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        System.out.println(String.format("succeed to send message [%s] to %s", messageText, serviceName));
        return true;
    }
    
    public void shutdown() {
        if (MessageSender.qConnection != null) {
            try {
                MessageSender.qConnection.close();
            }
            catch (JMSException e) {
                e.printStackTrace();
            }
            MessageSender.qConnection = null;
        }
        if (MessageSender.context != null) {
            try {
                MessageSender.context.close();
            }
            catch (NamingException e2) {
                e2.printStackTrace();
            }
            MessageSender.context = null;
        }
    }
    
    private static final JobInfo buildJobInfo(final String serviceName, final Map<String, String> argnames2values) {
        final JobInfo jobInfo = new JobInfo();
        jobInfo.jobArgs = argnames2values;
        jobInfo.jobId = argnames2values.get("job_id");
        if (null == jobInfo.jobId) {
            jobInfo.jobId = JobInfoParser.generateJobId(serviceName);
        }
        return jobInfo;
    }
    
    public void finalize() {
        this.shutdown();
    }
    
    public static void main(final String[] args) {
        final Map<String, String> argnames2values = new HashMap<String, String>();
        argnames2values.put("key", "test");
        get().sendMessage("DummyService", argnames2values);
        get().shutdown();
    }
    
    static {
        MessageSender.qConnection = null;
        MessageSender.context = null;
        MESSAGE_SENDER = new MessageSender();
    }
}

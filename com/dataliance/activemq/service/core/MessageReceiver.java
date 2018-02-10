package com.dataliance.activemq.service.core;

import com.dataliance.service.util.*;
import javax.jms.*;
import javax.naming.*;
import java.util.*;

public class MessageReceiver
{
    private static QueueConnection qConnection;
    private static Context context;
    private static final MessageReceiver MESSAGE_RECEIVER;
    
    public static final MessageReceiver get() {
        return MessageReceiver.MESSAGE_RECEIVER;
    }
    
    private void createQConnection() {
        if (MessageReceiver.qConnection != null) {
            return;
        }
        try {
            final Properties jndiProperties = JndiPropertyUtil.getBaseJndiProperties();
            MessageReceiver.context = new InitialContext(jndiProperties);
            final QueueConnectionFactory qFactory = (QueueConnectionFactory)MessageReceiver.context.lookup("service.qcf");
            (MessageReceiver.qConnection = qFactory.createQueueConnection()).start();
        }
        catch (Exception e) {
            throw new RuntimeException("failed to create the connection!");
        }
    }
    
    public Map<String, String> receiveMessage(final String serviceName) {
        Map<String, String> argnames2values = new HashMap<String, String>();
        QueueSession qSession = null;
        QueueReceiver qReceiver = null;
        try {
            if (null == MessageReceiver.qConnection) {
                this.createQConnection();
            }
            qSession = MessageReceiver.qConnection.createQueueSession(false, 1);
            final Queue receiveQueue = (Queue)MessageReceiver.context.lookup(serviceName);
            qReceiver = qSession.createReceiver(receiveQueue);
            final Message message = qReceiver.receive(2000L);
            if (message != null) {
                argnames2values = JobInfoParser.parse(message).jobArgs;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println("failed to receive message. ");
            try {
                if (qReceiver != null) {
                    qReceiver.close();
                }
                if (qSession != null) {
                    qSession.close();
                }
            }
            catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        finally {
            try {
                if (qReceiver != null) {
                    qReceiver.close();
                }
                if (qSession != null) {
                    qSession.close();
                }
            }
            catch (Exception e3) {
                e3.printStackTrace();
            }
        }
        System.out.println("succeed to receive message.");
        return argnames2values;
    }
    
    public void shutdown() {
        if (MessageReceiver.qConnection != null) {
            try {
                MessageReceiver.qConnection.close();
            }
            catch (JMSException e) {
                e.printStackTrace();
            }
            MessageReceiver.qConnection = null;
        }
        if (MessageReceiver.context != null) {
            try {
                MessageReceiver.context.close();
            }
            catch (NamingException e2) {
                e2.printStackTrace();
            }
            MessageReceiver.context = null;
        }
    }
    
    public void finalize() {
        this.shutdown();
    }
    
    public static void main(final String[] args) {
        final Map<String, String> argName2value = get().receiveMessage("DummyService");
        get().shutdown();
        for (final Map.Entry<String, String> entry : argName2value.entrySet()) {
            System.err.println(entry.getKey() + ":" + entry.getValue());
        }
    }
    
    static {
        MessageReceiver.qConnection = null;
        MessageReceiver.context = null;
        MESSAGE_RECEIVER = new MessageReceiver();
    }
}

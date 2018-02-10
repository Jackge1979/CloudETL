package com.dataliance.service.util;

import javax.jms.*;

public class MessageUtil
{
    public static final String getProducorName(final Message message) throws JMSException {
        final Destination src = message.getJMSReplyTo();
        return (null == src) ? "" : src.toString();
    }
    
    public static final String getCusumorName(final Message message) throws JMSException {
        return message.getJMSDestination().toString();
    }
    
    public static final String getMessageContent(final Message message) throws JMSException {
        if (message instanceof TextMessage) {
            return ((TextMessage)message).getText();
        }
        throw new JMSException("only can deal with the TextMessage");
    }
    
    public static final String dumpMessage(final Message message) {
        final StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("message info : {\n");
        try {
            stringBuffer.append(String.format("from:%s\n", getProducorName(message)));
            stringBuffer.append(String.format("to:%s\n", getCusumorName(message)));
            stringBuffer.append(String.format("data:%s\n\n", getMessageContent(message)));
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
        return stringBuffer.append("}").toString();
    }
}

package com.dataliance.activemq.service.core;

import java.text.*;
import javax.jms.*;
import org.codehaus.jackson.type.*;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import java.io.*;
import com.dataliance.service.util.*;
import java.util.*;

public class JobInfoParser
{
    public static final String JOB_ID_FIELD = "job_id";
    private static final Random RANDOM;
    private static final char[] escapedChars;
    private static final SimpleDateFormat DATE_FORMAT;
    
    public static String buildMessageText(final JobInfo jobInfo) {
        final StringBuffer messageBuffer = new StringBuffer();
        messageBuffer.append("{");
        messageBuffer.append(buildMessageTextField("job_id", jobInfo.jobId));
        if (jobInfo.jobArgs != null) {
            for (final Map.Entry<String, String> entry : jobInfo.jobArgs.entrySet()) {
                if (null != entry.getValue()) {
                    if (null == entry.getKey()) {
                        continue;
                    }
                    messageBuffer.append(buildMessageTextField(entry.getKey(), entry.getValue()));
                }
            }
        }
        if (messageBuffer.length() > 1) {
            messageBuffer.deleteCharAt(messageBuffer.length() - 1);
        }
        messageBuffer.append("}");
        return messageBuffer.toString();
    }
    
    private static String buildMessageTextField(final String key, String value) {
        value = value.replaceAll("\\s+", " ");
        return String.format("\"%s\":\"%s\",", key, StringUtil.escape(value, JobInfoParser.escapedChars));
    }
    
    public static JobInfo parse(final Message message) throws Exception {
        if (message instanceof TextMessage) {
            final TextMessage textMessage = (TextMessage)message;
            try {
                return parse(textMessage.getText(), extractServiceNameFromMessage(message));
            }
            catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        try {
            throw new IllegalArgumentException("Unsupported message type " + message.getJMSType());
        }
        catch (JMSException e2) {
            e2.printStackTrace();
            return null;
        }
    }
    
    private static Map<String, String> decodeStrInUnicode(final Map<String, String> map) {
        final Map<String, String> mapAfterDecode = new HashMap<String, String>();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            mapAfterDecode.put(Decoder.decode(entry.getKey()), Decoder.decode(entry.getValue()));
        }
        return mapAfterDecode;
    }
    
    public static final JobInfo parse(final String messageContent, final String serviceName) throws JsonParseException, JsonMappingException, IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final Map<String, String> resultMap = (Map<String, String>)mapper.readValue(messageContent, (TypeReference)new TypeReference<Map<String, String>>() {});
        final JobInfo jobInfo = new JobInfo();
        String jobId = resultMap.remove("job_id");
        if (null == jobId) {
            jobId = generateJobId(serviceName);
        }
        jobInfo.jobId = jobId;
        jobInfo.jobArgs = decodeStrInUnicode(resultMap);
        jobInfo.serviceName = serviceName;
        return jobInfo;
    }
    
    public static String extractServiceNameFromMessage(final Message message) {
        String serviceName = null;
        try {
            final String destination = message.getJMSDestination().toString();
            final int startIndex = destination.lastIndexOf(".");
            if (startIndex < destination.length() - 1) {
                serviceName = destination.substring(startIndex + 1);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return serviceName;
    }
    
    static String generateJobId(final String serviceName) {
        return String.format("%s_%s_%s_%d", serviceName, SystemUtil.getPID(), JobInfoParser.DATE_FORMAT.format(new Date()), JobInfoParser.RANDOM.nextInt());
    }
    
    static {
        RANDOM = new Random();
        escapedChars = new char[] { '\"' };
        DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmSS");
    }
}

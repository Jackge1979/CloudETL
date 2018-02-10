package com.dataliance.hbase.util;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;

public class HbaseUtil
{
    public static String scanToString(final Scan scan) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(out);
        scan.write((DataOutput)dos);
        return Base64.encodeBytes(out.toByteArray());
    }
}

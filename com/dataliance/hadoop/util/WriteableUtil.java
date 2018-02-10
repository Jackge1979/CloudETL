package com.dataliance.hadoop.util;

import org.apache.hadoop.io.*;
import java.io.*;

public class WriteableUtil
{
    public static final byte[] toBytes(final Writable writable) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataOutputStream data = new DataOutputStream(out);
        writable.write((DataOutput)data);
        data.close();
        return out.toByteArray();
    }
    
    public static final void read(final Writable writable, final byte[] data) throws IOException {
        final DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        writable.readFields((DataInput)in);
        in.close();
    }
    
    public static final void read(final Writable writable, final InputStream in) throws IOException {
        final DataInputStream dIn = new DataInputStream(in);
        writable.readFields((DataInput)dIn);
        dIn.close();
    }
    
    public static final void read(final Writable writable, final DataInput in) throws IOException {
        writable.readFields(in);
    }
}

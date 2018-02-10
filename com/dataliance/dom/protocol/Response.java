package com.dataliance.dom.protocol;

import com.dataliance.dom.*;
import java.net.*;
import com.dataliance.dom.meta.*;

public interface Response extends HttpHeaders
{
    URL getUrl();
    
    int getCode();
    
    String getHeader(final String p0);
    
    Metadata getHeaders();
    
    byte[] getContent();
}

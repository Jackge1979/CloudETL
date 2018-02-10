package com.dataliance.util;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PACKAGE })
public @interface DAVersionAnnotation {
    String version();
    
    String user();
    
    String date();
    
    String url();
    
    String revision();
    
    String srcChecksum();
}

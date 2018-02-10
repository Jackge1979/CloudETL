package com.dataliance.hadoop.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD })
public @interface WriteOrderNum {
    int num() default 0;
}

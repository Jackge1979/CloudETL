package com.dataliance.etl.workflow.driver;

import java.text.*;

public abstract class test
{
    public abstract void test2();
    
    public static void main(final String[] args) throws ParseException {
        String log = "201208012331@#$67039046001@#$778@#$http://211.136.236.89:15000/@#$211.136.236.89@#$MAUI WAP Browser@#$444@#$496@#$";
        log = "201208012046@#$75096354441@#$755@#$http://us.u.uc.cn:80/usquery.php@#$us.u.uc.cn@#$Mozilla/5.0 (Linux; U; Android@#$23634@#$";
        final String[] values = log.split("@#\\$");
        System.out.println(values.length);
        for (final String value : values) {
            System.out.println(value);
        }
        final String result = "67035460136".replaceAll("\\d{11}", "12345678901");
        System.out.println(result);
    }
}

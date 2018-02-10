package com.dataliance.url.style.validate;

import java.net.*;
import java.util.*;

public class Parse
{
    private static final Map<String, String> cates;
    
    public static void main(final String[] args) throws MalformedURLException {
        for (final Map.Entry<String, String> cate : Parse.cates.entrySet()) {
            final URL url = new URL(cate.getKey());
            final String host = url.getHost();
            System.out.println("cates.put(\"" + host + "\",\"" + cate.getValue() + "\");");
        }
    }
    
    static {
        (cates = new HashMap<String, String>()).put("blog.sina.cn", "71");
        Parse.cates.put("123.yidichina.com", "58");
        Parse.cates.put("123.duomi.com", "58");
        Parse.cates.put("98xn.com", "4");
        Parse.cates.put("221.130.15.161", "33");
        Parse.cates.put("bbs.waptw.com", "9");
        Parse.cates.put("13888.hk", "4");
        Parse.cates.put("98tx.net", "22");
        Parse.cates.put("008833.com", "4");
        Parse.cates.put("abc.666hz.net", "4");
        Parse.cates.put("cm.wap.wawagame.cn", "39");
        Parse.cates.put("bm.gfzl.info", "4");
        Parse.cates.put("133s.com", "50");
        Parse.cates.put("1hc.me", "22");
        Parse.cates.put("183.232.22.74", "41");
        Parse.cates.put("wap.51job.com", "7");
        Parse.cates.put("www.89xs.com", "47");
        Parse.cates.put("117.79.91.80", "34");
        Parse.cates.put("1mw.cc", "4");
        Parse.cates.put("qqt1.160m.nj.twsapp.com", "41");
        Parse.cates.put("123hw.com", "58");
        Parse.cates.put("13279.com", "4");
        Parse.cates.put("read.twoDA.net", "45");
        Parse.cates.put("c.mmlive.cn", "40");
        Parse.cates.put("3g.pipgame.com", "33");
        Parse.cates.put("yoyuan.net", "75");
        Parse.cates.put("www.66zw.net", "42");
        Parse.cates.put("8.138ap.com", "33");
        Parse.cates.put("qbar.3g.qq.com", "0");
        Parse.cates.put("2012.sina.cn", "19");
        Parse.cates.put("m.ttzzz.net", "4");
        Parse.cates.put("rxzq2.wappp.cn", "34");
        Parse.cates.put("18uu.net", "4");
        Parse.cates.put("1.hh5.cc", "4");
        Parse.cates.put("221.130.10.204", "33");
        Parse.cates.put("100822.com", "4");
        Parse.cates.put("00561.com", "4");
        Parse.cates.put("19qq.hk", "4");
        Parse.cates.put("8cai.biz", "4");
        Parse.cates.put("a.h499.com", "4");
        Parse.cates.put("10.0.03.12.seangel.in", "0");
        Parse.cates.put("a.m.taobao.com", "2");
        Parse.cates.put("3g.paobook.com", "45");
        Parse.cates.put("d.wiyun.com", "33");
        Parse.cates.put("139.igou.cn", "2");
        Parse.cates.put("blog60.z.qq.com", "71");
        Parse.cates.put("acg.3g.cn", "58");
        Parse.cates.put("121.52.209.118", "0");
        Parse.cates.put("3g.yzs8.com", "49");
        Parse.cates.put("174.139.178.82", "4");
        Parse.cates.put("88szs.com.yizhan.baidu.com", "57");
        Parse.cates.put("08747.com", "4");
        Parse.cates.put("03697.com", "4");
        Parse.cates.put("weq.wfun.cn", "50");
        Parse.cates.put("211.139.147.121", "22");
        Parse.cates.put("111.11hz.cc", "0");
        Parse.cates.put("168yy.cc", "75");
        Parse.cates.put("cp5p.com", "4");
        Parse.cates.put("008tm.com", "4");
        Parse.cates.put("9888.hk", "4");
        Parse.cates.put("wapyl.net", "4");
        Parse.cates.put("2012.3g.cn", "19");
        Parse.cates.put("a.m.tmall.com", "2");
        Parse.cates.put("00528.com", "4");
        Parse.cates.put("998909.com", "4");
        Parse.cates.put("117.135.158.138", "34");
        Parse.cates.put("98.126.169.172", "4");
    }
}

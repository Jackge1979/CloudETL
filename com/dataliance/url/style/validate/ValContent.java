package com.dataliance.url.style.validate;

import com.dataliance.util.*;
import com.dataliance.hbase.table.query.*;

import java.net.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import java.util.*;

public class ValContent
{
    private static final Map<String, String> cates;
    
    public static void main(final String[] args) throws IOException {
        final String Usage = "<file> <contentName> <categoryName>";
        if (args.length < 3) {
            System.out.println(Usage);
            return;
        }
        final String file = args[0];
        final String contentName = args[1];
        final String categoryName = args[2];
        final Configuration conf = DAConfigUtil.create();
        final ContentQuery contentVal = new ContentQuery(conf, contentName, categoryName);
        final BufferedReader br = StreamUtil.getBufferedReader(file);
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            final URL url = new URL(line);
            final String host = url.getHost();
            String cate = ValContent.cates.get(host);
            if (cate == null) {
                cate = "0";
            }
            try {
                contentVal.verify(line, cate);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    static {
        (cates = new HashMap<String, String>()).put("blog.sina.cn", "71");
        ValContent.cates.put("123.yidichina.com", "58");
        ValContent.cates.put("123.duomi.com", "58");
        ValContent.cates.put("98xn.com", "4");
        ValContent.cates.put("221.130.15.161", "33");
        ValContent.cates.put("bbs.waptw.com", "9");
        ValContent.cates.put("13888.hk", "4");
        ValContent.cates.put("98tx.net", "22");
        ValContent.cates.put("008833.com", "4");
        ValContent.cates.put("abc.666hz.net", "4");
        ValContent.cates.put("cm.wap.wawagame.cn", "39");
        ValContent.cates.put("bm.gfzl.info", "4");
        ValContent.cates.put("133s.com", "50");
        ValContent.cates.put("1hc.me", "22");
        ValContent.cates.put("183.232.22.74", "41");
        ValContent.cates.put("wap.51job.com", "7");
        ValContent.cates.put("www.89xs.com", "47");
        ValContent.cates.put("117.79.91.80", "34");
        ValContent.cates.put("1mw.cc", "4");
        ValContent.cates.put("qqt1.160m.nj.twsapp.com", "41");
        ValContent.cates.put("123hw.com", "58");
        ValContent.cates.put("13279.com", "4");
        ValContent.cates.put("read.twoDA.net", "45");
        ValContent.cates.put("c.mmlive.cn", "40");
        ValContent.cates.put("3g.pipgame.com", "33");
        ValContent.cates.put("yoyuan.net", "75");
        ValContent.cates.put("www.66zw.net", "42");
        ValContent.cates.put("8.138ap.com", "33");
        ValContent.cates.put("qbar.3g.qq.com", "0");
        ValContent.cates.put("2012.sina.cn", "19");
        ValContent.cates.put("m.ttzzz.net", "4");
        ValContent.cates.put("rxzq2.wappp.cn", "34");
        ValContent.cates.put("18uu.net", "4");
        ValContent.cates.put("1.hh5.cc", "4");
        ValContent.cates.put("221.130.10.204", "33");
        ValContent.cates.put("100822.com", "4");
        ValContent.cates.put("00561.com", "4");
        ValContent.cates.put("19qq.hk", "4");
        ValContent.cates.put("8cai.biz", "4");
        ValContent.cates.put("a.h499.com", "4");
        ValContent.cates.put("10.0.03.12.seangel.in", "0");
        ValContent.cates.put("a.m.taobao.com", "2");
        ValContent.cates.put("3g.paobook.com", "45");
        ValContent.cates.put("d.wiyun.com", "33");
        ValContent.cates.put("139.igou.cn", "2");
        ValContent.cates.put("blog60.z.qq.com", "71");
        ValContent.cates.put("acg.3g.cn", "58");
        ValContent.cates.put("121.52.209.118", "0");
        ValContent.cates.put("3g.yzs8.com", "49");
        ValContent.cates.put("174.139.178.82", "4");
        ValContent.cates.put("88szs.com.yizhan.baidu.com", "57");
        ValContent.cates.put("08747.com", "4");
        ValContent.cates.put("03697.com", "4");
        ValContent.cates.put("weq.wfun.cn", "50");
        ValContent.cates.put("211.139.147.121", "22");
        ValContent.cates.put("111.11hz.cc", "0");
        ValContent.cates.put("168yy.cc", "75");
        ValContent.cates.put("cp5p.com", "4");
        ValContent.cates.put("008tm.com", "4");
        ValContent.cates.put("9888.hk", "4");
        ValContent.cates.put("wapyl.net", "4");
        ValContent.cates.put("2012.3g.cn", "19");
        ValContent.cates.put("a.m.tmall.com", "2");
        ValContent.cates.put("00528.com", "4");
        ValContent.cates.put("998909.com", "4");
        ValContent.cates.put("117.135.158.138", "34");
        ValContent.cates.put("98.126.169.172", "4");
    }
}

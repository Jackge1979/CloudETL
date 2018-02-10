package com.dataliance.url.style.validate;

import com.dataliance.hbase.*;
import com.dataliance.util.*;

import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import java.io.*;
import java.util.*;

public class ValidateContent
{
    private static final Map<String, String> cates;
    
    public static void main(final String[] args) throws IOException {
        final String usAge = "Usage : <tableName> <output>";
        if (args.length < 2) {
            System.err.println(usAge);
            return;
        }
        final String tableName = args[0];
        final String out = args[1];
        final FileWriter fw = new FileWriter(out);
        final HTableInterface htable = HTableFactory.getHTableFactory(DAConfigUtil.create()).getHTable(tableName);
        for (final Map.Entry<String, String> entry : ValidateContent.cates.entrySet()) {
            final String startRow = entry.getKey();
            final String stopRow = entry.getKey() + "~";
            final Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
            final ResultScanner rs = htable.getScanner(scan);
            for (final Result r : rs) {
                fw.append((CharSequence)(Bytes.toString(r.getRow()) + "\n"));
            }
        }
        htable.close();
        fw.close();
    }
    
    static {
        (cates = new HashMap<String, String>()).put("http://008833.com/", "4");
        ValidateContent.cates.put("http://03697.com/", "4");
        ValidateContent.cates.put("http://1.hh5.cc/", "4");
        ValidateContent.cates.put("http://111.11hz.cc/", "0");
        ValidateContent.cates.put("http://123.duomi.com/", "58");
        ValidateContent.cates.put("http://123.yidichina.com/", "58");
        ValidateContent.cates.put("http://123hw.com/", "58");
        ValidateContent.cates.put("http://13279.com/", "4");
        ValidateContent.cates.put("http://08747.com/", "4");
        ValidateContent.cates.put("http://2012.3g.cn/", "19");
        ValidateContent.cates.put("http://998909.com/", "4");
        ValidateContent.cates.put("http://bm.gfzl.info/", "4");
        ValidateContent.cates.put("http://blog60.z.qq.com/", "71");
        ValidateContent.cates.put("http://acg.3g.cn/", "58");
        ValidateContent.cates.put("http://cm.wap.wawagame.cn/", "39");
        ValidateContent.cates.put("http://cp5p.com/", "4");
        ValidateContent.cates.put("http://d.wiyun.com/", "33");
        ValidateContent.cates.put("http://qbar.3g.qq.com/", "0");
        ValidateContent.cates.put("http://wap.51job.com/", "7");
        ValidateContent.cates.put("http://10.0.03.12.seangel.in:90/", "0");
        ValidateContent.cates.put("http://a.m.tmall.com/", "2");
        ValidateContent.cates.put("http://a.m.taobao.com/", "2");
        ValidateContent.cates.put("http://blog.sina.cn/", "71");
        ValidateContent.cates.put("http://read.twoDA.net/", "45");
        ValidateContent.cates.put("http://19qq.hk/", "4");
        ValidateContent.cates.put("http://98xn.com/", "4");
        ValidateContent.cates.put("http://a.h499.com/", "4");
        ValidateContent.cates.put("http://1hc.me/", "22");
        ValidateContent.cates.put("http://211.139.147.121/", "22");
        ValidateContent.cates.put("http://18uu.net/", "4");
        ValidateContent.cates.put("http://98tx.net/", "22");
        ValidateContent.cates.put("http://174.139.178.82/", "4");
        ValidateContent.cates.put("http://2012.sina.cn/", "19");
        ValidateContent.cates.put("yoyuan.net/", "75");
        ValidateContent.cates.put("http://abc.666hz.net/", "4");
        ValidateContent.cates.put("http://bbs.waptw.com/", "9");
        ValidateContent.cates.put("http://c.mmlive.cn", "40");
        ValidateContent.cates.put("http://117.135.158.138/", "34");
        ValidateContent.cates.put("http://qqt1.160m.nj.twsapp.com/", "41");
        ValidateContent.cates.put("http://1mw.cc/", "4");
        ValidateContent.cates.put("http://221.130.10.204/", "33");
        ValidateContent.cates.put("http://008tm.com/", "4");
        ValidateContent.cates.put("http://100822.com/", "4");
        ValidateContent.cates.put("http://183.232.22.74:25511/", "41");
        ValidateContent.cates.put("http://00528.com/", "4");
        ValidateContent.cates.put("http://00561.com/", "4");
        ValidateContent.cates.put("http://221.130.15.161:8080/", "33");
        ValidateContent.cates.put("http://3g.paobook.com/", "45");
        ValidateContent.cates.put("http://3g.pipgame.com/", "33");
        ValidateContent.cates.put("http://3g.yzs8.com/", "49");
        ValidateContent.cates.put("http://8.138ap.com/", "33");
        ValidateContent.cates.put("http://88szs.com.yizhan.baidu.com/", "57");
        ValidateContent.cates.put("http://8cai.biz/", "4");
        ValidateContent.cates.put("http://9888.hk/", "4");
        ValidateContent.cates.put("http://rxzq2.wappp.cn/", "34");
        ValidateContent.cates.put("http://98.126.169.172/", "4");
        ValidateContent.cates.put("http://117.79.91.80", "34");
        ValidateContent.cates.put("http://168yy.cc/", "75");
        ValidateContent.cates.put("http://m.ttzzz.net/", "4");
        ValidateContent.cates.put("http://wapyl.net/", "4");
        ValidateContent.cates.put("http://weq.wfun.cn/", "50");
        ValidateContent.cates.put("http://www.66zw.net/", "42");
        ValidateContent.cates.put("http://www.89xs.com/", "47");
        ValidateContent.cates.put("http://13888.hk/", "4");
        ValidateContent.cates.put("http://133s.com/", "50");
        ValidateContent.cates.put("http://139.igou.cn/", "2");
        ValidateContent.cates.put("http://121.52.209.118/", "0");
    }
}

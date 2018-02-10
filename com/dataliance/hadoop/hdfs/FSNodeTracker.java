package com.dataliance.hadoop.hdfs;

import org.apache.hadoop.conf.*;
import java.net.*;
import java.io.*;
import org.json.*;

import com.dataliance.util.*;
import com.dataliance.hadoop.hdfs.vo.*;
import java.util.*;
import org.apache.hadoop.util.*;

public class FSNodeTracker
{
    private static Configuration conf;
    
    public FSNodeTracker(final Configuration conf) throws IOException, InterruptedException {
        FSNodeTracker.conf = conf;
    }
    
    public static JSONObject fetchJMXBean(final String key) throws Exception {
        final String host = FSNodeTracker.conf.get("fs.jmx.host");
        System.out.println(host);
        final URL url = new URL("http://" + host + "/jmx?qry=" + key);
        final HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        final InputStream in = conn.getInputStream();
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        final StringBuffer temp = new StringBuffer();
        for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
            temp.append(line);
        }
        bufferedReader.close();
        final JSONObject json = new JSONObject(temp.toString());
        final JSONArray jarr = json.getJSONArray("beans");
        for (int i = 0; i < jarr.length(); ++i) {
            final JSONObject obj = jarr.getJSONObject(i);
            final String name = obj.getString("name");
            if (name.equals(key)) {
                return obj;
            }
        }
        return null;
    }
    
    public List<DatanodeInfo> getLiveNodes() throws Exception {
        return this.getNodes("LiveNodes");
    }
    
    public List<DatanodeInfo> getNodes(final String type) throws Exception {
        final List<DatanodeInfo> lnlist = new ArrayList<DatanodeInfo>();
        final JSONObject obj = fetchJMXBean("Hadoop:service=NameNode,name=NameNodeInfo");
        String nodes = obj.getString(type);
        String[] nodeArr = null;
        if (nodes.length() > 2) {
            nodes = nodes.substring(1, nodes.length() - 2);
            nodeArr = nodes.split("},");
        }
        String content = "";
        if (nodeArr != null) {
            for (int i = 0; i < nodeArr.length; ++i) {
                final String name = nodeArr[i].split(":")[0].replace("\"", "");
                content = nodeArr[i].substring(nodeArr[i].indexOf("{")) + "}";
                final JSONObject json = new JSONObject(content);
                final DatanodeInfo nodeinfo = new DatanodeInfo();
                nodeinfo.setName(name);
                nodeinfo.setHostName(name);
                nodeinfo.setAdminState(json.getString("adminState"));
                nodeinfo.setCapacity(StringUtils.byteDesc(json.getLong("capacity")));
                nodeinfo.setDfsUsed(StringUtils.byteDesc(json.getLong("usedSpace")));
                nodeinfo.setLastContact(json.getString("lastContact"));
                nodeinfo.setNonDFSUsed(StringUtils.byteDesc(json.getLong("nonDfsUsedSpace")));
                final long remain = json.getLong("capacity") - json.getLong("usedSpace") - json.getLong("nonDfsUsedSpace");
                nodeinfo.setRemaining(StringUtils.byteDesc(remain));
                nodeinfo.setDfsUsedPercent(StringUtils.limitDecimalTo2((double)(json.getLong("usedSpace") * 100.0f / json.getLong("capacity"))));
                nodeinfo.setRemainingPercent(StringUtils.limitDecimalTo2((double)(remain * 100.0f / json.getLong("capacity"))));
                lnlist.add(nodeinfo);
            }
        }
        return lnlist;
    }
    
    public List<DatanodeInfo> getDeadNodes() throws Exception {
        return this.getNodes("DeadNodes");
    }
    
    public DFSClusterStatus getDFSStats() throws Exception {
        final DFSClusterStatus dfscs = new DFSClusterStatus();
        final JSONObject obj = fetchJMXBean("Hadoop:service=NameNode,name=NameNodeInfo");
        dfscs.setConfiguredCapacity(StringUtils.byteDesc(Long.parseLong(obj.getString("Total"))));
        dfscs.setDfsUsed(StringUtils.byteDesc(Long.parseLong(obj.getString("Used"))));
        dfscs.setNonDfsUsed(StringUtils.byteDesc(Long.parseLong(obj.getString("NonDfsUsedSpace"))));
        dfscs.setDfsRemaining(StringUtils.byteDesc(Long.parseLong(obj.getString("Free"))));
        dfscs.setPercentDfsUse(StringUtils.limitDecimalTo2((double)Float.parseFloat(obj.getString("PercentUsed"))));
        dfscs.setPercentDfsRemaining(StringUtils.limitDecimalTo2((double)Float.parseFloat(obj.getString("PercentRemaining"))));
        dfscs.setBlocks(obj.getLong("TotalBlocks"));
        String live = obj.getString("LiveNodes");
        live = live.substring(1, live.length() - 1);
        String[] liveArr = null;
        String[] deadArr = null;
        if (!live.equals("")) {
            liveArr = live.split("},");
        }
        String dead = obj.getString("DeadNodes");
        dead = dead.substring(1, dead.length() - 1);
        if (!dead.equals("")) {
            deadArr = dead.split("},");
        }
        dfscs.setLiveNodes((liveArr != null) ? liveArr.length : 0);
        dfscs.setDeadNodes((deadArr != null) ? deadArr.length : 0);
        return dfscs;
    }
    
    public static void main(final String[] args) {
        final Configuration conf = DAConfigUtil.create();
        try {
            final FSNodeTracker fst = new FSNodeTracker(conf);
            final List list = fst.getLiveNodes();
            System.out.println(list.size());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

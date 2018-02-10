package com.dataliance.etl.inject.config;

import org.apache.hadoop.conf.*;
import java.net.*;
import javax.xml.transform.stream.*;
import javax.xml.parsers.*;
import javax.xml.transform.dom.*;
import org.w3c.dom.*;
import javax.xml.transform.*;
import java.io.*;

import com.dataliance.etl.inject.local.*;
import com.dataliance.etl.inject.shell.*;
import com.dataliance.etl.inject.ssh.*;
import com.dataliance.util.*;

import java.util.*;
import java.util.logging.Logger;

import org.slf4j.*;

class DAConf extends Configuration implements DAConfig
{
    private static final Logger LOG;
    private static final String DA_CONFIG_DELETE = "__DA__DELETE__";
    private String resource;
    private HadoopSource hadoopSource;
    private File tmpDir;
    private String local;
    private Filter filter;
    
    public DAConf(final Configuration conf, final String resource) {
        super(false);
        final URL url = this.getRes(resource);
        DAConf.LOG.info("从 URL 中加载资源 " + url);
        this.addResource(url);
        this.resource = resource;
        this.hadoopSource = new HadoopSource(conf);
        try {
            this.local = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            DAConf.LOG.error(e.getMessage(), (Throwable)e);
        }
        this.tmpDir = StreamUtil.getTmpDir();
    }
    
    private void save() {
        final URL url = this.getRes(this.resource);
        try {
            final File file = new File(url.toURI());
            DAConf.LOG.info("将配置保存到文件 " + file);
            this.writeXml(new FileOutputStream(file));
        }
        catch (Exception e) {
            DAConf.LOG.error(e.getMessage(), (Throwable)e);
        }
    }
    
    public void writeXml(final OutputStream out) throws IOException {
        this.writeXml(new StreamResult(out));
    }
    
    public void writeXml(final Writer writer) throws IOException {
        this.writeXml(new StreamResult(writer));
    }
    // 将配置文件更新写入到 XML 中
    private void writeXml(final StreamResult result) throws IOException {
        try {
            final Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            final Element conf = doc.createElement("configuration");
            doc.appendChild(conf);
            conf.appendChild(doc.createTextNode("\n"));
            for (final Map.Entry<String, String> entry : this) {
                final String name = entry.getKey();
                final String value = entry.getValue();
                if (value.equals("__DA__DELETE__")) {
                    continue;
                }
                final Element propNode = doc.createElement("property");
                conf.appendChild(propNode);
                final Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(name));
                propNode.appendChild(nameNode);
                final Element valueNode = doc.createElement("value");
                valueNode.appendChild(doc.createTextNode(value));
                propNode.appendChild(valueNode);
                conf.appendChild(doc.createTextNode("\n"));
            }
            final DOMSource source = new DOMSource(doc);
            final TransformerFactory transFactory = TransformerFactory.newInstance();
            final Transformer transformer = transFactory.newTransformer();
            transformer.transform(source, result);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public String delete(final String name) {
        final String value = this.get(name);
        this.set(name, "__DA__DELETE__");
        return value;
    }
    
    private URL getRes(final String resource) {
        URL url = this.getResource(resource);
        try {
            if (url == null) {
                final File file = new File(resource);
                if (file.exists()) {
                    url = file.toURI().toURL();
                }
            }
        }
        catch (Exception e) {
            DAConf.LOG.error(e.getMessage(), (Throwable)e);
        }
        return url;
    }
    
    private Collection<String> getNodes() throws IOException {
        final Collection<String> nodes = new ArrayList<String>();
        final String slaves = this.hadoopSource.getHome() + "/" + "conf" + "/" + "slaves";
        BufferedReader br = null;
        if (this.local.equals(this.hadoopSource.getMaster())) {
            br = StreamUtil.getBufferedReader(slaves);
        }
        else {
            final SSHClient sshClient = this.getSSHClient(this.hadoopSource.getMaster());
            sshClient.copyToLocal(slaves, this.tmpDir);
            sshClient.close();
            br = StreamUtil.getBufferedReader(new File(this.tmpDir, "slaves"));
        }
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (!StringUtil.isEmpty(line) && !line.startsWith("#")) {
                nodes.add(line);
            }
        }
        br.close();
        return nodes;
    }
    
    private SSHClient getSSHClient(final String host) throws IOException {
        SSHClient sshClient = null;
        if (this.hadoopSource.isUsePassword()) {
            sshClient = new SSHClient(host, this.hadoopSource.getUser(), this.hadoopSource.getPassword());
        }
        else {
            sshClient = new SSHClient(host, this.hadoopSource.getUser(), this.hadoopSource.getPriKeyFile());
        }
        return sshClient;
    }
    
    public void apply() throws Exception {
        final String hadoopConfig = this.hadoopSource.getHome() + "/" + "conf";
        final URL url = this.getRes(this.resource);
        final File file = new File(url.toURI());
        Shell shell = null;
        if (this.local.equals(this.hadoopSource.getMaster())) {
            StreamUtil.output(url.openStream(), new File(hadoopConfig, file.getName()));
            shell = new LocalShell(LogUtil.getInfoStream(DAConf.LOG), LogUtil.getErrorStream(DAConf.LOG));
        }
        else {
            final SSHClient sshClient = this.getSSHClient(this.hadoopSource.getMaster());
            sshClient.copyToRemote(file, hadoopConfig);
            shell = sshClient.getShell();
        }
        final Collection<String> nodes = this.getNodes();
        for (final String node : nodes) {
            shell.scp(node, this.hadoopSource.getUser(), hadoopConfig, hadoopConfig + "/" + file.getName());
        }
    }
    
    public void set(final String name, final String value) {
        super.set(name, value);
        this.save();
    }
    
    public Filter getFilter() {
        return this.filter;
    }
    
    public void setFilter(final Filter filter) {
        this.filter = filter;
    }
    
    public Iterator<Map.Entry<String, String>> iterator() {
        final Iterator<Map.Entry<String, String>> iter = (Iterator<Map.Entry<String, String>>)super.iterator();
        if (this.filter != null) {
            final Map<String, String> result = new HashMap<String, String>();
            while (iter.hasNext()) {
                final Map.Entry<String, String> entry = iter.next();
                if (this.filter.accept(entry.getKey(), entry.getValue())) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
            return result.entrySet().iterator();
        }
        return iter;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)ConfigManager.class);
    }
}

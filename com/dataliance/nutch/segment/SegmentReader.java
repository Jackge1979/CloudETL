package com.dataliance.nutch.segment;

import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.parse.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.*;
import org.apache.nutch.crawl.*;
import org.apache.nutch.util.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.slf4j.*;
import org.wltea.analyzer.lucene.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class SegmentReader extends Configured implements Reducer<Text, NutchWritable, Text, WebDocument>
{
    public static final Logger LOG;
    private static final int MODE_DUMP = 0;
    private static final int MODE_LIST = 1;
    private static final int MODE_GET = 2;
    private static Analyzer analyzer;
    private static final String WORD_SPLIT = " ";
    private boolean isTonken;
    long recNo;
    private boolean co;
    private boolean fe;
    private boolean ge;
    private boolean pa;
    private boolean pd;
    private boolean pt;
    private FileSystem fs;
    private static final String[][] keys;
    SimpleDateFormat sdf;
    
    public SegmentReader() {
        super((Configuration)null);
        this.isTonken = false;
        this.recNo = 0L;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    }
    
    public SegmentReader(final Configuration conf, final boolean co, final boolean fe, final boolean ge, final boolean pa, final boolean pd, final boolean pt) {
        super(conf);
        this.isTonken = false;
        this.recNo = 0L;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        this.co = co;
        this.fe = fe;
        this.ge = ge;
        this.pa = pa;
        this.pd = pd;
        this.pt = pt;
        this.isTonken = conf.getBoolean("web.content.token", false);
        try {
            this.fs = FileSystem.get(this.getConf());
        }
        catch (IOException e) {
            e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
        }
    }
    
    public void configure(final JobConf job) {
        this.setConf((Configuration)job);
        this.co = this.getConf().getBoolean("segment.reader.co", true);
        this.fe = this.getConf().getBoolean("segment.reader.fe", true);
        this.ge = this.getConf().getBoolean("segment.reader.ge", true);
        this.pa = this.getConf().getBoolean("segment.reader.pa", true);
        this.pd = this.getConf().getBoolean("segment.reader.pd", true);
        this.pt = this.getConf().getBoolean("segment.reader.pt", true);
        this.isTonken = this.getConf().getBoolean("web.content.token", false);
        try {
            this.fs = FileSystem.get(this.getConf());
        }
        catch (IOException e) {
            e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
        }
    }
    
    private JobConf createJobConf() {
        final JobConf job = (JobConf)new NutchJob(this.getConf());
        job.setBoolean("segment.reader.co", this.co);
        job.setBoolean("segment.reader.fe", this.fe);
        job.setBoolean("segment.reader.ge", this.ge);
        job.setBoolean("segment.reader.pa", this.pa);
        job.setBoolean("segment.reader.pd", this.pd);
        job.setBoolean("segment.reader.pt", this.pt);
        return job;
    }
    
    public void reduce(final Text key, final Iterator<NutchWritable> values, final OutputCollector<Text, WebDocument> output, final Reporter reporter) throws IOException {
        String title = null;
        String content = null;
        while (values.hasNext()) {
            final Writable value = values.next().get();
            if (value instanceof ParseData) {
                title = ((ParseData)value).getTitle();
            }
            else {
                if (!(value instanceof ParseText)) {
                    continue;
                }
                content = ((ParseText)value).getText();
            }
        }
        String tokens = "";
        if (this.isTonken) {
            tokens = getTokens(title + content, " ");
            SegmentReader.LOG.debug("tokens:" + tokens);
        }
        output.collect((Object)key, (Object)new WebDocument(key.toString(), title, content, tokens, " "));
    }
    
    public void close() throws IOException {
    }
    
    public static String getTokens(final String content, final String splitTag) {
        if (content == null || "".equals(content.trim())) {
            return "";
        }
        final StringBuilder buffer = new StringBuilder();
        final TokenStream tokenStream = SegmentReader.analyzer.tokenStream((String)null, (Reader)new StringReader(content));
        final CharTermAttribute termAtt = (CharTermAttribute)tokenStream.addAttribute((Class)CharTermAttribute.class);
        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                final char[] termBuffer = termAtt.buffer();
                final int termLen = termAtt.length();
                if (termLen < 2) {
                    continue;
                }
                buffer.append(termBuffer, 0, termLen);
                buffer.append(splitTag);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }
    
    public void dump(final Path segment, final Path output) throws IOException {
        if (SegmentReader.LOG.isInfoEnabled()) {
            SegmentReader.LOG.info("SegmentReader: dump segment: " + segment);
        }
        final JobConf job = this.createJobConf();
        job.setJobName("read " + segment);
        if (this.ge) {
            FileInputFormat.addInputPath(job, new Path(segment, "crawl_generate"));
        }
        if (this.fe) {
            FileInputFormat.addInputPath(job, new Path(segment, "crawl_fetch"));
        }
        if (this.pa) {
            FileInputFormat.addInputPath(job, new Path(segment, "crawl_parse"));
        }
        if (this.co) {
            FileInputFormat.addInputPath(job, new Path(segment, "content"));
        }
        if (this.pd) {
            FileInputFormat.addInputPath(job, new Path(segment, "parse_data"));
        }
        if (this.pt) {
            FileInputFormat.addInputPath(job, new Path(segment, "parse_text"));
        }
        final Path dumpFile = new Path(output, "segments-dump");
        this.fs.delete(dumpFile, true);
        job.setJarByClass((Class)SegmentReader.class);
        job.setInputFormat((Class)SequenceFileInputFormat.class);
        job.setMapperClass((Class)InputCompatMapper.class);
        job.setReducerClass((Class)SegmentReader.class);
        FileOutputFormat.setOutputPath(job, dumpFile);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)NutchWritable.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)WebDocument.class);
        job.setOutputFormat((Class)SequenceFileOutputFormat.class);
        JobClient.runJob(job);
        if (SegmentReader.LOG.isInfoEnabled()) {
            SegmentReader.LOG.info("SegmentReader: done");
        }
    }
    
    public void get(final Path segment, final Text key, final Writer writer, final Map<String, List<Writable>> results) throws Exception {
        SegmentReader.LOG.info("SegmentReader: get '" + key + "'");
        final ArrayList<Thread> threads = new ArrayList<Thread>();
        if (this.co) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        final List<Writable> res = SegmentReader.this.getMapRecords(new Path(segment, "content"), key);
                        results.put("co", res);
                    }
                    catch (Exception e) {
                        e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
                    }
                }
            });
        }
        if (this.fe) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        final List<Writable> res = SegmentReader.this.getMapRecords(new Path(segment, "crawl_fetch"), key);
                        results.put("fe", res);
                    }
                    catch (Exception e) {
                        e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
                    }
                }
            });
        }
        if (this.ge) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        final List<Writable> res = SegmentReader.this.getSeqRecords(new Path(segment, "crawl_generate"), key);
                        results.put("ge", res);
                    }
                    catch (Exception e) {
                        e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
                    }
                }
            });
        }
        if (this.pa) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        final List<Writable> res = SegmentReader.this.getSeqRecords(new Path(segment, "crawl_parse"), key);
                        results.put("pa", res);
                    }
                    catch (Exception e) {
                        e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
                    }
                }
            });
        }
        if (this.pd) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        final List<Writable> res = SegmentReader.this.getMapRecords(new Path(segment, "parse_data"), key);
                        results.put("pd", res);
                    }
                    catch (Exception e) {
                        e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
                    }
                }
            });
        }
        if (this.pt) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        final List<Writable> res = SegmentReader.this.getMapRecords(new Path(segment, "parse_text"), key);
                        results.put("pt", res);
                    }
                    catch (Exception e) {
                        e.printStackTrace(LogUtil.getWarnStream(SegmentReader.LOG));
                    }
                }
            });
        }
        Iterator<Thread> it = threads.iterator();
        while (it.hasNext()) {
            it.next().start();
        }
        int cnt;
        do {
            cnt = 0;
            try {
                Thread.sleep(5000L);
            }
            catch (Exception ex) {}
            it = threads.iterator();
            while (it.hasNext()) {
                if (it.next().isAlive()) {
                    ++cnt;
                }
            }
            if (cnt > 0 && SegmentReader.LOG.isDebugEnabled()) {
                SegmentReader.LOG.debug("(" + cnt + " to retrieve)");
            }
        } while (cnt > 0);
        for (int i = 0; i < SegmentReader.keys.length; ++i) {
            final List<Writable> res = results.get(SegmentReader.keys[i][0]);
            if (res != null && res.size() > 0) {
                for (int k = 0; k < res.size(); ++k) {
                    writer.write(SegmentReader.keys[i][1]);
                    writer.write(res.get(k) + "\n");
                }
            }
            writer.flush();
        }
    }
    
    private List<Writable> getMapRecords(final Path dir, final Text key) throws Exception {
        final MapFile.Reader[] readers = MapFileOutputFormat.getReaders(this.fs, dir, this.getConf());
        final ArrayList<Writable> res = new ArrayList<Writable>();
        final Class keyClass = readers[0].getKeyClass();
        final Class valueClass = readers[0].getValueClass();
        if (!keyClass.getName().equals("org.apache.hadoop.io.Text")) {
            throw new IOException("Incompatible key (" + keyClass.getName() + ")");
        }
        final Writable value = valueClass.newInstance();
        for (int i = 0; i < readers.length; ++i) {
            if (readers[i].get((WritableComparable)key, value) != null) {
                res.add(value);
            }
            readers[i].close();
        }
        return res;
    }
    
    private List<Writable> getSeqRecords(final Path dir, final Text key) throws Exception {
        final SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(this.getConf(), dir);
        final ArrayList<Writable> res = new ArrayList<Writable>();
        final Class keyClass = readers[0].getKeyClass();
        final Class valueClass = readers[0].getValueClass();
        if (!keyClass.getName().equals("org.apache.hadoop.io.Text")) {
            throw new IOException("Incompatible key (" + keyClass.getName() + ")");
        }
        final Writable aKey = keyClass.newInstance();
        final Writable value = valueClass.newInstance();
        for (int i = 0; i < readers.length; ++i) {
            while (readers[i].next(aKey, value)) {
                if (aKey.equals(key)) {
                    res.add(value);
                }
            }
            readers[i].close();
        }
        return res;
    }
    
    public void list(final List<Path> dirs, final Writer writer) throws Exception {
        writer.write("NAME\t\tGENERATED\tFETCHER START\t\tFETCHER END\t\tFETCHED\tPARSED\n");
        for (int i = 0; i < dirs.size(); ++i) {
            final Path dir = dirs.get(i);
            final SegmentReaderStats stats = new SegmentReaderStats();
            this.getStats(dir, stats);
            writer.write(dir.getName() + "\t");
            if (stats.generated == -1L) {
                writer.write("?");
            }
            else {
                writer.write(stats.generated + "");
            }
            writer.write("\t\t");
            if (stats.start == -1L) {
                writer.write("?\t");
            }
            else {
                writer.write(this.sdf.format(new Date(stats.start)));
            }
            writer.write("\t");
            if (stats.end == -1L) {
                writer.write("?");
            }
            else {
                writer.write(this.sdf.format(new Date(stats.end)));
            }
            writer.write("\t");
            if (stats.fetched == -1L) {
                writer.write("?");
            }
            else {
                writer.write(stats.fetched + "");
            }
            writer.write("\t");
            if (stats.parsed == -1L) {
                writer.write("?");
            }
            else {
                writer.write(stats.parsed + "");
            }
            writer.write("\n");
            writer.flush();
        }
    }
    
    public void getStats(final Path segment, final SegmentReaderStats stats) throws Exception {
        final SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(this.getConf(), new Path(segment, "crawl_generate"));
        long cnt = 0L;
        final Text key = new Text();
        for (int i = 0; i < readers.length; ++i) {
            while (readers[i].next((Writable)key)) {
                ++cnt;
            }
            readers[i].close();
        }
        stats.generated = cnt;
        final Path fetchDir = new Path(segment, "crawl_fetch");
        if (this.fs.exists(fetchDir) && this.fs.getFileStatus(fetchDir).isDir()) {
            cnt = 0L;
            long start = Long.MAX_VALUE;
            long end = Long.MIN_VALUE;
            final CrawlDatum value = new CrawlDatum();
            final MapFile.Reader[] mreaders = MapFileOutputFormat.getReaders(this.fs, fetchDir, this.getConf());
            for (int j = 0; j < mreaders.length; ++j) {
                while (mreaders[j].next((WritableComparable)key, (Writable)value)) {
                    ++cnt;
                    if (value.getFetchTime() < start) {
                        start = value.getFetchTime();
                    }
                    if (value.getFetchTime() > end) {
                        end = value.getFetchTime();
                    }
                }
                mreaders[j].close();
            }
            stats.start = start;
            stats.end = end;
            stats.fetched = cnt;
        }
        final Path parseDir = new Path(segment, "parse_data");
        if (this.fs.exists(fetchDir) && this.fs.getFileStatus(fetchDir).isDir()) {
            cnt = 0L;
            long errors = 0L;
            final ParseData value2 = new ParseData();
            final MapFile.Reader[] mreaders2 = MapFileOutputFormat.getReaders(this.fs, parseDir, this.getConf());
            for (int k = 0; k < mreaders2.length; ++k) {
                while (mreaders2[k].next((WritableComparable)key, (Writable)value2)) {
                    ++cnt;
                    if (!value2.getStatus().isSuccess()) {
                        ++errors;
                    }
                }
                mreaders2[k].close();
            }
            stats.parsed = cnt;
            stats.parseErrors = errors;
        }
    }
    
    private static void usage() {
        System.err.println("Usage: SegmentReader (-dump ... | -list ... | -get ...) [general options]\n");
        System.err.println("* General options:");
        System.err.println("\t-nocontent\tignore content directory");
        System.err.println("\t-nofetch\tignore crawl_fetch directory");
        System.err.println("\t-nogenerate\tignore crawl_generate directory");
        System.err.println("\t-noparse\tignore crawl_parse directory");
        System.err.println("\t-noparsedata\tignore parse_data directory");
        System.err.println("\t-noparsetext\tignore parse_text directory");
        System.err.println();
        System.err.println("* SegmentReader -dump <segment_dir> <output> [general options]");
        System.err.println("  Dumps content of a <segment_dir> as a text file to <output>.\n");
        System.err.println("\t<segment_dir>\tname of the segment directory.");
        System.err.println("\t<output>\tname of the (non-existent) output directory.");
        System.err.println();
        System.err.println("* SegmentReader -list (<segment_dir1> ... | -dir <segments>) [general options]");
        System.err.println("  List a synopsis of segments in specified directories, or all segments in");
        System.err.println("  a directory <segments>, and print it on System.out\n");
        System.err.println("\t<segment_dir1> ...\tlist of segment directories to process");
        System.err.println("\t-dir <segments>\t\tdirectory that contains multiple segments");
        System.err.println();
        System.err.println("* SegmentReader -get <segment_dir> <keyValue> [general options]");
        System.err.println("  Get a specified record from a segment, and print it on System.out.\n");
        System.err.println("\t<segment_dir>\tname of the segment directory.");
        System.err.println("\t<keyValue>\tvalue of the key (url).");
        System.err.println("\t\tNote: put double-quotes around strings with spaces.");
    }
    
    public static void main(final String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }
        int mode = -1;
        if (args[0].equals("-dump")) {
            mode = 0;
        }
        else if (args[0].equals("-list")) {
            mode = 1;
        }
        else if (args[0].equals("-get")) {
            mode = 2;
        }
        boolean co = false;
        boolean fe = false;
        boolean ge = false;
        boolean pa = false;
        boolean pd = true;
        boolean pt = true;
        for (int i = 1; i < args.length; ++i) {
            if (args[i].equals("-nocontent")) {
                co = false;
                args[i] = null;
            }
            else if (args[i].equals("-nofetch")) {
                fe = false;
                args[i] = null;
            }
            else if (args[i].equals("-nogenerate")) {
                ge = false;
                args[i] = null;
            }
            else if (args[i].equals("-noparse")) {
                pa = false;
                args[i] = null;
            }
            else if (args[i].equals("-noparsedata")) {
                pd = false;
                args[i] = null;
            }
            else if (args[i].equals("-noparsetext")) {
                pt = false;
                args[i] = null;
            }
        }
        final Configuration conf = NutchConfiguration.create();
        final FileSystem fs = FileSystem.get(conf);
        final SegmentReader segmentReader = new SegmentReader(conf, co, fe, ge, pa, pd, pt);
        switch (mode) {
            case 0: {
                final String input = args[1];
                if (input == null) {
                    System.err.println("Missing required argument: <segment_dir>");
                    usage();
                    return;
                }
                final String output = (args.length > 2) ? args[2] : null;
                if (output == null) {
                    System.err.println("Missing required argument: <output>");
                    usage();
                    return;
                }
                segmentReader.dump(new Path(input), new Path(output));
            }
            case 1: {
                final ArrayList<Path> dirs = new ArrayList<Path>();
                for (int j = 1; j < args.length; ++j) {
                    if (args[j] != null) {
                        if (args[j].equals("-dir")) {
                            final Path dir = new Path(args[++j]);
                            final FileStatus[] fstats = fs.listStatus(dir, HadoopFSUtil.getPassDirectoriesFilter(fs));
                            final Path[] files = HadoopFSUtil.getPaths(fstats);
                            if (files != null && files.length > 0) {
                                dirs.addAll(Arrays.asList(files));
                            }
                        }
                        else {
                            dirs.add(new Path(args[j]));
                        }
                    }
                }
                segmentReader.list(dirs, new OutputStreamWriter(System.out, "UTF-8"));
            }
            case 2: {
                final String input = args[1];
                if (input == null) {
                    System.err.println("Missing required argument: <segment_dir>");
                    usage();
                    return;
                }
                final String key = (args.length > 2) ? args[2] : null;
                if (key == null) {
                    System.err.println("Missing required argument: <keyValue>");
                    usage();
                    return;
                }
                segmentReader.get(new Path(input), new Text(key), new OutputStreamWriter(System.out, "UTF-8"), new HashMap<String, List<Writable>>());
            }
            default: {
                System.err.println("Invalid operation: " + args[0]);
                usage();
            }
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)SegmentReader.class);
        SegmentReader.analyzer = (Analyzer)new IKAnalyzer();
        keys = new String[][] { { "co", "Content::\n" }, { "ge", "Crawl Generate::\n" }, { "fe", "Crawl Fetch::\n" }, { "pa", "Crawl Parse::\n" }, { "pd", "ParseData::\n" }, { "pt", "ParseText::\n" } };
    }
    
    public static class InputCompatMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, NutchWritable>
    {
        private Text newKey;
        
        public InputCompatMapper() {
            this.newKey = new Text();
        }
        
        public void map(WritableComparable key, final Writable value, final OutputCollector<Text, NutchWritable> collector, final Reporter reporter) throws IOException {
            if (key instanceof UTF8) {
                this.newKey.set(key.toString());
                key = (WritableComparable)this.newKey;
            }
            if (value instanceof ParseData || value instanceof ParseText) {
                collector.collect((Object)key, (Object)new NutchWritable(value));
            }
        }
    }
    
    public static class SegmentReaderStats
    {
        public long start;
        public long end;
        public long generated;
        public long fetched;
        public long fetchErrors;
        public long parsed;
        public long parseErrors;
        
        public SegmentReaderStats() {
            this.start = -1L;
            this.end = -1L;
            this.generated = -1L;
            this.fetched = -1L;
            this.fetchErrors = -1L;
            this.parsed = -1L;
            this.parseErrors = -1L;
        }
    }
}

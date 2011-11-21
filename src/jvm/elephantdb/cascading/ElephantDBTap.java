package elephantdb.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.*;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.hadoop.*;
import elephantdb.store.DomainStore;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;


public class ElephantDBTap extends Tap implements FlowListener {
    public static class ElephantScheme extends Scheme {
        @Override
        public void sourceInit(Tap tap, JobConf jc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void sinkInit(Tap tap, JobConf jc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Tuple source(Object o, Object o1) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void sink(TupleEntry te, OutputCollector oc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public static class Args implements Serializable {
        //for source and sink
        public Map<String, Object> persistenceOptions = null;
        public List<String> tmpDirs = null;
        public int timeoutMs = 2 * 60 * 60 * 1000; // 2 hours

        //source specific
        public Fields sourceFields = new Fields("key", "value");
        public Long version = null; //for sourcing
        public Deserializer deserializer = null;

        //sink specific
        public Fields sinkFields = Fields.ALL;
        public ElephantUpdater updater = new ReplaceUpdater();
            //set this to null to prevent updating
    }

    String _domainDir;
    DomainSpec _spec;
    Args _args;
    String _newVersionPath;

    public ElephantDBTap(String dir, Args args) throws IOException {
        this(dir, null, args);
    }

    public ElephantDBTap(String dir, DomainSpec spec) throws IOException {
        this(dir, spec, new Args());
    }

    public ElephantDBTap(String dir) throws IOException {
        this(dir, null, new Args());
    }

    public ElephantDBTap(String dir, DomainSpec spec, Args args) throws IOException {
        super(new ElephantScheme());
        _domainDir = dir;
        _spec = new DomainStore(dir, spec).getSpec();
        _args = args;
        _id = globalid++;
    }

    private DomainStore getDomainStore() throws IOException {
        return new DomainStore(_domainDir, _spec);
    }

    public DomainSpec getSpec() {
        return _spec;
    }

    @Override
    public Fields getSinkFields() {
        return _args.sinkFields;
    }

    @Override
    public Fields getSourceFields() {
        return _args.sourceFields;
    }

    @Override
    public boolean isWriteDirect() {
        return true;
    }

    @Override
    public Tuple source(Object key, Object value) {
        key = _args.deserializer == null ? key : _args.deserializer.deserialize((BytesWritable) key);
        return new Tuple(key, value);
    }

    @Override
    public void sourceInit(JobConf conf) throws IOException {

        // Why do we use this random string?
        FileInputFormat.setInputPaths(conf, "/" + UUID.randomUUID().toString());

        ElephantInputFormat.Args eargs = new ElephantInputFormat.Args(_domainDir);
        eargs.inputDirHdfs = _domainDir;
        if (_args.persistenceOptions != null) {
            eargs.persistenceOptions = _args.persistenceOptions;
        }
        if (_args.tmpDirs != null) { LocalElephantManager.setTmpDirs(conf, _args.tmpDirs); }
        eargs.version = _args.version;

        conf.setInt("mapred.task.timeout", _args.timeoutMs);
        Utils.setObject(conf, ElephantInputFormat.ARGS_CONF, eargs);
        conf.setInputFormat(ElephantInputFormat.class);
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        int shard = tupleEntry.getInteger(0);
        Object key = tupleEntry.get(1);
        byte[] keybytes = Common.serializeElephantVal(key);
        byte[] valuebytes = Common.getBytes((BytesWritable) tupleEntry.get(2));

        ElephantRecordWritable record = new ElephantRecordWritable(keybytes, valuebytes);
        outputCollector.collect(new IntWritable(shard), record);
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
        DomainStore dstore = getDomainStore();
        if (_newVersionPath == null) { //working around cascading calling sinkinit twice
            _newVersionPath = dstore.createVersion();
        }
        ElephantOutputFormat.Args eargs = new ElephantOutputFormat.Args(_spec, _newVersionPath);
        if (_args.persistenceOptions != null) {
            eargs.persistenceOptions = _args.persistenceOptions;
        }
        if (_args.tmpDirs != null) {
            LocalElephantManager.setTmpDirs(conf, _args.tmpDirs);
        }
        if (_args.updater != null) {
            eargs.updater = _args.updater;
            eargs.updateDirHdfs = dstore.mostRecentVersionPath();
        }
        Utils.setObject(conf, ElephantOutputFormat.ARGS_CONF, eargs);
        conf.setInt("mapred.task.timeout", _args.timeoutMs);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setInt("mapred.reduce.tasks", _spec.getNumShards());
        conf.setOutputFormat(ElephantOutputFormat.class);
    }

    @Override
    public Path getPath() {
        return new Path(_domainDir);
    }

    @Override
    public boolean makeDirs(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean deletePath(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean pathExists(JobConf jc) throws IOException {
        return false;
    }

    @Override
    public long getPathModified(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

    public void onStarting(Flow flow) {

    }

    public void onStopping(Flow flow) {

    }

    private boolean isSinkOf(Flow flow) {
        for (Entry<String, Tap> e : flow.getSinks().entrySet()) {
            if (e.getValue() == this) { return true; }
        }
        return false;
    }

    public void onCompleted(Flow flow) {
        try {
            if (isSinkOf(flow)) {
                DomainStore dstore = getDomainStore();
                if (flow.getFlowStats().isSuccessful()) {
                    dstore.getFileSystem().mkdirs(new Path(_newVersionPath));
                    if (_args.updater != null) {
                        dstore.synchronizeInProgressVersion(_newVersionPath);
                    }
                    dstore.succeedVersion(_newVersionPath);
                } else {
                    dstore.failVersion(_newVersionPath);
                }
            }
        } catch (IOException e) {
            throw new TapException("Couldn't finalize new elephant domain version", e);
        } finally {
            _newVersionPath = null; //working around cascading calling sinkinit twice
        }
    }

    public boolean onThrowable(Flow flow, Throwable t) {
        return false;
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
        return new TapCollector(this, conf);
    }

    @Override
    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
        return new TupleEntryIterator(getSourceFields(), new TapIterator(this, conf));
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof ElephantDBTap) {
            return _id == ((ElephantDBTap) object)._id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new Integer(_id).hashCode();
    }

    private int _id;
    private static int globalid = 0;

}

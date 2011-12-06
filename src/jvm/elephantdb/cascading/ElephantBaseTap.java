package elephantdb.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.*;
import com.esotericsoftware.kryo.ObjectBuffer;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.hadoop.*;
import elephantdb.persistence.KeyValDocument;
import elephantdb.persistence.LocalPersistenceFactory;
import elephantdb.persistence.Transmitter;
import elephantdb.store.DomainStore;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import sun.plugin2.message.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;


public abstract class ElephantBaseTap extends Tap implements FlowListener {

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
    LocalPersistenceFactory _fact;
    ObjectBuffer _kryoBuf;
    Args _args;
    String _newVersionPath;

    public ElephantBaseTap(String dir, DomainSpec spec, Args args) throws IOException {
        super(new ElephantScheme());
        _domainDir = dir;
        _spec = new DomainStore(dir, spec).getSpec();
        _fact = _spec.getLPFactory();
        _kryoBuf = _spec.getObjectBuffer();

        _args = args;
    }

    public DomainStore getDomainStore() throws IOException {
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

    @Override public Tuple source(Object key, Object value) {
        byte[] valBytes = Utils.getBytes((BytesWritable) value);
        Object doc = _kryoBuf.readClassAndObject(valBytes);
        return new Tuple(doc);
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


    // This is generic, this is good stuff.
    @Override public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
        throws IOException {
        int shard = tupleEntry.getInteger(0);
        Object doc = tupleEntry.get(1);
        byte[] crushedDocument = _kryoBuf.writeClassAndObject(doc);
        outputCollector.collect(new IntWritable(shard), new BytesWritable(crushedDocument));
    }

    public ElephantOutputFormat.Args outputArgs(JobConf conf) throws IOException {
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

        return eargs;
    }

    @Override public void sinkInit(JobConf conf) throws IOException {

        ElephantOutputFormat.Args args = outputArgs(conf);

        // serialize this particular argument off into the JobConf.
        Utils.setObject(conf, ElephantOutputFormat.ARGS_CONF, args);
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
}

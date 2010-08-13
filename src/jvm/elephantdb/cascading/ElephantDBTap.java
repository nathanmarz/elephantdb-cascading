package elephantdb.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.scheme.Scheme;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.hadoop.ElephantOutputFormat;
import elephantdb.hadoop.ElephantRecordWritable;
import elephantdb.hadoop.ElephantUpdater;
import elephantdb.hadoop.ReplaceUpdater;
import elephantdb.store.DomainStore;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;


public class ElephantDBTap extends SinkTap implements FlowListener {
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
        public Map<String, Map<String, Object>> persistenceOptions = null;
        public List<String> tmpDirs = null;
        public ElephantUpdater updater = new ReplaceUpdater(); //set this to null to prevent updating
        public Fields fields = Fields.ALL;
        public int timeoutMs = 2*60*60*1000; // 2 hours
    }

    String _outdir;
    DomainSpec _spec;
    Args _args;
    String _newVersionPath;

    public ElephantDBTap(String outdir, Args args) throws IOException {
        this(outdir, null, args);
    }

    public ElephantDBTap(String outdir, DomainSpec spec) throws IOException {
        this(outdir, spec, new Args());
    }

    public ElephantDBTap(String outdir) throws IOException {
        this(outdir, null, new Args());
    }

    public ElephantDBTap(String outdir, DomainSpec spec, Args args) throws IOException {
        super(new ElephantScheme());
        _outdir = outdir;
        _spec = new DomainStore(outdir, spec).getSpec();
        _args = args;
    }

    private DomainStore getDomainStore() throws IOException {
        return new DomainStore(_outdir, _spec);
    }

    public DomainSpec getSpec() {
        return _spec;
    }

    @Override
    public Fields getSinkFields() {
        return _args.fields;
    }

    @Override
    public boolean isWriteDirect() {
        return true;
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        int bucket = tupleEntry.getInteger(0);
        Object key = tupleEntry.get(1);
        byte[] keybytes = Common.serializeElephantVal(key);
        byte[] valuebytes = Common.getBytes((BytesWritable)tupleEntry.get(2));

        ElephantRecordWritable record = new ElephantRecordWritable(keybytes, valuebytes);
        outputCollector.collect(new IntWritable(bucket), record);
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
        DomainStore dstore = getDomainStore();
        if(_newVersionPath==null) { //working around cascading calling sinkinit twice
            _newVersionPath = dstore.createVersion();
        }
        ElephantOutputFormat.Args eargs = new ElephantOutputFormat.Args(_spec, _newVersionPath);
        if(_args.persistenceOptions!=null) {
            eargs.persistenceOptions = _args.persistenceOptions;
        }
        if(_args.tmpDirs!=null) {
            eargs.tmpDirs = _args.tmpDirs;
        }
        if(_args.updater!=null) {
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
        return new Path(_outdir);
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

    public void onCompleted(Flow flow) {
        try {
            DomainStore dstore = getDomainStore();
            if(flow.getFlowStats().isSuccessful()) {
                if(_args.updater!=null) {
                    dstore.synchronizeInProgressVersion(_newVersionPath);
                }
                dstore.succeedVersion(_newVersionPath);
            } else {
                dstore.failVersion(_newVersionPath);
            }
        } catch(IOException e) {
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
}

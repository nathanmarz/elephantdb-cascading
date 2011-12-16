package elephantdb.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
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
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

public abstract class ElephantBaseTap<G extends IGateway>
    extends Tap<HadoopFlowProcess, JobConf, RecordReader, OutputCollector> implements FlowListener {

    public static final Logger LOG = Logger.getLogger(ElephantBaseTap.class);

    public static class Args implements Serializable {
        //for source and sink
        public Map<String, Object> persistenceOptions = null;
        public List<String> tmpDirs = null;
        public int timeoutMs = 2 * 60 * 60 * 1000; // 2 hours

        //source specific
        public Fields sourceFields = new Fields("key", "value");
        public Long version = null; //for sourcing

        //sink specific
        public Fields sinkFields = Fields.ALL;
        public ElephantUpdater updater = new IdentityUpdater();

    }

    String _domainDir;
    DomainSpec _spec;
    Args _args;
    String _newVersionPath;

    public ElephantBaseTap(String dir, DomainSpec spec, Args args) throws IOException {
        super();

        _domainDir = dir;
        _args = args;
        _spec = new DomainStore(dir, spec).getSpec();
        setScheme(new ElephantScheme(_spec.getCoordinator(), freshGateway()));
    }

    public abstract G freshGateway();

    public DomainStore getDomainStore() throws IOException {
        return new DomainStore(_domainDir, _spec);
    }

    public DomainSpec getSpec() {
        return _spec;
    }

    @Override public Fields getSourceFields() {
        return _args.sourceFields;
    }

    @Override public Fields getSinkFields() {
        return _args.sinkFields;
    }

    @Override public boolean isSink() {
        return true;
    }

    @Override public void sourceConfInit(HadoopFlowProcess process, JobConf conf) {
        FileInputFormat.setInputPaths(conf, "/" + UUID.randomUUID().toString());

        ElephantInputFormat.Args eargs = new ElephantInputFormat.Args(_domainDir);
        eargs.inputDirHdfs = _domainDir;
        if (_args.persistenceOptions != null) {
            eargs.persistenceOptions = _args.persistenceOptions;
        }
        if (_args.tmpDirs != null) {
            LocalElephantManager.setTmpDirs(conf, _args.tmpDirs);
        }

        eargs.version = _args.version;

        conf.setInt("mapred.task.timeout", _args.timeoutMs);
        Utils.setObject(conf, ElephantInputFormat.ARGS_CONF, eargs);

        super.sourceConfInit( null, conf );
    }

    @Override public void sinkConfInit(HadoopFlowProcess process, JobConf conf) {
        ElephantOutputFormat.Args args = null;
        try {
            args = outputArgs(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // serialize this particular argument off into the JobConf.
        Utils.setObject(conf, ElephantOutputFormat.ARGS_CONF, args);
        conf.setInt("mapred.task.timeout", _args.timeoutMs);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setInt("mapred.reduce.tasks", _spec.getNumShards());

        super.sinkConfInit( null, conf );
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

    public Path getPath() {
        return new Path(_domainDir);
    }

    @Override
    public String getIdentifier() {
        return getPath().toString();
    }

    @Override
    public boolean createResource(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean deleteResource(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean resourceExists(JobConf jc) throws IOException {
        return false;
    }

    @Override
    public long getModifiedTime(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

    public void onStarting(Flow flow) {
    }

    public void onStopping(Flow flow) {
    }

    private boolean isSinkOf(Flow<JobConf> flow) {
        for (Entry<String, Tap> e : flow.getSinks().entrySet()) {
            if (e.getValue() == this) { return true; }
        }
        return false;
    }

    public void onCompleted(Flow flow) {
        try {
            if (isSinkOf(flow)) {
                DomainStore domainStore = getDomainStore();
                if (flow.getFlowStats().isSuccessful()) {
                    domainStore.getFileSystem().mkdirs(new Path(_newVersionPath));
                    if (_args.updater != null) {
                        domainStore.synchronizeInProgressVersion(_newVersionPath);
                    }
                    domainStore.succeedVersion(_newVersionPath);
                } else {
                    domainStore.failVersion(_newVersionPath);
                }
            }
        } catch (IOException e) {
            throw new TapException("Couldn't finalize new elephant domain version", e);
        } finally {
            _newVersionPath = null; //working around cascading calling sinkConfInit twice
        }
    }

    public boolean onThrowable(Flow flow, Throwable t) {
        return false;
    }

    @Override
    public TupleEntryCollector openForWrite( HadoopFlowProcess flowProcess, OutputCollector output ) throws IOException
    {
        if( output != null )
            return super.openForWrite( flowProcess, output );

        HadoopTapCollector schemeCollector = new HadoopTapCollector( flowProcess, this );

        schemeCollector.prepare();

        return schemeCollector;
    }

    @Override
    public TupleEntryIterator openForRead(HadoopFlowProcess flowProcess, RecordReader input)
        throws IOException {
        return new TupleEntrySchemeIterator(flowProcess, getScheme(), new RecordReaderIterator(input));
    }

    @Override public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (_domainDir != null ? _domainDir.hashCode() : 0);
        return result;
    }

    @Override public boolean equals(Object object) {
        if (this == object) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        if (!super.equals(object)) { return false; }

        ElephantBaseTap tap = (ElephantBaseTap) object;

        if (_domainDir != null ? !_domainDir.equals(tap._domainDir) : tap._domainDir != null) {
            return false;
        }

        return true;
    }
}

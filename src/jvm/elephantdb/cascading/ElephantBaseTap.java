package elephantdb.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.hadoop.ElephantInputFormat;
import elephantdb.hadoop.ElephantOutputFormat;
import elephantdb.hadoop.LocalElephantManager;
import elephantdb.index.IdentityIndexer;
import elephantdb.index.Indexer;
import elephantdb.store.DomainStore;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;


public abstract class ElephantBaseTap<G extends IGateway> extends Hfs implements FlowListener {
    public static final Logger LOG = Logger.getLogger(ElephantBaseTap.class);

    public static class Args implements Serializable {
        //for source and sink
        public Map<String, Object> persistenceOptions = null;
        public List<String> tmpDirs = null;
        public int timeoutMs = 2 * 60 * 60 * 1000; // 2 hours

        //source specific
        public Fields sourceFields = new Fields("key", "value");
        public Long version = null; //for sourcing
        public boolean recompute = false;

        //sink specific
        public Fields sinkFields = Fields.ALL;
        public Indexer indexer = new IdentityIndexer();

    }

    String domainDir;
    DomainSpec spec;
    Args args;
    String newVersionPath;

    public ElephantBaseTap(String dir, DomainSpec spec, Args args) throws IOException {
        domainDir = dir;
        this.args = args;
        this.spec = new DomainStore(dir, spec).getSpec();

        setStringPath(domainDir);
        setScheme(new ElephantScheme(this.args.sourceFields, this.args.sinkFields, this.spec, freshGateway()));
    }

    public abstract G freshGateway();

    public DomainStore getDomainStore() throws IOException {
        return new DomainStore(domainDir, spec);
    }

    public DomainSpec getSpec() {
        return spec;
    }

    @Override public void sourceConfInit(HadoopFlowProcess process, JobConf conf) {
        super.sourceConfInit( process, conf );

        FileInputFormat.setInputPaths(conf, "/" + UUID.randomUUID().toString());

        ElephantInputFormat.Args eargs = new ElephantInputFormat.Args(domainDir);
        eargs.inputDirHdfs = domainDir;
        if (args.persistenceOptions != null) {
            eargs.persistenceOptions = args.persistenceOptions;

        }
        if (args.tmpDirs != null) {
            LocalElephantManager.setTmpDirs(conf, args.tmpDirs);
        }

        eargs.version = args.version;

        conf.setInt("mapred.task.timeout", args.timeoutMs);
        Utils.setObject(conf, ElephantInputFormat.ARGS_CONF, eargs);
    }

    @Override public void sinkConfInit(HadoopFlowProcess process, JobConf conf) {
        super.sinkConfInit( process, conf );

        ElephantOutputFormat.Args args = null;
        try {
            args = outputArgs(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // serialize this particular argument off into the JobConf.
        Utils.setObject(conf, ElephantOutputFormat.ARGS_CONF, args);
        conf.setInt("mapred.task.timeout", this.args.timeoutMs);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setInt("mapred.reduce.tasks", spec.getNumShards());
    }

    public ElephantOutputFormat.Args outputArgs(JobConf conf) throws IOException {
        DomainStore dstore = getDomainStore();

        if (newVersionPath == null) { //working around cascading calling sinkinit twice
            newVersionPath = dstore.createVersion();
        }
        ElephantOutputFormat.Args eargs = new ElephantOutputFormat.Args(spec, newVersionPath);
        if (args.persistenceOptions != null) {
            eargs.persistenceOptions = args.persistenceOptions;
        }
        if (args.tmpDirs != null) {
            LocalElephantManager.setTmpDirs(conf, args.tmpDirs);
        }

        if (args.indexer != null)
            eargs.indexer = args.indexer;

        // If recompute is set to false, we go ahead and populate the
        // update Dir. Else, we leave it blank.

        if (!args.recompute)
            eargs.updateDirHdfs = dstore.mostRecentVersionPath();

        return eargs;
    }

    @Override
    public Path getPath() {
        return new Path(domainDir);
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
    public long getModifiedTime(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

    public void onStarting(Flow flow) {
    }

    public void onStopping(Flow flow) {
    }

    private boolean isSinkOf(Flow<JobConf> flow) {
        for (Entry<String, Tap> e : flow.getSinks().entrySet()) {
            if (e.getValue() == this)
                return true;
        }
        return false;
    }

    // TODO: Move guts of onCompleted over to commitResource.
    @Override public boolean commitResource(JobConf conf) {
        return true;
    }

    public void onCompleted(Flow flow) {
        try {
            if (isSinkOf(flow)) {
                DomainStore domainStore = getDomainStore();
                if (flow.getFlowStats().isSuccessful()) {
                    domainStore.getFileSystem().mkdirs(new Path(newVersionPath));
                    if (args.indexer != null) {
                        domainStore.synchronizeInProgressVersion(newVersionPath);
                    }
                    domainStore.succeedVersion(newVersionPath);
                } else {
                    domainStore.failVersion(newVersionPath);
                }
            }
        } catch (IOException e) {
            throw new TapException("Couldn't finalize new elephant domain version", e);
        } finally {
            newVersionPath = null; //working around cascading calling sinkConfInit twice
        }
    }

    public boolean onThrowable(Flow flow, Throwable t) {
        return false;
    }
}

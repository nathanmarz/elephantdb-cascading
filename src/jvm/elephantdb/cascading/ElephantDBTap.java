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
import java.util.Map.Entry;
import java.util.UUID;


public class ElephantDBTap extends Hfs implements FlowListener {
    public static final Logger LOG = Logger.getLogger(ElephantDBTap.class);

    public static class Args implements Serializable {
        //for source and sink
        public List<String> tmpDirs = null;
        public int timeoutMs = 2 * 60 * 60 * 1000; // 2 hours
        public Gateway gateway = new IdentityGateway();

        //source specific
        public Fields sourceFields = Fields.ALL;
        public Long version = null; //for sourcing

        //sink specific
        public Fields sinkFields = Fields.ALL;
        public Indexer indexer = new IdentityIndexer();
        public boolean incremental = false; //for sourcing
    }

    String domainDir;
    DomainSpec spec;
    Args args;
    String newVersionPath;

    public ElephantDBTap(String dir, DomainSpec spec, Args args) throws IOException {
        domainDir = dir;
        this.args = args;
        this.spec = new DomainStore(dir, spec).getSpec();

        setStringPath(domainDir);
        setScheme(new ElephantScheme(this.args.sourceFields,
            this.args.sinkFields, this.spec, this.args.gateway));
    }

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

        if (args.tmpDirs != null) {
            LocalElephantManager.setTmpDirs(conf, args.tmpDirs);
        }

        if (args.indexer != null)
            eargs.indexer = args.indexer;

        // If incremental is set to true, we go ahead and populate the
        // update Dir. Else, we leave it blank.
        if (args.incremental)
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

    public void onCompleted(Flow flow) {
        try {
            if (isSinkOf(flow)) {
                DomainStore dstore = getDomainStore();
                if (flow.getFlowStats().isSuccessful()) {
                    dstore.getFileSystem().mkdirs(new Path(newVersionPath));

                    // If the user wants to run a incremental, skip version synchronization.
                    if (args.incremental) {
                        dstore.synchronizeInProgressVersion(newVersionPath);
                    }

                    dstore.succeedVersion(newVersionPath);
                } else {
                    dstore.failVersion(newVersionPath);
                }
            }
        } catch (IOException e) {
            throw new TapException("Couldn't finalize new elephant domain version", e);
        } finally {
            newVersionPath = null; //working around cascading calling sinkinit twice
        }
    }

    // TODO: Once failResource is implemented, move guts of onCompleted over to commitResource.
    @Override public boolean commitResource(JobConf conf) {
        return true;
    }

    public boolean onThrowable(Flow flow, Throwable t) {
        return false;
    }
}

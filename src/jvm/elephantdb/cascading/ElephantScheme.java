package elephantdb.cascading;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import elephantdb.Utils;
import elephantdb.hadoop.ElephantInputFormat;
import elephantdb.hadoop.ElephantOutputFormat;
import elephantdb.persistence.PersistenceCoordinator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class ElephantScheme extends
    Scheme<HadoopFlowProcess, JobConf, RecordReader, OutputCollector<IntWritable, BytesWritable>, Object[], Void> {

    PersistenceCoordinator _coordinator;
    IGateway _gateway;

    public ElephantScheme(PersistenceCoordinator coordinator, IGateway gateway) {
        _coordinator = coordinator;
        _gateway = gateway;
    }

    @Override
    public void sourceConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
        conf.setInputFormat(ElephantInputFormat.class);
    }

    @Override
    public void sinkConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
        conf.setOutputFormat(ElephantOutputFormat.class);
    }

    @Override public void sourcePrepare(HadoopFlowProcess flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {
        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override public boolean source(HadoopFlowProcess hadoopFlowProcess,
        SourceCall<Object[], RecordReader> sourceCall) throws IOException {

        NullWritable key = (NullWritable) sourceCall.getContext()[0];
        BytesWritable value = (BytesWritable) sourceCall.getContext()[1];

        boolean result = sourceCall.getInput().next(key, value);

        if (!result) { return false; }

        byte[] valBytes = Utils.getBytes(value);
        Object doc = _coordinator.getKryoBuffer().deserialize(valBytes);

        sourceCall.getIncomingEntry().setTuple(new Tuple(doc));
        return true;
    }

    @Override public void sink(HadoopFlowProcess hadoopFlowProcess,
        SinkCall<Void, OutputCollector<IntWritable, BytesWritable>> sinkCall) throws IOException {
        TupleEntry tuple = sinkCall.getOutgoingEntry();

        int shard = tuple.getInteger(0);
        Object doc = tuple.getObject(1);

        byte[] crushedDocument = _coordinator.getKryoBuffer().serialize(doc);
        sinkCall.getOutput().collect(new IntWritable(shard), new BytesWritable(crushedDocument));
    }
}

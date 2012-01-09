package elephantdb.cascading;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.hadoop.ElephantInputFormat;
import elephantdb.hadoop.ElephantOutputFormat;
import elephantdb.serialize.Serializer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class ElephantScheme extends Scheme<HadoopFlowProcess, JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    Serializer serializer;
    IGateway gateway;

    public ElephantScheme(Fields sourceFields, Fields sinkFields, DomainSpec spec, IGateway gateway) {
        setSourceFields(sourceFields);
        setSinkFields(sinkFields);
        this.serializer = Utils.makeSerializer(spec);
        this.gateway = gateway;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    @Override
    public void sourceConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
        conf.setInputFormat(ElephantInputFormat.class);
    }

    @Override public void sinkConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
        conf.setOutputKeyClass(IntWritable.class); // be explicit
        conf.setOutputValueClass( BytesWritable.class ); // be explicit
        conf.setOutputFormat(ElephantOutputFormat.class);
    }

    @Override public void sourcePrepare(HadoopFlowProcess flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {

        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override public boolean source(HadoopFlowProcess flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) throws IOException {

        NullWritable key = (NullWritable) sourceCall.getContext()[0];
        BytesWritable value = (BytesWritable) sourceCall.getContext()[1];

        boolean result = sourceCall.getInput().next(key, value);

        if (!result) {
            return false;
        }

        byte[] valBytes = Utils.getBytes(value);
        Object doc = getSerializer().deserialize(valBytes);

        sourceCall.getIncomingEntry().setTuple(gateway.buildTuple(doc));
        return true;
    }

    @Override public void sink(HadoopFlowProcess flowProcess,
        SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();


        int shard = tuple.getInteger(0);
        Object doc = gateway.buildDocument(tuple);

        byte[] crushedDocument = getSerializer().serialize(doc);
        sinkCall.getOutput().collect(new IntWritable(shard), new BytesWritable(crushedDocument));
    }
}

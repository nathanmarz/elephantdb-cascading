package elephantdb.cascading;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.hadoop.ElephantInputFormat;
import elephantdb.hadoop.ElephantOutputFormat;
import elephantdb.serialize.Serializer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

public class ElephantScheme extends Scheme {
    Serializer serializer;
    Gateway gateway;

    public ElephantScheme(Fields sourceFields, Fields sinkFields, DomainSpec spec, Gateway gateway) {
        setSourceFields(sourceFields);
        setSinkFields(sinkFields);
        this.serializer = Utils.makeSerializer(spec);
        this.gateway = gateway;
    }

    @Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
        conf.setInputFormat(ElephantInputFormat.class);
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        conf.setOutputKeyClass( IntWritable.class ); // be explicit
        conf.setOutputValueClass( BytesWritable.class ); // be explicit
        conf.setOutputFormat(ElephantOutputFormat.class);
    }

    public Serializer getSerializer() {
        return serializer;
    }

    @Override
    public Tuple source(Object key, Object value) {
        byte[] valBytes = Utils.getBytes((BytesWritable) value);
        Object doc = getSerializer().deserialize(valBytes);
        return gateway.toTuple(doc);
    }

    // This is generic, this is good stuff.
    @Override public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
        throws IOException {
        Tuple tuple = tupleEntry.getTuple();

        int shard = tuple.getInteger(0);
        Object doc = gateway.fromTuple(tuple);

        byte[] crushedDocument = getSerializer().serialize(doc);
        outputCollector.collect(new IntWritable(shard), new BytesWritable(crushedDocument));
    }
}

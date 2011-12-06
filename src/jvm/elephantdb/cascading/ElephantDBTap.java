package elephantdb.cascading;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.KeyValDocument;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;


public class ElephantDBTap extends ElephantBaseTap {

    public ElephantDBTap(String dir, Args args) throws IOException {
        this(dir, null, args);
    }

    public ElephantDBTap(String dir) throws IOException {
        this(dir, null, new Args());
    }

    public ElephantDBTap(String dir, DomainSpec spec) throws IOException {
        this(dir, spec, new Args());
    }

    public ElephantDBTap(String dir, DomainSpec spec, Args args) throws IOException {
        super(dir, spec, args);
    }

    // TODO: Modify this to use the DomainSpec deserializer. key is a NullWritable,
    @Override public Tuple source(Object key, Object value) {
        byte[] valBytes = Utils.getBytes((BytesWritable) value);
        KeyValDocument doc = _kryoBuf.readObject(valBytes, KeyValDocument.class);
        return new Tuple(doc.key, doc.value);
    }

    /**
     * Sinks 3-tuples of the form [shardIdx, key, val] out to Hadoop. key and val are serialized
     * with Kryo.
     * @param tupleEntry
     * @param outputCollector
     * @throws IOException
     */
    @Override public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
        throws IOException {
        int shard = tupleEntry.getInteger(0);
        Object key = tupleEntry.get(1);
        Object val = tupleEntry.get(2);

        KeyValDocument<Object, Object> doc = new KeyValDocument<Object, Object>(key, val);
        byte[] docbytes = _kryoBuf.writeClassAndObject(doc);
        outputCollector.collect(new IntWritable(shard), new BytesWritable(docbytes));
    }

    // TODO: Implement hashcode and equals in the superclass.
    @Override public int hashCode() {
        return new Integer(_id).hashCode();
    }

    @Override public boolean equals(Object object) {
        if (object instanceof ElephantDBTap) {
            return _id == ((ElephantDBTap) object)._id;
        } else {
            return false;
        }
    }

    private int _id;
    private static int globalid = 0;
}

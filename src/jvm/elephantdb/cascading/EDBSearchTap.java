package elephantdb.cascading;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import elephantdb.DomainSpec;
import elephantdb.hadoop.ElephantRecordWritable;
import elephantdb.persistence.Transmitter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/** User: sritchie Date: 11/22/11 Time: 5:58 PM */
public class EDBSearchTap extends ElephantBaseTap {

    // TODO: What constructors make sense? Check wonderdog.
    public EDBSearchTap(String dir, Args args) throws IOException {
        this(dir, null, args);
    }

    public EDBSearchTap(String dir) throws IOException {
        this(dir, null, new Args());
    }

    public EDBSearchTap(String dir, DomainSpec spec) throws IOException {
        this(dir, spec, new Args());
    }

    public EDBSearchTap(String dir, DomainSpec spec, Args args) throws IOException {
        super(dir, spec, args);
    }

    // TODO: needs to change for search.
    @Override public Tuple source(Object key, Object value) {
        key = (_args.deserializer == null) ? key :
            _args.deserializer.deserialize((BytesWritable) key);
        return new Tuple(key, value);
    }

    // Generic! Serialization's implemented with the transmitter.
    // Can take any object, at this point.
    // TODO: Remove this from here and ElephantDBTap.
    @Override public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
        throws IOException {
        int shard = tupleEntry.getInteger(0);
        Object key = tupleEntry.get(1);
        Object val = tupleEntry.get(2);

        Transmitter trans = _fact.getTransmitter();
        byte[] keybytes = trans.serializeKey(key);
        byte[] valuebytes = trans.serializeVal(val);

        ElephantRecordWritable record = new ElephantRecordWritable(keybytes, valuebytes);
        outputCollector.collect(new IntWritable(shard), record);
    }

    // TODO: Implement hashcode and equals in the superclass.
    @Override public int hashCode() {
        return new Integer(_id).hashCode();

    }

    @Override public boolean equals(Object object) {
        if (object instanceof EDBSearchTap) {
            return _id == ((EDBSearchTap) object)._id;
        } else {
            return false;
        }
    }

    private int _id;
    private static int globalid = 0;
}


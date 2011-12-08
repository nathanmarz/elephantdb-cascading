package elephantdb.cascading;

import cascading.tuple.Tuple;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.lucene.document.Document;

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

    @Override public Tuple source(Object key, Object value) {
        byte[] valBytes = Utils.getBytes((BytesWritable) value);
        Document doc = _spec.deserialize(valBytes, Document.class);
        return new Tuple(doc);
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


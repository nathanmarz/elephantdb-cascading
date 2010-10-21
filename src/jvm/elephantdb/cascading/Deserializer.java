package elephantdb.cascading;

import java.io.Serializable;
import org.apache.hadoop.io.BytesWritable;

public interface Deserializer extends Serializable {
    public Object deserialize(BytesWritable bw);
}

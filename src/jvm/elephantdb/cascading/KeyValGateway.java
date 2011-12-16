package elephantdb.cascading;

import cascading.tuple.Tuple;
import elephantdb.persistence.KeyValDocument;

/** User: sritchie Date: 12/16/11 Time: 12:08 AM */
public class KeyValGateway implements IGateway<KeyValDocument> {
    public KeyValDocument buildDocument(Tuple tuple) {
        Object key = tuple.getObject(1);
        Object val = tuple.getObject(2);

        return new KeyValDocument<Object, Object>(key, val);
    }

    public Tuple buildTuple(KeyValDocument document) {
        return new Tuple(document.key, document.value);
    }
}